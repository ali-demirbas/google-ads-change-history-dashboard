"""fetch_live_data.py — optional live Google Ads API adapter.

NOT part of the skill (skills/google-ads-change-history-dashboard/ads_change_history.py
stays single-file, zero-dependency, offline — that promise is unchanged).
This is a separate, opt-in tool: it calls the real Google Ads API, flattens
the result into the exact JSON shape ads_change_history.py already knows how
to read (the google_ads_api_change_event_json known_source), and writes it
to a file. You then run the actual skill against that file, same as any
other export:

    python3 tools/fetch_live_data.py --customer-id 1234567890 --out changes.json
    python3 skills/google-ads-change-history-dashboard/ads_change_history.py run changes.json --out-dir ./out --open

WHO THIS IS FOR: only accounts with real Google Ads API access (a developer
token + OAuth client). Everyone else keeps using the skill exactly as
before, feeding it a CSV/JSON export — nothing about the core tool changes.

REQUIRES (only for this script, never for the skill itself):
    pip install google-ads

CREDENTIALS: read from environment variables only — never passed on the
command line (shell history), never written to a file this script creates,
never logged. Uses the google-ads library's own GoogleAdsClient.load_from_env(),
which expects:
    GOOGLE_ADS_DEVELOPER_TOKEN
    GOOGLE_ADS_CLIENT_ID
    GOOGLE_ADS_CLIENT_SECRET
    GOOGLE_ADS_REFRESH_TOKEN
    GOOGLE_ADS_LOGIN_CUSTOMER_ID   (optional — only if querying a client
                                    account through a manager account)
    GOOGLE_ADS_USE_PROTO_PLUS=true (recommended; this script assumes it)

Never paste these into a chat, a commit, or this repo. Set them in your own
shell/CI secret store.

VERIFIED vs BEST-EFFORT — read before trusting the output blindly:
  - The event-level fields (resource_name, change_date_time,
    change_resource_type, change_resource_name, client_type, user_email,
    resource_change_operation, changed_fields, campaign, ad_group, feed,
    feed_item, asset) are the real, documented ChangeEvent fields — verified
    against Google's own proto definitions this session.
  - old_resource/new_resource are NOT simple strings in the real API — each
    is a ChangedResource message (a nested snapshot of the whole resource,
    keyed by resource type). This script's flatten_changed_value() makes a
    best-effort attempt to walk the changed_fields path into that nested
    structure and pull out just the one value that changed. When that
    doesn't resolve cleanly (multiple changed_fields paths at once, or a
    path shape this hasn't been tested against), it falls back to the whole
    serialized resource as a JSON string rather than guessing wrong or
    dropping data. For the multiple-paths case that fallback is exactly
    what the skill wants: ads_change_history.py splits such an event into
    one row per changed field, reading each field's value out of that
    serialized resource itself. This is the part most likely to need refinement against
    real output — if a row's old_value/new_value looks like a big JSON blob
    instead of a clean value, that's this fallback firing; it's a real
    finding worth reporting back, not a crash.
"""
import argparse
import json
import sys
import os
from datetime import date, timedelta

# The Google Ads API version every request here is made against. Google
# retires each version about a year after release, and a request to a
# retired version fails outright — this script shipped pinned to v21 and
# stopped working when v21 was sunset. Checked 2026-11-14 against Google's
# API sunset-dates page: v26 (released 2026-10-14) sunsets November 2027 and
# needs google-ads >= 34.0.0 for Python (its changelog: "34.0.0 - v26
# release") — the lower bound in requirements.txt. ChangeEvent fields and
# enums used here are unchanged v20-v26. Change all three together; the
# skill's self-test warns once API_VERSION_SUNSET has passed.
# --api-version overrides it for one run without editing this file.
API_VERSION = "v26"
API_VERSION_SUNSET = date(2027, 11, 1)


def _to_dict(proto_plus_message):
    """proto-plus message -> plain dict with enum NAMES ("PAUSED"), not numbers.
    proto-plus's to_dict is a classmethod on the message type and defaults to
    use_integers_for_enums=True (checked 2026-11-10 against proto-plus 1.29),
    which turned every status/type value into a bare integer. Returns {} for
    an unset oneof member (a CREATE has no "old" state)."""
    if proto_plus_message is None:
        return {}
    to_dict = getattr(type(proto_plus_message), "to_dict", None)
    if to_dict is not None:
        for kwargs in ({"use_integers_for_enums": False, "preserving_proto_field_name": True}, {"use_integers_for_enums": False}):
            try:
                return to_dict(proto_plus_message, **kwargs) or {}
            except TypeError:
                continue
            except Exception:
                return {}
    try:
        return proto_plus_message.to_dict() or {}
    except Exception:
        return {}


def _enum_name(value):
    """Enum field -> its name; tolerant of a plain int/str (non-proto-plus mode)."""
    name = getattr(value, "name", None)
    return name if isinstance(name, str) else str(value)


def _to_camel(snake):
    head, *rest = snake.split("_")
    return head + "".join(w.capitalize() for w in rest)


def flatten_changed_value(changed_resource_dict, field_paths):
    """Best-effort extraction of "the value that changed" from a
    ChangedResource's serialized dict, given the FieldMask path(s) that
    changed. Never raises — always returns a string, falling back to the
    full serialized resource when the path can't be walked cleanly or more
    than one field changed at once (there's no single "the" value then).

    Tries both snake_case (changed_fields.paths' own spelling, e.g.
    "amount_micros") and camelCase (raw protobuf's default JSON-mapping
    spelling, e.g. "amountMicros") at every step, since which one
    .to_dict() actually returns is exactly the part this script can't
    verify without a live account — see the module docstring."""
    if not changed_resource_dict:
        return ""
    if len(field_paths) != 1:
        return json.dumps(changed_resource_dict, ensure_ascii=False)
    node = changed_resource_dict
    # ChangedResource is a oneof — the actual resource sits one level down,
    # under a key named after the resource type (e.g. "campaignBudget" or
    # "campaign_budget" depending on the client's key casing).
    if isinstance(node, dict) and len(node) == 1:
        node = next(iter(node.values()))
    for part in field_paths[0].split("."):
        if not isinstance(node, dict):
            return json.dumps(changed_resource_dict, ensure_ascii=False)
        if part in node:
            node = node[part]
        elif _to_camel(part) in node:
            node = node[_to_camel(part)]
        else:
            return json.dumps(changed_resource_dict, ensure_ascii=False)
    if isinstance(node, (dict, list)):
        return json.dumps(node, ensure_ascii=False)
    return str(node)


CHANGE_EVENT_QUERY = """
SELECT
  change_event.resource_name,
  change_event.change_date_time,
  change_event.change_resource_type,
  change_event.change_resource_name,
  change_event.client_type,
  change_event.user_email,
  change_event.resource_change_operation,
  change_event.changed_fields,
  change_event.old_resource,
  change_event.new_resource,
  change_event.campaign,
  change_event.ad_group,
  change_event.feed,
  change_event.feed_item,
  change_event.asset
FROM change_event
WHERE change_event.change_date_time >= '{start}'
  AND change_event.change_date_time <= '{end}'
ORDER BY change_event.change_date_time DESC
LIMIT {limit}
"""


MAX_LIMIT = 10000


def build_query(days, limit, today=None):
    """2026-11-10 (S-6): GAQL only has LAST_7/14/30_DAYS, so LAST_{n}_DAYS was
    invalid for any other --days and never included today. An explicit date
    range covers `days` calendar days ending today (account time zone)."""
    today = today or date.today()
    start = today - timedelta(days=days - 1)
    return CHANGE_EVENT_QUERY.format(start=start.isoformat(), end=f"{today.isoformat()} 23:59:59", limit=limit)


def fetch(customer_id, days=30, limit=10000, api_version=API_VERSION):
    try:
        from google.ads.googleads.client import GoogleAdsClient
        from google.ads.googleads.errors import GoogleAdsException
    except ImportError:
        print(json.dumps({
            "status": "error",
            "message": "The 'google-ads' package isn't installed. Run: pip install google-ads",
        }, indent=2))
        sys.exit(2)

    if days > 30:
        print(f"Note: Google's ChangeEvent API only retains 30 days regardless of what's requested — capping at 30 (you asked for {days}).", file=sys.stderr)
        days = 30
    days = max(1, int(days))
    limit = min(max(1, int(limit)), MAX_LIMIT)

    try:
        client = GoogleAdsClient.load_from_env(version=api_version)
    except Exception as e:
        print(json.dumps({
            "status": "error",
            "message": f"Couldn't load Google Ads API credentials from environment variables ({e}). "
                       f"Set GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, "
                       f"GOOGLE_ADS_REFRESH_TOKEN (and GOOGLE_ADS_LOGIN_CUSTOMER_ID if querying via a manager account).",
        }, indent=2))
        sys.exit(2)

    ga_service = client.get_service("GoogleAdsService")
    query = build_query(days, limit)

    rows_out = []
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            ce = row.change_event
            old_dict = _to_dict(ce.old_resource)
            new_dict = _to_dict(ce.new_resource)
            field_paths = list(ce.changed_fields.paths)
            out = {
                "customer_id": customer_id,
                "resource_name": ce.resource_name,
                "change_date_time": ce.change_date_time,
                "change_resource_type": _enum_name(ce.change_resource_type),
                "change_resource_name": ce.change_resource_name,
                "client_type": _enum_name(ce.client_type),
                "user_email": ce.user_email or None,
                "resource_change_operation": _enum_name(ce.resource_change_operation),
                "changed_fields": field_paths if len(field_paths) != 1 else field_paths[0],
                "old_resource": flatten_changed_value(old_dict, field_paths),
                "new_resource": flatten_changed_value(new_dict, field_paths),
            }
            for extra in ("campaign", "ad_group", "feed", "feed_item", "asset"):
                v = getattr(ce, extra, "")
                if v:
                    out[extra] = v
            rows_out.append(out)
    except GoogleAdsException as ex:
        errors = [{"message": e.message, "error_code": str(e.error_code)} for e in ex.failure.errors]
        print(json.dumps({"status": "error", "message": "Google Ads API request failed.", "errors": errors}, indent=2))
        sys.exit(2)

    return rows_out


def write_private_json(path, data):
    """Owner-only (0600) output: the file holds account data and e-mails (S-9).
    Zero events are written as [] -- the skill renders that as an empty report."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        os.fchmod(fd, 0o600)
    except (AttributeError, OSError):
        pass
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=None)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--customer-id", required=True, help="Target Google Ads customer ID (digits only, no dashes).")
    ap.add_argument("--days", type=int, default=30, help="Calendar days to fetch, ending today (1-30; more is capped at 30, Google's own retention limit).")
    ap.add_argument("--limit", type=int, default=MAX_LIMIT, help="Row cap, 1-10000 (Google's own hard limit is 10,000 per query).")
    ap.add_argument("--out", required=True, help="Output JSON file path.")
    ap.add_argument("--api-version", default=API_VERSION, help=f"Google Ads API version to call (default: {API_VERSION}). Must be one your installed google-ads library supports.")
    args = ap.parse_args()

    customer_id = args.customer_id.replace("-", "")
    if not customer_id.isdigit():
        ap.error("--customer-id must be digits (dashes allowed)")
    if args.days < 1:
        ap.error("--days must be at least 1")
    if not 1 <= args.limit <= MAX_LIMIT:
        ap.error(f"--limit must be between 1 and {MAX_LIMIT}")
    rows = fetch(customer_id, days=args.days, limit=args.limit, api_version=args.api_version)

    write_private_json(args.out, rows)

    print(json.dumps({
        "status": "ok",
        "rows_fetched": len(rows),
        "note": "No change events in the requested range; the skill will build an empty report." if not rows else None,
        "out": args.out,
        "next_step": f"python3 skills/google-ads-change-history-dashboard/ads_change_history.py run {args.out} --out-dir ./out --open",
    }, indent=2))


if __name__ == "__main__":
    main()
