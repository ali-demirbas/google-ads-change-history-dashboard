#!/usr/bin/env python3
"""
ads_change_history.py — Ads Change History, single file.

WHAT THIS IS
  Answers: who changed what, when, in which account/campaign/ad group, what the
  old and new value were, and what category the change falls into. It also
  reports which campaigns were touched recently and which weren't (within the
  scope of the data it was given).

  It does NOT evaluate whether a change was good, risky, or strategic. "Campaign
  X hasn't been touched in 23 days" is something this tool says. "The agency is
  neglecting Campaign X" is not — that's a V2 judgment layer, not built here.

PIPELINE
  INPUT -> SOURCE DETECTION -> VALIDATION -> NORMALIZATION -> CATEGORIZATION
        -> AGGREGATION -> DASHBOARD

  Never guesses past a genuine ambiguity. Three points can stop the pipeline
  and ask a human (via whoever is running this — a Claude session in chat, or
  a person at a terminal) rather than silently picking an answer:

    1. Unknown column headers (source format never seen before)
       -> exit 2, status "needs_mapping". Re-run with --mapping-file.
    2. Ambiguous date format (DD/MM vs MM/DD) or ambiguous decimal separator
       -> exit 2, status "needs_date_format" / "needs_decimal_style".
    3. Unknown (resource_type, field_name, operation) combination
       -> exit 2, status "needs_category_review". A new rule gets added to
          CATEGORY_RULES (edit this file's CATEGORY_RULES dict directly, or
          keep confirmed rules in a small JSON override file passed via
          --extra-rules) and the categorize step is re-run.

  A resolved column-mapping for a previously-unseen source is saved to
  mapping-profiles/<fingerprint>.json (next to this script by default) so the
  same source never has to be re-confirmed.

CANONICAL SCHEMA (one row = one (resource, field, operation) change)
  change_id, event_id, source_event_id, timestamp, timestamp_iso, timezone,
  timestamp_utc, account_id, account_name, user_id, user_name, user_email,
  client_type, actor_type (human|automation|unknown), resource_type,
  resource_path, resource_id, resource_name, campaign_id, campaign_name,
  campaign_resource, ad_group_id, ad_group_name, ad_group_resource,
  operation (CREATE|UPDATE|REMOVE — plus UNKNOWN/UNSPECIFIED preserved
  as-is, un-coerced, when the source states them explicitly),
  operation_confidence (explicit|inferred), field_name, multi_field,
  old_value, new_value, old_value_num, new_value_num, old_value_display,
  new_value_display, value_unit, currency, value_confidence
  (structured|parsed_from_summary), category, subcategory, category_confidence
  (rule_matched|fallback_other), source, raw_summary
  (list kept in sync with HEADER_ALIASES["canonical_fields"]["all"] — audit
  2026-08-19 found this prose copy had drifted out of sync with it and with
  the actual output dict; both are now current as of this fix)

  change_id is a content hash (account+timestamp+user+resource+field+old+
  new+source_event_id — source_event_id joined 2026-08-19, see below) — used
  for DEDUPING (dedupe_rows(), called by run_pipeline before categorization:
  identical rows from a re-imported/overlapping file collapse, duplicate
  count reported in coverage.txt). event_id is source_event_id when
  the source provides one (verified real for the API: ChangeEvent's own
  top-level resource_name field, format
  customers/{id}/changeEvents/{timestamp_micros}~{command_index}~{mutate_index}
  — distinct from change_resource_name, which identifies the CHANGED resource,
  mapped to resource_path here), else falls back to change_id.
  resource_id is a best-effort tail-segment parse of resource_path
  (derive_resource_id_from_path()) when no separate bare-ID column exists.
  campaign_resource/ad_group_resource hold ChangeEvent's own literal
  campaign/ad_group fields (resource-name/path strings) verbatim;
  campaign_id/ad_group_id are the path tail parsed from them when no
  separate bare-ID column exists — see API_EXACT_CASE_RESOURCE_ALIASES.
  multi_field is true when the source's changed_fields arrived as a real
  JSON list (Google's FieldMask is a repeated field) — field_name is then a
  comma-joined string, and old_value/new_value represent the WHOLE changed
  resource, not a single field's before/after (this codebase has no clean
  way to split a flat old/new string per field). resource_name is currently
  always null: no known source maps anything to it (parallel to user_id —
  both declared, neither populated by any of the 3 registered sources).

VALUE PARSING RULES
  - Numbers: decimal separator is genuinely ambiguous for a pattern like
    "150.000" (150 vs 150000) unless the column disambiguates itself (two
    different separators present -> last one is decimal, locale-standard; or a
    1-2 digit trailing group -> clearly a decimal, not thousands). Ambiguous
    columns require --decimal-style TR|US — never guessed.
  - Micros: when field_name contains "micros" (the Google Ads API convention,
    1,000,000 = 1 unit), old_value_display/new_value_display hold the divided,
    human-readable number; old_value/new_value keep the untouched raw string.
    No currency symbol is invented — none is known here.
  - Dates: DD/MM vs MM/DD is ambiguous unless some value's day exceeds 12.
    Ambiguous columns require --date-format DMY|MDY|ISO — never guessed.
  - Timezone: never invented, but never discarded either — if the source
    string itself carries an offset ('+03:00', 'Z'), that's used directly for
    timestamp_utc regardless of whether --timezone was passed (fixed
    2026-08-18: an earlier version tried a truncated strptime parse first,
    which could silently match a prefix of an offset-bearing string and drop
    the offset). Only when the source has NO offset info at all does
    --timezone (or its absence) matter; rows stay timezone "unknown" then and
    are excluded from the UTC-bucketed timeline chart specifically (they still
    appear everywhere else).
  - actor_type: derived from client_type / user_name patterns. Automation
    hints: AUTOMATED, RULE, SCRIPT, RECOMMENDATION, BULK, SYNC, SA360, or a
    user_name starting with "ads-"/containing "system"/"bot". Human hints:
    WEB_CLIENT, MOBILE_APP, EDITOR. GOOGLE_ADS_API is deliberately NOT a human
    hint (fixed 2026-08-18 — "made via the API" doesn't tell you if a human
    ran a script or a service account did; treated as "unknown" instead of
    assumed human). Neither -> "unknown", its own bucket, never silently
    merged into "human".

SOURCE FORMATS REGISTERED IN KNOWN_SOURCES
  - legacy_summary_tr: verified against a real prior project's Sheet schema
    (Hesap Adı / Tarih / Kullanıcı / Değişiklik Tipi / Kampanya / Reklam Grubu
    / Değişiklik Özeti / Eski-Yeni Teknik Veri). No structured field/operation
    column — falls back to regex over the free-text summary.
  - google_ads_api_change_event_json: verified against Google's own docs
    (developers.google.com/google-ads/api/docs/change-event, fetched
    2026-08-17; re-verified against Google's proto definitions 2026-08-18 —
    audit F-03/F-14). Confirmed ChangeEvent fields: resource_name (the
    event's own ID), change_date_time, change_resource_type,
    change_resource_name, client_type, user_email, old_resource, new_resource,
    resource_change_operation, changed_fields, campaign, ad_group, feed,
    feed_item, asset. Retention: API covers only the last 30 days, 10,000 row
    cap — say so plainly if asked for older data rather than silently
    truncating. Latency: per Google's own documentation, ChangeEvent "could
    have up to 3 minutes delay to reflect a new change" — a very recent
    change may not yet appear. Coverage: ChangeEvent may not include every
    entry the Google Ads UI's own Change History page shows (Google states
    this explicitly) — the dashboard displays a caveat banner when the
    dataset includes API-sourced rows (detected via source_event_id being
    populated). Nested old_resource/new_resource and the repeated
    changed_fields FieldMask must already be flattened to one row per
    field before reaching this script (a live API adapter isn't built here —
    no credentials were available to build/test one; this script accepts
    already-flattened JSON).
  - google_ads_ui_export_en_guess: UNVERIFIED, low confidence — not confirmed
    against a real UI export in this session. Deliberately weak so a real
    export safely falls through to Layer 3 (ask+learn) instead of silently
    misapplying a guess.

REPO / PRIVACY NOTES
  Local-only for now (no repo visibility decided yet). --mask-users defaults
  OFF (raw user identity shown) — pass --mask-users to replace human user
  identities with "User A"/"User B" for external sharing. Account/campaign
  names are never masked — that's the real user's own data, showing it is the
  point of the tool.

USAGE
  python3 ads_change_history.py run <input.csv|.json> --out-dir ./out \\
      [--timezone Europe/Istanbul] [--date-format DMY] [--decimal-style TR] \\
      [--mapping-file confirmed_mapping.json] [--mask-users] [--open]

  python3 ads_change_history.py query <out/changes.jsonl> --user "User A" --since 7d

  python3 ads_change_history.py self-test
      Writes the 3 built-in sample fixtures to a temp dir, runs the full
      pipeline on each, asserts expected shape. This is the test suite —
      no external test framework needed.
"""
import argparse
import csv
import hashlib
import json
import re
import string
import sys
import webbrowser
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta, timezone as dt_timezone
from difflib import get_close_matches
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROFILES_DIR = SCRIPT_DIR / "mapping-profiles"
EMPTY_FRACTION_ABORT_THRESHOLD = 0.30
SCALE_WARN_THRESHOLD = 150_000

# =====================================================================
# HEADER ALIASES — canonical schema + known source formats
# =====================================================================
HEADER_ALIASES = {
    "canonical_fields": {
        # Documentation only — kept in sync with check_hard_required(), the
        # actual enforcement, not read by it. Fixed 2026-08-18 (3rd audit
        # round, confirmed): this list used to name a "user_identifier" field
        # that doesn't exist anywhere else in this codebase and was never
        # enforced (user identity is a soft warning in soft_warnings() —
        # missing it just means actor_type defaults to "unknown", it doesn't
        # stop the pipeline); it also implied "account_name" alone and
        # "operation" were hard requirements, when the real rule is
        # account_name-OR-account_id and operation is inferred, not required.
        # This now names exactly the 4 things check_hard_required() actually
        # enforces — nothing softer, nothing stricter.
        "required": ["timestamp", "account_name_or_account_id", "resource_type", "change_info"],
        "all": [
            "change_id", "event_id", "source_event_id", "timestamp", "timezone", "timestamp_utc",
            "account_id", "account_name", "user_id", "user_name", "user_email",
            "client_type", "actor_type", "resource_type", "resource_path", "resource_id", "resource_name",
            "campaign_id", "campaign_name", "campaign_resource", "ad_group_id", "ad_group_name", "ad_group_resource",
            "operation", "operation_confidence", "field_name", "multi_field", "old_value", "new_value", "old_value_num",
            "new_value_num", "old_value_display", "new_value_display", "value_unit",
            "currency", "value_confidence", "category", "subcategory",
            "category_confidence", "source", "raw_summary",
        ],
    },
    "aliases": {
        "timestamp": ["Tarih", "Change Date", "Change Date/Time", "Date", "change_date_time", "Değişiklik Tarihi", "Date & time", "Change date", "Zaman"],
        "account_id": ["Hesap ID", "Account ID", "Customer ID", "customer_id", "external_customer_id"],
        "account_name": ["Hesap Adı", "Account", "Account name", "Client", "Hesap"],
        # Fixed 2026-08-18 (found via a synthetic-dataset dry run, not a
        # pasted audit): "user_name" itself was missing from its own alias
        # list — user_email/client_type both already self-reference (their
        # alias lists include their own literal snake_case name), user_name
        # didn't. A source using that literal column name (e.g. a JSON export
        # naming an automation script via "user_name") mapped nothing —
        # actor_type silently fell back to "unknown" for every such row
        # instead of correctly resolving to "automation".
        "user_name": ["user_name", "Kullanıcı", "Changed by", "User", "Değiştiren", "Değiştiren Kullanıcı"],
        "user_email": ["user_email", "Email", "Kullanıcı E-postası", "User email", "E-posta"],
        "client_type": ["client_type", "Client", "İstemci Türü", "Client type"],
        "resource_type": ["change_resource_type", "Item type", "Değişiklik Tipi", "Change type", "Level", "Resource type"],
        # change_resource_name is a full resource PATH (e.g.
        # "customers/1/campaignBudgets/456"), not a bare ID — mapped to
        # resource_path, not resource_id. Fixed 2026-08-18 (was semantically
        # wrong before: resource_id used to be the thing that's actually a
        # path). A short resource_id is derived from the path's tail when no
        # separate bare-ID column exists — see normalize_changes().
        "resource_path": ["change_resource_name", "Resource path", "resource_path"],
        "resource_id": ["Resource ID", "resource_id"],
        # ChangeEvent's own top-level "resource_name" is the EVENT's identity
        # (customers/{id}/changeEvents/{ts}~{cmd}~{mutate}), verified against
        # Google's own sample query docs — distinct from change_resource_name,
        # which identifies the resource that changed. None of the other known
        # sources use a bare "resource_name" column, so this alias is
        # effectively API-source-specific.
        "source_event_id": ["resource_name"],
        "campaign_id": ["Campaign ID", "Kampanya ID", "campaign.id"],
        "campaign_name": ["Campaign", "Kampanya", "campaign.name"],
        "ad_group_id": ["Ad group ID", "Reklam Grubu ID", "ad_group.id"],
        "ad_group_name": ["Ad group", "Reklam Grubu", "ad_group.name"],
        "operation": ["resource_change_operation", "Change", "Değişiklik", "Operation"],
        "field_name": ["changed_fields", "Field", "Alan", "Değişen Alan"],
        "old_value": ["old_resource", "Eski Teknik Veri", "Before", "Old value", "Önceki", "Old"],
        "new_value": ["new_resource", "Yeni Teknik Veri", "After", "New value", "Yeni", "New"],
        "raw_summary": ["Değişiklik Özeti (Nereden -> Nereye)", "Değişiklik Özeti", "Change summary", "Summary", "Description"],
    },
    "value_aliases": {
        "operation": {
            "CREATE": ["CREATE", "Created", "Oluşturuldu", "Eklendi", "Added", "New"],
            "UPDATE": ["UPDATE", "Updated", "Değişti", "Changed", "Değiştirildi"],
            "REMOVE": ["REMOVE", "REMOVED", "DELETE", "Deleted", "Removed", "Silindi", "Kaldırıldı"],
        },
        "status": {
            "ENABLED": ["ENABLED", "Enabled", "Etkin", "Active", "1"],
            "PAUSED": ["PAUSED", "Paused", "Duraklatıldı", "Duraklatildi", "0"],
            "REMOVED": ["REMOVED", "Removed", "Kaldırıldı", "Silindi", "2"],
        },
    },
    "known_sources": {
        "legacy_summary_tr": {
            "confidence": "verified_from_user_file_2026-08-17",
            "header_set": ["Hesap Adı", "Hesap ID", "Tarih", "Kullanıcı", "Değişiklik Tipi", "Kampanya", "Reklam Grubu", "Değişiklik Özeti (Nereden -> Nereye)", "Eski Teknik Veri", "Yeni Teknik Veri"],
        },
        "google_ads_api_change_event_json": {
            "confidence": "verified_from_google_docs_2026-08-17",
            "header_set": ["change_date_time", "customer_id", "resource_name", "user_email", "client_type", "change_resource_type", "change_resource_name", "resource_change_operation", "changed_fields", "old_resource", "new_resource"],
            "note": "customer_id is NOT a ChangeEvent field in Google's API — a query is always scoped to one customer via the request path, not a selectable column. A real fetch adapter must attach customer_id to each row itself before this mapping applies. resource_name (verified via Google's official get-change-details sample query, 2026-08-18) is the ChangeEvent's OWN identity — customers/{id}/changeEvents/{timestamp_micros}~{command_index}~{mutate_index} — distinct from change_resource_name, which identifies the resource that changed. Maps to canonical source_event_id.",
        },
        "google_ads_ui_export_en_guess": {
            "confidence": "unverified_low_confidence",
            "header_set": ["Date", "Changed by", "Campaign", "Ad group", "Change", "Item type"],
        },
    },
}

# =====================================================================
# CATEGORY RULES
# =====================================================================
CATEGORY_RULES = {
    "categories": {
        "Budget": ["Budget changed", "Budget created", "Budget removed"],
        "Bidding": ["Bid changed", "Strategy changed", "Target CPA", "Target ROAS", "Bid modifier changed"],
        "Keyword": ["Keyword added", "Keyword removed", "Negative keyword"],
        "Ad": ["Ad created", "Ad changed", "Ad removed"],
        "Asset": ["Asset added", "Asset changed", "Asset removed", "Asset set changed"],
        "Targeting": ["Location targeting changed", "Device targeting changed", "Ad schedule changed", "Language targeting changed"],
        "Audience": ["Audience added", "Audience removed", "Audience bid adjustment changed"],
        "Status": ["Enabled", "Paused", "Removed"],
        "Campaign": ["Campaign created", "Campaign name changed", "Campaign settings changed", "Campaign removed"],
        "AdGroup": ["Ad group created", "Ad group name changed", "Ad group removed"],
        "Conversion": ["Conversion action created", "Conversion action changed", "Conversion tracking changed"],
        "Feed": ["Feed created", "Feed changed", "Feed removed", "Feed item added", "Feed item changed", "Feed item removed", "Feed attached to campaign", "Feed attached to ad group"],
        "Other": ["Other"],
    },
    "structured_rules": [
        {"resource_type": "CAMPAIGN_BUDGET", "operation": "CREATE", "category": "Budget", "subcategory": "Budget created"},
        {"resource_type": "CAMPAIGN_BUDGET", "operation": "REMOVE", "category": "Budget", "subcategory": "Budget removed"},
        {"resource_type": "CAMPAIGN_BUDGET", "operation": "UPDATE", "category": "Budget", "subcategory": "Budget changed"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "cpc_bid", "category": "Bidding", "subcategory": "Bid changed"},
        {"resource_type": "AD_GROUP", "field_name_contains": "cpc_bid", "category": "Bidding", "subcategory": "Bid changed"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "bidding_strategy", "category": "Bidding", "subcategory": "Strategy changed"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "target_cpa", "category": "Bidding", "subcategory": "Target CPA"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "target_roas", "category": "Bidding", "subcategory": "Target ROAS"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "keyword", "operation": "CREATE", "field_name_not_contains": "negative", "category": "Keyword", "subcategory": "Keyword added"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "keyword", "operation": "CREATE", "field_name_contains": "negative", "category": "Keyword", "subcategory": "Negative keyword"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "keyword", "operation": "REMOVE", "category": "Keyword", "subcategory": "Keyword removed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "negative", "category": "Keyword", "subcategory": "Negative keyword"},
        {"resource_type_in": ["AD", "AD_GROUP_AD"], "operation": "CREATE", "category": "Ad", "subcategory": "Ad created"},
        {"resource_type_in": ["AD", "AD_GROUP_AD"], "operation": "UPDATE", "category": "Ad", "subcategory": "Ad changed"},
        {"resource_type_in": ["AD", "AD_GROUP_AD"], "operation": "REMOVE", "category": "Ad", "subcategory": "Ad removed"},
        {"resource_type_in": ["ASSET", "ASSET_SET", "ASSET_SET_ASSET", "CAMPAIGN_ASSET", "AD_GROUP_ASSET", "CUSTOMER_ASSET"], "operation": "CREATE", "category": "Asset", "subcategory": "Asset added"},
        {"resource_type_in": ["ASSET", "ASSET_SET", "ASSET_SET_ASSET", "CAMPAIGN_ASSET", "AD_GROUP_ASSET", "CUSTOMER_ASSET"], "operation": "UPDATE", "category": "Asset", "subcategory": "Asset changed"},
        {"resource_type_in": ["ASSET", "ASSET_SET", "ASSET_SET_ASSET", "CAMPAIGN_ASSET", "AD_GROUP_ASSET", "CUSTOMER_ASSET"], "operation": "REMOVE", "category": "Asset", "subcategory": "Asset removed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "location", "category": "Targeting", "subcategory": "Location targeting changed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "device", "category": "Targeting", "subcategory": "Device targeting changed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "ad_schedule", "category": "Targeting", "subcategory": "Ad schedule changed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "language", "category": "Targeting", "subcategory": "Language targeting changed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "user_list", "category": "Audience", "subcategory": "Audience added"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "user_list", "category": "Audience", "subcategory": "Audience added"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "bid_modifier", "category": "Audience", "subcategory": "Audience bid adjustment changed"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        {"resource_type": "AD_GROUP", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "AD_GROUP", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        {"resource_type": "AD_GROUP_AD", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "AD_GROUP_AD", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        # These two must come BEFORE the generic REMOVE catch-all below —
        # first-match-wins, and the catch-all previously intercepted every
        # CAMPAIGN/AD_GROUP removal into Status/Removed, making the
        # "Campaign removed"/"Ad group removed" categories declared in
        # CATEGORY_RULES["categories"] unreachable for structured data.
        # Fixed 2026-08-19 (2nd audit).
        {"resource_type": "CAMPAIGN", "operation": "REMOVE", "category": "Campaign", "subcategory": "Campaign removed"},
        {"resource_type": "AD_GROUP", "operation": "REMOVE", "category": "AdGroup", "subcategory": "Ad group removed"},
        {"operation": "REMOVE", "category": "Status", "subcategory": "Removed"},
        {"resource_type": "CAMPAIGN", "operation": "CREATE", "category": "Campaign", "subcategory": "Campaign created"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "name", "operation": "UPDATE", "category": "Campaign", "subcategory": "Campaign name changed"},
        {"resource_type": "CAMPAIGN", "operation": "UPDATE", "category": "Campaign", "subcategory": "Campaign settings changed"},
        {"resource_type": "AD_GROUP", "operation": "CREATE", "category": "AdGroup", "subcategory": "Ad group created"},
        {"resource_type": "AD_GROUP", "field_name_contains": "name", "operation": "UPDATE", "category": "AdGroup", "subcategory": "Ad group name changed"},
        # These two rules can only ever be reached via --extra-rules or a
        # non-ChangeEvent source: verified 2026-08-18 that none of Google's 21
        # real ChangeEventResourceType values contain "CONVERSION" —
        # conversion actions are a separate GAQL resource entirely outside
        # ChangeEvent's coverage, not something this codebase can map around.
        # Left in place (harmless, unreachable for API data) rather than
        # removed, since a --extra-rules source could still use this category.
        {"resource_type_contains": "CONVERSION", "operation": "CREATE", "category": "Conversion", "subcategory": "Conversion action created"},
        {"resource_type_contains": "CONVERSION", "operation": "UPDATE", "category": "Conversion", "subcategory": "Conversion action changed"},
        # Added 2026-08-18 (audit F-11): these 6 real ChangeEventResourceType
        # values had zero structured_rules coverage — any real event of these
        # types always needed manual category review before this fix.
        {"resource_type": "FEED", "operation": "CREATE", "category": "Feed", "subcategory": "Feed created"},
        {"resource_type": "FEED", "operation": "REMOVE", "category": "Feed", "subcategory": "Feed removed"},
        {"resource_type": "FEED", "operation": "UPDATE", "category": "Feed", "subcategory": "Feed changed"},
        {"resource_type": "FEED_ITEM", "operation": "CREATE", "category": "Feed", "subcategory": "Feed item added"},
        {"resource_type": "FEED_ITEM", "operation": "REMOVE", "category": "Feed", "subcategory": "Feed item removed"},
        {"resource_type": "FEED_ITEM", "operation": "UPDATE", "category": "Feed", "subcategory": "Feed item changed"},
        {"resource_type": "CAMPAIGN_FEED", "category": "Feed", "subcategory": "Feed attached to campaign"},
        {"resource_type": "AD_GROUP_FEED", "category": "Feed", "subcategory": "Feed attached to ad group"},
        {"resource_type": "AD_GROUP_BID_MODIFIER", "category": "Bidding", "subcategory": "Bid modifier changed"},
        {"resource_type": "CAMPAIGN_ASSET_SET", "category": "Asset", "subcategory": "Asset set changed"},
    ],
    "summary_text_rules": [
        {"pattern": r"^Yeni Kelime Eklendi", "category": "Keyword", "subcategory": "Keyword added"},
        {"pattern": r"^Negatif Kelime Eklendi", "category": "Keyword", "subcategory": "Negative keyword"},
        {"pattern": r"^(TBM Değişti|AdGroup TBM Değişti|Teklif Değişti)", "category": "Bidding", "subcategory": "Bid changed"},
        {"pattern": r"^(Strateji Değişti|Teklif Stratejisi)", "category": "Bidding", "subcategory": "Strategy changed"},
        {"pattern": r"^Bütçe Değişti", "category": "Budget", "subcategory": "Budget changed"},
        {"pattern": r"^Durum Değişti.*(ENABLED|Etkin)", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^Durum Değişti.*(PAUSED|Duraklat)", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^(Öğe Silindi|Kaldırıldı)", "category": "Status", "subcategory": "Removed"},
    ],
    "fallback": {"category": "Other", "subcategory": "Other"},
}

AUTOMATION_USER_PATTERNS = re.compile(r"(^ads-|system|automated|rule|script|bot|api)", re.IGNORECASE)
# Fixed 2026-08-18 (audit F-08/F-09): "SA360" never occurs in either real
# ChangeClientType enum value (they're "SEARCH_ADS_360_SYNC" and
# "SEARCH_ADS_360_POST" — "ADS_360", not "SA360") — that alternative was dead
# code, and its absence meant SEARCH_ADS_360_POST fell through to "unknown"
# while its sibling SEARCH_ADS_360_SYNC matched via "SYNC" alone, an
# unintended inconsistency within the same SA360 family. "ADS_360" replaces
# it and matches both real values.
AUTOMATION_CLIENT_TYPE = re.compile(r"(AUTOMATED|RULE|SCRIPT|RECOMMENDATION|BULK|SYNC|ADS_360)", re.IGNORECASE)
# GOOGLE_ADS_API deliberately excluded: a bare "made via the API" doesn't tell
# you whether a human ran the script or a service account/automation did.
# Calling it "human" was an earlier design call that turned out to be too
# confident — leave it "unknown" unless client_type OR the user_name pattern
# says otherwise. Fixed 2026-08-18.
HUMAN_CLIENT_TYPE = re.compile(r"(WEB_CLIENT|MOBILE_APP|EDITOR)", re.IGNORECASE)
SUMMARY_OLD_NEW_RE = re.compile(r"(-?\d[\d.,]*)\s*->\s*(-?\d[\d.,]*)")
# Second fallback for free-text summaries phrased as "... changed/increased/
# decreased from X to Y" instead of the "X -> Y" arrow SUMMARY_OLD_NEW_RE
# looks for. Tried only when the arrow pattern finds nothing. Not numeric-only
# on purpose (X/Y can be "ENABLED"/"PAUSED", a keyword, etc.) — old_value_num/
# new_value_num just stay None when normalize_number() can't parse the result,
# same as any other non-numeric old_value/new_value. Added 2026-08-18 after
# comparing against a similar public change-history tool's summary parser.
SUMMARY_CHANGED_FROM_TO_RE = re.compile(
    r"(?:changed|increased|decreased)\s+from\s+\"?([^\"\n]{1,60}?)\"?\s+to\s+\"?([^\"\n]{1,60}?)\"?(?=\n|$)",
    re.IGNORECASE,
)
# -? prefix added 2026-08-18 (3rd audit round, confirmed): without it, a
# negative value like "-150.00" or "-3,50" matched neither pattern (both
# required the string to START with a digit), fell through to
# normalize_number()'s catch-all branch, and had its separator silently
# stripped instead of converted — "-150.00" became -15000.0, not -150.0. The
# sign character sits before the digits either way, so adding "-?" is
# sufficient; the replace() calls downstream don't touch it.
DIGIT_GROUP_3 = re.compile(r"^-?\d{1,3}([.,]\d{3})+$")
DIGIT_GROUP_SHORT = re.compile(r"^-?\d+[.,]\d{1,2}$")


# =====================================================================
# STAGE 1-2: SOURCE DETECTION + VALIDATION
# =====================================================================
def normalize_header(h):
    return " ".join(h.strip().lower().split())


def read_rows(path):
    """Raises ValueError with a clear message for every malformed-input case
    this codebase is expected to handle (missing file, invalid JSON, unreadable
    encoding) — never a raw stdlib exception. Fixed 2026-08-18 (audit F-16,
    F-17, F-18): all three previously escaped as unhandled
    FileNotFoundError / JSONDecodeError / 'I/O operation on closed file'.
    Fixed 2026-08-18 (3rd audit round, confirmed): a JSON array of
    non-object values (e.g. [1, 2, 3]) reached rows[0].keys() and crashed
    with an unhandled AttributeError — the one case that had slipped past
    the "never a raw stdlib exception" contract this docstring already
    promised. A dict lacking both known wrapper keys ("results"/
    "changeEvents") already surfaced as a clear downstream error via
    validate_source()'s "no headers found" check; this makes that specific
    case's message name the actual problem instead of just its symptom."""
    path = Path(path)
    if not path.exists():
        raise ValueError(f"Input file not found: {path}")

    if path.suffix.lower() == ".json":
        with open(path, encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Input file is not valid JSON: {path} ({e})")
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            if "results" in data:
                rows = data["results"]
            elif "changeEvents" in data:
                rows = data["changeEvents"]
            else:
                raise ValueError(
                    f"Input file is a JSON object without a recognized 'results' or "
                    f"'changeEvents' key: {path}. Top-level keys found: {list(data.keys())}"
                )
        else:
            raise ValueError(f"Input file's top-level JSON value must be a list or object, got {type(data).__name__}: {path}")
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                raise ValueError(f"Input file's rows must be JSON objects, got {type(row).__name__} at index {i}: {path}")
        # Found 2026-08-18 via a synthetic-dataset dry run (not a pasted
        # audit): headers used to come from ONLY rows[0].keys(). A perfectly
        # realistic heterogeneous export (e.g. automation-authored rows
        # carrying "user_name", human-authored rows carrying "user_email"
        # instead, same file) silently lost whichever key wasn't present on
        # the very first row — that key was never even offered to
        # try_alias_match, no matter how many other rows had it. Union of
        # keys across every row, first-seen order preserved, fixes this for
        # any column present on at least one row.
        headers, seen = [], set()
        for row in rows:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    headers.append(k)
        return headers, rows

    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    for enc in ("utf-8-sig", "cp1254", "latin-1"):
        try:
            with open(path, encoding=enc, newline="") as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                rows = list(reader)
                # Fixed 2026-08-18 (audit F-16): reading .fieldnames was
                # previously done AFTER this `with` block closed the file —
                # harmless when the file has content (DictReader already
                # populated .fieldnames while iterating), but an empty file
                # never triggers that lazy population, and accessing
                # .fieldnames afterward on the closed handle raised
                # "I/O operation on closed file." Reading it here, still
                # inside the open block, fixes both cases uniformly.
                headers = reader.fieldnames or []
            if "�" in " ".join(headers):
                continue
            return headers, rows
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode {path} with utf-8-sig, cp1254, or latin-1.")


def detect_duplicate_headers(headers):
    """Fixed 2026-08-18 (audit F-19): csv.DictReader silently keeps only the
    last column's value per duplicate header name — this codebase previously
    added no detection of its own on top of that, so a genuine duplicate
    column lost data with zero diagnostic. Returns the list of normalized
    header names that appear more than once (empty list if none)."""
    seen, dupes = {}, []
    for h in headers:
        nh = normalize_header(h)
        seen[nh] = seen.get(nh, 0) + 1
    return [h for h, count in seen.items() if count > 1]


def fingerprint(headers):
    key = "|".join(sorted(normalize_header(h) for h in headers))
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def try_fingerprint_match(headers):
    fp = fingerprint(headers)
    profile_path = PROFILES_DIR / f"{fp}.json"
    if profile_path.exists():
        with open(profile_path, encoding="utf-8") as f:
            return json.load(f)
    return None


# Google's literal ChangeEvent.campaign / ChangeEvent.ad_group fields are
# lowercase snake_case strings holding a resource NAME/path (e.g. "campaign":
# "customers/1/campaigns/456"), not a display name — verified against
# Google's proto definitions 2026-08-18 (audit F-03). A UI-export-style
# "Campaign" column (Title Case, human-readable display name) is a DIFFERENT,
# unrelated convention that happens to collide with it once case is folded by
# normalize_header(). Checked as an exact, case-SENSITIVE match before the
# general case-insensitive alias lookup — case is the only reliable signal
# distinguishing "the API's own field" from "a human-authored column titled
# the same word." A prior version let these collide, causing the API's
# resource path to be stored in campaign_name (a field meant for display
# names) and losing ad_group entirely (its Title Case alias "Ad group" never
# matched the underscored "ad_group" to begin with).
#
# Fixed 2026-08-18 (3rd audit round, confirmed): case alone turned out not to
# be reliable enough — a perfectly ordinary source with a literal lowercase
# "campaign" column (e.g. any JSON export using snake_case keys for display
# fields, not just Google's own API) collided the OTHER way: it got forced
# into campaign_resource, and campaign_name/campaign_id were lost entirely for
# that row, with no fallback. The exact-case match is now only a CANDIDATE
# signal — it's confirmed by sampling the column's actual values. Google's own
# field always holds a slash-separated path ("customers/1/campaigns/456"); a
# plain display-name column never does. When the sample doesn't look like a
# path, the header falls through to the normal case-insensitive alias lookup
# instead (where literal "campaign" already resolves to campaign_name, same as
# "Campaign").
API_EXACT_CASE_RESOURCE_ALIASES = {
    "campaign": "campaign_resource",
    "ad_group": "ad_group_resource",
}


def _column_looks_like_resource_path(rows, header, n=5):
    """True only if at least one of the first N non-empty sampled values for
    this header contains '/' — the one structural trait a Google Ads resource
    path always has and a plain display name never does. Empty-string default
    on a missing/all-empty column: a genuinely empty column gives no evidence
    either way, so it doesn't get misclassified as a resource path."""
    if not rows:
        return False
    checked = 0
    for r in rows:
        v = r.get(header)
        if v is None or not str(v).strip():
            continue
        if "/" in str(v):
            return True
        checked += 1
        if checked >= n:
            break
    return False


def try_alias_match(headers, rows=None):
    alias_lookup = {}
    for canonical, alias_list in HEADER_ALIASES["aliases"].items():
        for a in alias_list:
            alias_lookup[normalize_header(a)] = canonical
    mapping, unmatched = {}, []
    for h in headers:
        if h in API_EXACT_CASE_RESOURCE_ALIASES and _column_looks_like_resource_path(rows, h):
            mapping[API_EXACT_CASE_RESOURCE_ALIASES[h]] = h
            continue
        nh = normalize_header(h)
        if nh in alias_lookup:
            mapping[alias_lookup[nh]] = h
        else:
            unmatched.append(h)
    return mapping, unmatched


def detect_known_source(headers):
    """Fixed 2026-08-19 (2nd audit, P3): known_sources was pure documentation
    — never consulted at runtime, so every Layer-2 match reported the generic
    source_label "alias_match" regardless of which registered format it
    actually was (this is also why the dashboard's API-caveat banner had to
    detect API data via source_event_id presence instead of the source label
    — the label itself carried no information). Returns a known_sources key
    when ALL of that entry's declared headers are present (case/whitespace-
    normalized) among the input's actual headers, else None. Checked in the
    dict's declared order — the two 'verified' entries before the one
    'unverified_low_confidence' entry."""
    normalized_input = {normalize_header(h) for h in headers}
    for key, entry in HEADER_ALIASES["known_sources"].items():
        declared = {normalize_header(h) for h in entry["header_set"]}
        if declared <= normalized_input:
            return key
    return None


def check_hard_required(mapping):
    missing = []
    if "timestamp" not in mapping:
        missing.append("timestamp")
    if "account_name" not in mapping and "account_id" not in mapping:
        missing.append("account_ref (account_name or account_id)")
    if "resource_type" not in mapping:
        missing.append("resource_type")
    has_structured = "old_value" in mapping and "new_value" in mapping
    has_summary = "raw_summary" in mapping
    if not has_structured and not has_summary:
        missing.append("change_info ((old_value+new_value) or raw_summary)")
    return missing


def soft_warnings(mapping):
    warnings = []
    if "user_name" not in mapping and "user_email" not in mapping:
        warnings.append("No user identity column found — actor_type will default to 'unknown' unless client_type infers automation.")
    if "operation" not in mapping:
        warnings.append("No structured 'operation' column — inferred from raw_summary text, else defaults to UPDATE.")
    if "campaign_name" not in mapping and "campaign_id" not in mapping:
        warnings.append("No campaign reference column — campaign-level views will be empty.")
    return warnings


def suggest_fuzzy_mapping(unmatched):
    all_aliases, alias_to_canonical = [], {}
    for canonical, alias_list in HEADER_ALIASES["aliases"].items():
        for a in alias_list:
            all_aliases.append(a)
            alias_to_canonical[a] = canonical
    suggestions = {}
    for h in unmatched:
        matches = get_close_matches(h, all_aliases, n=1, cutoff=0.6)
        if matches:
            suggestions[h] = {"suggested_canonical_field": alias_to_canonical[matches[0]], "matched_against": matches[0], "confidence": "low — confirm before using"}
    return suggestions


def sample_values(rows, header, n=3):
    vals = []
    for r in rows:
        v = r.get(header)
        if v is not None and str(v).strip():
            vals.append(str(v))
        if len(vals) >= n:
            break
    return vals


def empty_fraction(rows, mapping, canonical_field):
    header = mapping.get(canonical_field)
    if header is None:
        return None
    empty = sum(1 for r in rows if not str(r.get(header, "")).strip())
    return empty / len(rows) if rows else 0.0


def validate_source(input_path, mapping_file=None, force=False, save_profile=True):
    """Returns dict. status=ok includes 'mapping'. Any other status means STOP —
    caller must resolve with a human before continuing."""
    try:
        headers, rows = read_rows(input_path)
    except ValueError as e:
        # Fixed 2026-08-18 (audit F-16/F-17/F-18): read_rows() used to let
        # FileNotFoundError / JSONDecodeError / a closed-file I/O error
        # escape uncaught. It now always raises ValueError with a clear
        # message, and this is the single place that turns that into the
        # same structured status every other failure mode already used.
        return {"status": "error", "message": str(e)}
    if not headers:
        return {"status": "error", "message": "No headers/columns found in input."}

    dup_headers = detect_duplicate_headers(headers)

    # Fixed 2026-08-18 (found via a synthetic-dataset dry run, not a pasted
    # audit): check_hard_required() used to run ONLY on the freshly-computed
    # try_alias_match() path. A --mapping-file (or a cached fingerprint
    # profile — itself only ever created by one of these same paths) that
    # genuinely omits a hard-required canonical field, e.g. its source has no
    # resource_type column at all, sailed through as status "ok" with 100%
    # "Other" categorization and 0 tracked accounts — no error, no warning,
    # just silently useless output. The check now runs once, after mapping is
    # resolved, regardless of which of the 3 paths produced it.
    unmatched = []
    source_label = "unmapped_confirmed"
    if mapping_file:
        with open(mapping_file, encoding="utf-8") as f:
            confirmed = json.load(f)
        mapping = confirmed.get("mapping", confirmed)
        source_label = confirmed.get("source_label", "user_confirmed")
    else:
        profile = try_fingerprint_match(headers)
        if profile:
            mapping = profile["mapping"]
            source_label = profile.get("source_label", "fingerprint_match")
        else:
            mapping, unmatched = try_alias_match(headers, rows)
            source_label = detect_known_source(headers) or "alias_match"

    missing = check_hard_required(mapping)
    if missing:
        return {
            "status": "needs_mapping",
            "input": str(input_path),
            "fingerprint": fingerprint(headers),
            "auto_mapped": mapping,
            "missing_required": missing,
            "unmapped_headers": unmatched,
            "sample_values": {h: sample_values(rows, h) for h in unmatched},
            "suggested_mapping": suggest_fuzzy_mapping(unmatched),
            "instructions": "Show unmapped_headers + sample_values to the user in chat. Ask which canonical field each maps to (see module docstring: CANONICAL SCHEMA). Write the confirmed mapping to a JSON file ({'source_label': ..., 'mapping': {canonical: header}}) and re-run with --mapping-file <file>. If the source genuinely has no column for a missing field (e.g. no resource_type at all), that field cannot be satisfied by mapping alone — this input can't proceed until the data itself provides it.",
        }

    review_issues = []
    for canonical in ("timestamp", "resource_type"):
        frac = empty_fraction(rows, mapping, canonical)
        if frac is not None and frac > EMPTY_FRACTION_ABORT_THRESHOLD:
            review_issues.append({"field": canonical, "empty_fraction": round(frac, 3), "total_rows": len(rows)})
    for cf in ("account_name", "account_id", "old_value", "raw_summary"):
        if cf in mapping:
            frac = empty_fraction(rows, mapping, cf)
            if frac is not None and frac > EMPTY_FRACTION_ABORT_THRESHOLD:
                review_issues.append({"field": cf, "empty_fraction": round(frac, 3), "total_rows": len(rows)})

    if review_issues and not force:
        return {
            "status": "needs_review",
            "input": str(input_path),
            "message": "More than 30% of rows are missing a value in a required field even though the column exists — usually a wrong mapping, not genuinely sparse data. Confirm with the user; re-run with force=True if the data really is this sparse.",
            "issues": review_issues,
            "mapping": mapping,
        }

    if save_profile:
        PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        fp = fingerprint(headers)
        with open(PROFILES_DIR / f"{fp}.json", "w", encoding="utf-8") as f:
            json.dump({"source_label": source_label, "mapping": mapping, "headers_seen": headers}, f, indent=2, ensure_ascii=False)

    return {
        "status": "ok", "input": str(input_path), "row_count": len(rows),
        "fingerprint": fingerprint(headers), "source_label": source_label,
        "mapping": mapping,
        "warnings": soft_warnings(mapping) + ([f"Duplicate column header(s) detected: {dup_headers} — only the last occurrence of each is used (stdlib csv.DictReader behavior), earlier columns of the same name are silently discarded."] if dup_headers else []),
    }


# =====================================================================
# STAGE 3: NORMALIZATION
# =====================================================================
def normalize_number(raw, decimal_style):
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    s = re.sub(r"[^\d.,-]", "", s)
    if not s:
        return None
    has_dot, has_comma = "." in s, "," in s
    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif has_dot or has_comma:
        sep = "." if has_dot else ","
        if DIGIT_GROUP_SHORT.match(s):
            s = s.replace(sep, ".")
        elif DIGIT_GROUP_3.match(s):
            if decimal_style in ("TR", "US"):
                s = s.replace(sep, "") if not (decimal_style == "US" and sep == ".") else s
            else:
                raise ValueError(f"ambiguous_decimal:{raw}")
        else:
            s = s.replace(sep, "")
    try:
        return float(s)
    except ValueError:
        return None


def decimal_ambiguity_present(values):
    for v in values:
        if v is None:
            continue
        s = re.sub(r"[^\d.,-]", "", str(v))
        has_dot, has_comma = "." in s, "," in s
        if has_dot and has_comma:
            continue
        if (has_dot or has_comma) and DIGIT_GROUP_3.match(s) and not DIGIT_GROUP_SHORT.match(s):
            return True
    return False


def detect_date_format(values):
    sample = [str(v) for v in values if v][:200]
    if not sample:
        return "ISO"
    if all(re.match(r"^\d{4}-\d{2}-\d{2}", s) for s in sample):
        return "ISO"
    for s in sample:
        m = re.match(r"^(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})", s)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            if a > 12:
                return "DMY"
            if b > 12:
                return "MDY"
    return None


DMY_OFFSET_RE = re.compile(
    r"^(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})[ T]?(\d{1,2}:\d{2}(:\d{2})?)?\s*(Z|[+-]\d{2}:?\d{2})?$"
)


def parse_timestamp(raw, fmt):
    """Returns a datetime — timezone-AWARE if the source string itself carried
    an offset (e.g. '+03:00' or 'Z'), naive otherwise. Fixed 2026-08-18: the
    previous version tried a truncated strptime first, which could silently
    match a prefix of an offset-bearing string and drop the offset. Fixed
    2026-08-19 (2nd audit, two further gaps found in that same fallback):
    (1) the truncating strptime fallback (`s[:len(f)+2]`) accepted garbage
    trailing the valid portion — "2026-08-01 10:00:00JUNK" silently parsed by
    ignoring "JUNK", and "...+03:00JUNK" silently parsed by ignoring BOTH the
    junk and the now-untruncated offset. The fallback is now a strict, whole-
    string match — anything left over after the format is consumed is a
    parse failure, not a truncation. (2) the DMY/MDY branch never attempted
    to capture a trailing offset at all (the regex simply didn't have a group
    for one) — a valid DMY timestamp with an offset silently lost it even
    though the ISO branch's offset-preservation logic existed right next to
    it. The regex now captures an optional trailing offset/Z and applies it
    the same way the ISO branch does."""
    s = str(raw).strip()
    if fmt == "ISO":
        iso_candidate = s.replace("Z", "+00:00") if s.endswith("Z") else s
        # Fixed 2026-08-18 (3rd audit round, confirmed): a slash-separated
        # ISO-shaped date ("2026/08/01...") failed to parse even with
        # explicit --date-format ISO — fromisoformat/strptime both require
        # '-' between the year/month/day. DMY/MDY already accept both '/'
        # and '.' via DMY_OFFSET_RE; ISO was the odd one out. Only the first
        # 2 slashes (the date separators) are ever present in a genuine
        # slash-ISO string, so this is unambiguous when the year-first shape
        # is confirmed first.
        if re.match(r"^\d{4}/\d{2}/\d{2}", iso_candidate):
            iso_candidate = iso_candidate.replace("/", "-", 2)
        try:
            return datetime.fromisoformat(iso_candidate)
        except ValueError:
            pass
        for f in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(iso_candidate, f)  # whole-string match — no silent truncation of trailing junk
            except ValueError:
                continue
        raise ValueError(f"unparseable ISO timestamp: {raw}")
    m = DMY_OFFSET_RE.match(s)
    if not m:
        raise ValueError(f"unparseable timestamp: {raw}")
    a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    day, month = (a, b) if fmt == "DMY" else (b, a)
    hh = mm = ss = 0
    if m.group(4):
        parts = m.group(4).split(":")
        hh, mm = int(parts[0]), int(parts[1])
        ss = int(parts[2]) if len(parts) > 2 else 0
    if y < 100:
        y += 2000
    dt = datetime(y, month, day, hh, mm, ss)
    offset_str = m.group(6)
    if offset_str:
        if offset_str == "Z":
            return dt.replace(tzinfo=dt_timezone.utc)
        sign = 1 if offset_str[0] == "+" else -1
        digits = offset_str[1:].replace(":", "")
        off_h, off_m = int(digits[:2]), int(digits[2:4])
        return dt.replace(tzinfo=dt_timezone(sign * timedelta(hours=off_h, minutes=off_m)))
    return dt


def infer_operation(raw_summary):
    if not raw_summary:
        return "UPDATE"
    s = raw_summary.lower()
    if any(k in s for k in ("eklendi", "added", "created", "oluşturuldu")):
        return "CREATE"
    if any(k in s for k in ("silindi", "kaldırıldı", "removed", "deleted")):
        return "REMOVE"
    return "UPDATE"


def derive_actor_type(user_name, client_type):
    if client_type:
        if AUTOMATION_CLIENT_TYPE.search(client_type):
            return "automation"
        if HUMAN_CLIENT_TYPE.search(client_type):
            return "human"
    if user_name and AUTOMATION_USER_PATTERNS.search(user_name):
        return "automation"
    if user_name:
        return "human"
    return "unknown"


def apply_value_alias(field, raw):
    if not raw:
        return raw
    table = HEADER_ALIASES.get("value_aliases", {}).get(field, {})
    raw_norm = str(raw).strip()
    for canonical, alts in table.items():
        if raw_norm in alts or raw_norm.upper() == canonical:
            return canonical
    return raw_norm


def make_change_id(account_ref, ts, user_ref, resource_ref, field_name, old_value, new_value, source_event_id=None):
    """Fixed 2026-08-18 (audit F-01): source_event_id is now part of the hash.
    Before this fix, two genuinely DIFFERENT real events (distinct, verified
    source_event_id) that happened to share every other hash input collapsed
    into the same change_id — dedupe_rows() then silently discarded one of
    them as a "duplicate", which it was not. Including source_event_id (when
    the source provides one) guarantees two distinct real events never
    collide, while sources with no event identity (CSV/legacy) keep the
    original content-hash behavior unchanged."""
    key = "|".join(str(x or "") for x in (account_ref, ts, user_ref, resource_ref, field_name, old_value, new_value, source_event_id))
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:20]


def derive_resource_id_from_path(resource_path):
    """'customers/1/campaignBudgets/456' -> '456'. Best-effort only — used
    when no separate bare-ID column exists."""
    if not resource_path or "/" not in str(resource_path):
        return None
    return str(resource_path).rsplit("/", 1)[-1]


def normalize_changes(input_path, mapping, source_label, date_format=None, decimal_style=None, tz="unknown"):
    """Returns dict: status 'ok' with 'rows' (list of canonical dicts), or a
    needs_date_format / needs_decimal_style status to stop and ask."""
    try:
        headers, rows = read_rows(input_path)
    except ValueError as e:
        # Defense in depth for callers that invoke this directly without
        # going through validate_source() first — see F-16/F-17/F-18 fix note there.
        return {"status": "error", "message": str(e)}

    ts_header = mapping.get("timestamp")
    ts_values = [r.get(ts_header) for r in rows] if ts_header else []
    resolved_date_fmt = date_format or detect_date_format(ts_values)
    if resolved_date_fmt is None:
        return {"status": "needs_date_format", "message": "Timestamp column is ambiguous (DD/MM vs MM/DD). Ask the user, then pass date_format='DMY'|'MDY'|'ISO'.", "sample_values": [v for v in ts_values if v][:5]}

    numeric_cols = [mapping[c] for c in ("old_value", "new_value") if c in mapping]
    numeric_values = [r.get(h) for h in numeric_cols for r in rows]
    if decimal_style is None and decimal_ambiguity_present(numeric_values):
        return {"status": "needs_decimal_style", "message": "Numeric columns have an ambiguous separator (e.g. '150.000'). Ask the user, then pass decimal_style='TR'|'US'.", "sample_values": [v for v in numeric_values if v][:5]}
    resolved_decimal_style = decimal_style or "TR"

    out_rows, unparseable = [], 0
    for r in rows:
        def g(field):
            h = mapping.get(field)
            return r.get(h) if h else None

        raw_ts = g("timestamp")
        try:
            dt = parse_timestamp(raw_ts, resolved_date_fmt)
        except ValueError:
            unparseable += 1
            continue

        ts_utc = None
        row_timezone = tz
        if dt.tzinfo is not None:
            # The source string carried its own offset (e.g. '+03:00' or 'Z') —
            # that's more authoritative than the --timezone flag, use it directly
            # and don't let a mismatched flag override it.
            ts_utc = dt.astimezone(dt_timezone.utc).isoformat()
            row_timezone = dt.isoformat()[19:] or "+00:00"  # the offset portion, e.g. '+03:00'
            dt = dt.replace(tzinfo=None)  # keep dt naive from here on for local-time fields
        elif tz != "unknown":
            try:
                from zoneinfo import ZoneInfo
                ts_utc = dt.replace(tzinfo=ZoneInfo(tz)).astimezone(dt_timezone.utc).isoformat()
            except Exception:
                ts_utc = None

        raw_summary = g("raw_summary")
        old_value, new_value = g("old_value"), g("new_value")
        value_confidence = "structured"
        if (not old_value and not new_value) and raw_summary:
            m = SUMMARY_OLD_NEW_RE.search(str(raw_summary))
            if m:
                old_value, new_value = m.group(1), m.group(2)
                value_confidence = "parsed_from_summary"
            else:
                m2 = SUMMARY_CHANGED_FROM_TO_RE.search(str(raw_summary))
                if m2:
                    old_value, new_value = m2.group(1).strip().rstrip("."), m2.group(2).strip().rstrip(".")
                    value_confidence = "parsed_from_summary"

        old_num = new_num = None
        try:
            old_num = normalize_number(old_value, resolved_decimal_style)
            new_num = normalize_number(new_value, resolved_decimal_style)
        except ValueError:
            pass

        # Google Ads API amounts are in micros (1,000,000 = 1 unit) — a raw
        # "150000000" is unreadable and was shown as-is before this fix.
        # Divide when the field name says micros; never invent a currency
        # symbol since none is known here.
        # Fixed 2026-08-18 (audit F-02): changed_fields is a FieldMask in
        # Google's model — a repeated field. A row whose source gives it as a
        # real JSON list (e.g. ["status","name"]) crashed match_structured()
        # ('list' object has no attribute 'lower') since field_name was
        # assumed to always be a string. Joined into one string here rather
        # than split into per-field rows: old_value/new_value in this script's
        # model represent the WHOLE changed resource as one flat string, not a
        # per-field breakdown, so splitting into N rows would force the same
        # whole-resource old/new value onto each field — an invented
        # per-field value this codebase doesn't actually have. multi_field
        # flags this rather than silently presenting it as a single clean field.
        raw_field_name = g("field_name")
        multi_field = isinstance(raw_field_name, list)
        field_name = ", ".join(str(x) for x in raw_field_name) if multi_field else raw_field_name
        is_micros_field = bool(field_name) and "micros" in str(field_name).lower()
        old_value_display = round(old_num / 1_000_000, 2) if is_micros_field and old_num is not None else old_num
        new_value_display = round(new_num / 1_000_000, 2) if is_micros_field and new_num is not None else new_num

        # Fixed 2026-08-18 (audit F-07): a present-but-empty operation value
        # ("" — distinct from the column being absent) was silently treated
        # the same as missing and routed into infer_operation(), whose final
        # fallback is "UPDATE" — indistinguishable afterward from a genuine
        # UPDATE. operation_confidence now records which path produced the
        # value: "explicit" (the source said so, including literal
        # UNKNOWN/UNSPECIFIED strings, which were already preserved
        # un-coerced before this fix) vs "inferred" (this script guessed).
        raw_operation = g("operation")
        operation = apply_value_alias("operation", raw_operation) if raw_operation else infer_operation(raw_summary)
        operation_confidence = "explicit" if raw_operation else "inferred"
        user_name, user_email, client_type = g("user_name"), g("user_email"), g("client_type")
        actor_type = derive_actor_type(user_name or user_email, client_type)
        account_id = g("account_id")
        account_name = g("account_name") or account_id
        resource_path = g("resource_path")
        resource_id = g("resource_id") or derive_resource_id_from_path(resource_path)
        resource_name = g("resource_name")
        source_event_id = g("source_event_id")

        # campaign_resource/ad_group_resource are the API's own attributed-
        # resource fields (resource-name/path strings) — a bare ID is derived
        # from the path tail as a fallback only; never used to populate
        # campaign_name/ad_group_name, which are reserved for actual display
        # names and would otherwise show a raw path where a name belongs.
        campaign_resource = g("campaign_resource")
        ad_group_resource = g("ad_group_resource")
        campaign_id = g("campaign_id") or derive_resource_id_from_path(campaign_resource)
        ad_group_id = g("ad_group_id") or derive_resource_id_from_path(ad_group_resource)

        # Fixed 2026-08-18 (3rd audit round, confirmed): the hash used to key
        # off raw_ts (the source's own un-normalized timestamp string). Two
        # files representing the exact same event with differently-formatted
        # timestamps ("2026-08-01 09:00:00" vs "2026-08-01T09:00:00") hashed
        # differently and dedupe_rows() never recognized them as the same
        # change. ts_utc/timestamp_iso (same preference order as chrono_key())
        # is the normalized identity of "when", independent of source string
        # formatting.
        change_id = make_change_id(account_id or account_name, ts_utc or dt.isoformat(), user_email or user_name, resource_id or resource_path or resource_name or g("campaign_name"), field_name, old_value, new_value, source_event_id=source_event_id)
        event_id = source_event_id or change_id

        out_rows.append({
            "change_id": change_id, "event_id": event_id, "source_event_id": source_event_id, "timestamp": str(raw_ts),
            "timestamp_iso": dt.isoformat(), "timezone": row_timezone, "timestamp_utc": ts_utc,
            "account_id": account_id, "account_name": account_name, "user_id": None,
            "user_name": user_name, "user_email": user_email, "client_type": client_type,
            "actor_type": actor_type, "resource_type": g("resource_type"), "resource_path": resource_path, "resource_id": resource_id,
            "resource_name": resource_name, "campaign_id": campaign_id, "campaign_name": g("campaign_name"),
            "campaign_resource": campaign_resource, "ad_group_resource": ad_group_resource,
            "ad_group_id": ad_group_id, "ad_group_name": g("ad_group_name"), "operation": operation, "operation_confidence": operation_confidence,
            "field_name": field_name, "multi_field": multi_field, "old_value": old_value, "new_value": new_value,
            "old_value_num": old_num, "new_value_num": new_num,
            "old_value_display": old_value_display, "new_value_display": new_value_display,
            "value_unit": "units (converted from micros)" if is_micros_field else None, "currency": None,
            "value_confidence": value_confidence, "category": None, "subcategory": None,
            "category_confidence": None, "source": source_label, "raw_summary": raw_summary,
        })

    return {"status": "ok", "rows": out_rows, "rows_skipped_unparseable_date": unparseable, "date_format_used": resolved_date_fmt, "decimal_style_used": resolved_decimal_style}


def dedupe_rows(rows):
    """change_id is a content hash — identical (account, timestamp, user,
    resource, field, old, new) always produces the same id, on purpose, so a
    re-imported/overlapping file collapses instead of doubling. Until this
    function existed that collapse never actually happened: change_id was
    computed but nothing filtered on it. Fixed 2026-08-18. Returns
    (deduped_rows, duplicate_count)."""
    seen = set()
    out = []
    duplicates = 0
    for r in rows:
        cid = r["change_id"]
        if cid in seen:
            duplicates += 1
            continue
        seen.add(cid)
        out.append(r)
    return out, duplicates


# =====================================================================
# STAGE 4: CATEGORIZATION
# =====================================================================
def match_structured(row, extra_rules=None):
    rules = (extra_rules or []) + CATEGORY_RULES["structured_rules"]
    rt = (row.get("resource_type") or "").upper()
    fn = (row.get("field_name") or "").lower()
    op = (row.get("operation") or "").upper()
    nv = (row.get("new_value") or "").upper()
    for rule in rules:
        if "resource_type" in rule and rule["resource_type"] != rt:
            continue
        if "resource_type_in" in rule and rt not in rule["resource_type_in"]:
            continue
        if "resource_type_contains" in rule and rule["resource_type_contains"].upper() not in rt:
            continue
        if "field_name_contains" in rule and rule["field_name_contains"].lower() not in fn:
            continue
        if "field_name_not_contains" in rule and rule["field_name_not_contains"].lower() in fn:
            continue
        if "operation" in rule and rule["operation"] != op:
            continue
        if "new_value_equals" in rule and rule["new_value_equals"].upper() != nv:
            continue
        return rule["category"], rule["subcategory"]
    return None


def match_summary(row):
    summary = row.get("raw_summary") or ""
    for rule in CATEGORY_RULES["summary_text_rules"]:
        if re.search(rule["pattern"], summary):
            return rule["category"], rule["subcategory"]
    return None


def categorize_changes(rows, allow_unknown=False, extra_rules=None):
    """Returns dict: status 'ok' with categorized 'rows', or 'needs_category_review'
    with 'unknown_combinations' to resolve before continuing (unless allow_unknown)."""
    unknown_combos = {}
    counter = Counter()
    for row in rows:
        if row.get("field_name"):
            hit = match_structured(row, extra_rules)
            if hit:
                row["category"], row["subcategory"] = hit
                row["category_confidence"] = "rule_matched"
            else:
                key = (row.get("resource_type"), row.get("field_name"), row.get("operation"))
                if key not in unknown_combos:
                    unknown_combos[key] = {"resource_type": row.get("resource_type"), "field_name": row.get("field_name"), "operation": row.get("operation"), "example_old_value": row.get("old_value"), "example_new_value": row.get("new_value"), "count": 0}
                unknown_combos[key]["count"] += 1
                row["category"], row["subcategory"] = CATEGORY_RULES["fallback"]["category"], CATEGORY_RULES["fallback"]["subcategory"]
                row["category_confidence"] = "fallback_other"
        else:
            hit = match_summary(row)
            if hit:
                row["category"], row["subcategory"] = hit
                row["category_confidence"] = "rule_matched"
            else:
                row["category"], row["subcategory"] = CATEGORY_RULES["fallback"]["category"], CATEGORY_RULES["fallback"]["subcategory"]
                row["category_confidence"] = "fallback_other"
        counter[row["category"]] += 1

    if unknown_combos and not allow_unknown:
        return {
            "status": "needs_category_review",
            "unknown_combinations": sorted(unknown_combos.values(), key=lambda x: -x["count"]),
            "instructions": "Show each (resource_type, field_name, operation) + example old/new to the user. Ask which category/subcategory it belongs to. Add a matching dict to CATEGORY_RULES['structured_rules'] (or pass via extra_rules), then re-run categorize_changes.",
        }

    total = len(rows)
    other_pct = round(100 * counter.get("Other", 0) / total, 1) if total else 0.0
    return {"status": "ok", "rows": rows, "other_pct": other_pct, "category_breakdown": dict(counter)}


# =====================================================================
# STAGE 5-6: AGGREGATION + DASHBOARD
# =====================================================================
def mask_label(index):
    letters = string.ascii_uppercase
    label, n = "", index
    while True:
        label = letters[n % 26] + label
        n = n // 26 - 1
        if n < 0:
            break
    return f"User {label}"


def mask_rows(rows, masked_users):
    """Fixed 2026-08-18 (audit F-04): --mask-users previously only masked
    dashboard.html/change_history.json — that masking happened inside
    build_aggregation(), which changes.jsonl never passed through
    (changes.jsonl was written earlier in run_pipeline, straight from the
    unmasked categorized rows). A run with --mask-users therefore still wrote
    the raw human user_email/user_name into changes.jsonl. Masking now
    happens once, upstream of every output file, so none of them can leak an
    identity the caller explicitly asked to have masked. Account/campaign
    names are never masked — that's the real user's own data.

    Fixed 2026-08-18 (3rd audit round, confirmed): label assignment used to be
    pure per-run encounter order — the same real person could come out as
    "User A" in one run and "User B" in a later run on different/reordered
    data, defeating the point of masking for external sharing (comparing two
    masked reports over time needs a stable identity->label mapping). Labels
    are now persisted in mask-labels.json (same state directory as the header
    mapping profiles) keyed by the real identity, reused on every later run,
    and only ever extended for genuinely new identities — never reassigned or
    reordered for ones already seen."""
    if not masked_users:
        return rows
    # Path is resolved from the current PROFILES_DIR at call time, not a
    # module-level constant — self_test() temporarily repoints PROFILES_DIR
    # at an isolated temp directory (established practice, same reason
    # try_fingerprint_match/save_profile already read PROFILES_DIR live
    # instead of caching it), and a precomputed path would silently miss that
    # and leak test data into the real state directory.
    mask_labels_path = PROFILES_DIR / "mask-labels.json"
    store = {}
    if mask_labels_path.exists():
        with open(mask_labels_path, encoding="utf-8") as f:
            store = json.load(f)
    identity_order, seen = [], set()
    for r in rows:
        if r["actor_type"] == "human":
            ident = r.get("user_email") or r.get("user_name")
            if ident and ident not in seen:
                seen.add(ident)
                identity_order.append(ident)
    next_index = len(store)
    mask_map = {}
    for ident in identity_order:
        if ident not in store:
            store[ident] = mask_label(next_index)
            next_index += 1
        mask_map[ident] = store[ident]
    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    with open(mask_labels_path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)
    out = []
    for r in rows:
        r = dict(r)
        if r["actor_type"] == "human":
            ident = r.get("user_email") or r.get("user_name")
            r["user_name"] = mask_map.get(ident, r.get("user_name"))
            r["user_email"] = None
        out.append(r)
    return out


def account_identity(r):
    return r.get("account_id") or r.get("account_name")


def campaign_identity(r):
    # Fixed 2026-08-19 (2nd audit): previously always r["campaign_name"] —
    # correct for cross-account collisions (fixed 2026-08-18) but still
    # collided on two DIFFERENT campaign_ids sharing a name within the SAME
    # account, and on a renamed campaign (same id, name changed mid-window)
    # being treated as two campaigns. campaign_id is Google's own stated
    # unique identifier; used here when available, name only as a fallback
    # for sources that never provide an id (typical UI CSV exports).
    return r.get("campaign_id") or r.get("campaign_name")


def ad_group_identity(r):
    return r.get("ad_group_id") or r.get("ad_group_name")


def user_identity(r):
    # email over display name: two different people can share a display
    # name ("Alex"); email is the more likely-unique identifier when both
    # are available. Fixed 2026-08-19 (2nd audit, P2 user-identity finding).
    return r.get("user_email") or r.get("user_name")


def chrono_key(r):
    # Fixed 2026-08-19 (2nd audit): window_start/end and campaign_last
    # previously always compared timestamp_iso (naive, local-to-whatever-
    # timezone-the-row-happened-to-resolve-to) — two rows from genuinely
    # different, both-resolved timezones could sort in the wrong order.
    # timestamp_utc is preferred when resolved; rows without a resolved
    # timezone still fall back to local-naive comparison — a real,
    # documented limitation (see VALUE PARSING RULES above), not invented
    # away, since fabricating a timezone would be worse than an honest gap.
    return r.get("timestamp_utc") or r.get("timestamp_iso") or ""


def build_aggregation(rows, sources, other_pct, masked_users, generated_at, date_parse_stats=None):
    total = len(rows)
    event_ids = {r["event_id"] for r in rows}

    account_counter, account_display = Counter(), {}
    for r in rows:
        if r.get("account_name") or r.get("account_id"):
            key = account_identity(r)
            account_counter[key] += 1
            account_display[key] = r.get("account_name") or f"Account {key}"

    campaign_counter, campaign_display = Counter(), {}
    for r in rows:
        if r.get("campaign_name") or r.get("campaign_id"):
            key = (account_identity(r), campaign_identity(r))
            campaign_counter[key] += 1
            name = r.get("campaign_name") or f"Campaign {r.get('campaign_id')}"
            campaign_display[key] = name if len(account_counter) <= 1 else f"{name} ({account_display.get(account_identity(r), r.get('account_name') or '')})"

    ad_group_counter = Counter()
    for r in rows:
        if r.get("ad_group_name") or r.get("ad_group_id"):
            ad_group_counter[(account_identity(r), campaign_identity(r), ad_group_identity(r))] += 1

    chrono_vals = [chrono_key(r) for r in rows if chrono_key(r)]
    window_start = min(chrono_vals) if chrono_vals else None
    window_end = max(chrono_vals) if chrono_vals else None

    human_counter, human_display, automation_counter = Counter(), {}, Counter()
    for r in rows:
        if r["actor_type"] == "human":
            key = user_identity(r) or "Unknown"
            human_counter[key] += 1
            human_display[key] = r.get("user_name") or r.get("user_email") or "Unknown"
        elif r["actor_type"] == "automation":
            automation_counter[r.get("client_type") or r.get("user_name") or "Automation"] += 1
    most_active_user_key = human_counter.most_common(1)[0][0] if human_counter else None
    most_active_user = human_display.get(most_active_user_key)

    most_active_campaign_key = campaign_counter.most_common(1)[0][0] if campaign_counter else None
    most_active_campaign = campaign_display.get(most_active_campaign_key)

    timeline_counter, excluded_from_timeline = Counter(), 0
    for r in rows:
        tsu = r.get("timestamp_utc")
        if tsu:
            timeline_counter[tsu[:10]] += 1
        else:
            excluded_from_timeline += 1
    timeline_days = sorted(timeline_counter.keys())
    timeline = [{"date": d, "count": timeline_counter[d], "is_window_edge": bool(timeline_days) and (d == timeline_days[0] or d == timeline_days[-1])} for d in timeline_days]

    # accounts_map: account_name is already the top-level grouping key here
    # (a same-named campaign in two different accounts was never actually at
    # risk in THIS view — it lands under two different account entries by
    # construction). The gap this view did have: two different campaign_ids
    # sharing a name WITHIN one account collapsing into one nested bar — keyed
    # by campaign_identity now, same fix class as campaign_counter above.
    accounts_map = defaultdict(lambda: defaultdict(int))
    accounts_map_display = defaultdict(dict)
    for r in rows:
        if r.get("account_name"):
            camp_key = campaign_identity(r) or "(no campaign)"
            accounts_map[r["account_name"]][camp_key] += 1
            accounts_map_display[r["account_name"]][camp_key] = r.get("campaign_name") or (f"Campaign {r.get('campaign_id')}" if r.get("campaign_id") else "(no campaign)")
    accounts = [
        {
            "account_name": acc, "count": sum(cc.values()),
            "campaigns": [{"campaign_name": accounts_map_display[acc][c], "count": n} for c, n in sorted(cc.items(), key=lambda kv: -kv[1])],
        }
        for acc, cc in sorted(accounts_map.items(), key=lambda kv: -sum(kv[1].values()))
    ]

    category_counter = Counter(r["category"] for r in rows)
    categories = [{"category": c, "count": n} for c, n in category_counter.most_common()]

    # Fixed 2026-08-18: keyed by campaign_name alone before, so "Campaign
    # Alpha" in two different accounts silently collapsed into one entry and
    # one of them was lost. Key by (account, campaign) instead.
    # Fixed 2026-08-19 (2nd audit): campaign component upgraded to
    # campaign_identity (id-preferred), and the chronological comparison
    # upgraded to chrono_key (UTC-preferred) — see those functions' notes.
    campaign_last = {}
    for r in rows:
        c = r.get("campaign_name") or r.get("campaign_id")
        ts = chrono_key(r)
        if not c or not ts:
            continue
        key = (account_identity(r), campaign_identity(r))
        if key not in campaign_last or ts > campaign_last[key]["_sort_ts"]:
            campaign_last[key] = {
                "campaign_name": r.get("campaign_name") or f"Campaign {r.get('campaign_id')}",
                "account_name": r.get("account_name"),
                "last_change_ts": r.get("timestamp_iso"),
                "_sort_ts": ts,
            }
    window_end_date = datetime.fromisoformat(window_end[:19]).date() if window_end else date.today()
    untouched = []
    for entry in campaign_last.values():
        last_date = datetime.fromisoformat(entry["last_change_ts"]).date()
        # Fixed 2026-08-19 (2nd audit): renamed from days_since_last_change.
        # This is a STATIC snapshot value (relative to this dataset's own
        # window_end, computed once at build time) — it is NOT the same
        # thing as the dashboard's live days-since-today figure (computed in
        # the browser from the real clock at render time, per the 1st
        # audit's fix). The two previously shared a field name that implied
        # they meant the same thing; they don't, and can't (a static JSON
        # file has no "today"). The name now says what it actually is.
        entry["days_since_last_change_at_generation"] = (window_end_date - last_date).days
        del entry["_sort_ts"]
        untouched.append(entry)
    untouched.sort(key=lambda e: -e["days_since_last_change_at_generation"])

    # Fixed 2026-08-18 (audit F-04): masking now happens once in run_pipeline,
    # upstream of this function and of every output file (see mask_rows()) —
    # `rows` arriving here are already masked when masked_users is True, so
    # human_counter/most_active_user above were already computed from masked
    # names. No second masking pass is needed or performed here.
    changes_out = rows
    users_human = [{"name": human_display[key], "count": n} for key, n in human_counter.most_common()]

    unparseable = (date_parse_stats or {}).get("rows_skipped_unparseable_date", 0)
    input_rows = total + unparseable
    unparseable_pct = round(100 * unparseable / input_rows, 1) if input_rows else 0.0

    return {
        "meta": {
            "generated_at": generated_at, "data_window_start": window_start, "data_window_end": window_end,
            "row_count": total, "event_count": len(event_ids), "sources": sorted(sources), "other_pct": other_pct,
            "masked_users": masked_users, "timeline_reference_tz": "UTC",
            "rows_excluded_from_timeline_unknown_tz": excluded_from_timeline,
            "rows_skipped_unparseable_date": unparseable, "rows_skipped_unparseable_date_pct": unparseable_pct,
            "untouched_scope_note": "Based only on campaigns that appear in this change log. A campaign never in the log may not exist, not just be untouched.",
        },
        "summary": {
            "total_changes": total, "active_accounts": len(account_counter), "active_users": len(human_counter),
            "changed_campaigns": len(campaign_counter), "changed_ad_groups": len(ad_group_counter),
            "last_change_ts": window_end,
            "most_active_user": most_active_user,
            "most_active_campaign": most_active_campaign,
        },
        "timeline": timeline,
        "users": {"human": users_human, "automation": [{"name": n, "count": c} for n, c in automation_counter.most_common()]},
        "accounts": accounts, "categories": categories, "untouched": untouched, "changes": changes_out,
    }


DASHBOARD_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Ads Change History</title>
<style>
  :root {
    --bg: #0b0e14; --panel: #12151d; --panel-2: #191d28; --border: rgba(120,140,200,.16);
    --text: #e8ecf5; --text-dim: #8b93a8; --text-muted: #5c6478; --accent: #5b8cff; --accent-2: #8f6bff;
    --good: #35c98f; --warn: #f2b84b; --bad: #ef6a6a;
  }
  * { box-sizing: border-box; }
  body { margin:0; color:var(--text); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background: radial-gradient(1100px 560px at 85% -10%, rgba(167,139,250,.14), transparent 60%),
                radial-gradient(900px 480px at -5% 0%, rgba(45,212,191,.10), transparent 55%),
                radial-gradient(800px 500px at 50% 110%, rgba(244,114,182,.06), transparent 60%), var(--bg); }
  header { padding:26px 28px 10px; }
  header h1 { margin:0 0 4px; font-size:21px; font-weight:700; letter-spacing:.01em; }
  header .meta { color:var(--text-dim); font-size:12px; letter-spacing:.02em; }
  main { padding:4px 28px 44px; display:flex; flex-direction:column; gap:20px; max-width:1440px; margin:0 auto; }
  section {
    background:linear-gradient(180deg, var(--panel), var(--panel) 60%, var(--panel-2));
    border:1px solid var(--border); border-radius:14px; padding:20px 22px;
    box-shadow: 0 8px 28px rgba(0,0,0,.28), inset 0 1px 0 rgba(255,255,255,.03);
  }
  /* "section-label" treatment — uppercase, tracked out, trailing rule to the
     card edge instead of a bare heading. Borrowed idea (2026-08-18) from a
     public dashboard-generation skill's visual language; adapted to our own
     accent color, no new class needed since every section already uses h2. */
  section h2 {
    margin:0 0 16px; font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.12em;
    color:var(--text-muted); display:flex; align-items:center; gap:10px;
  }
  section h2::after { content:''; flex:1; height:1px; background:var(--border); }
  section h2 .note { margin:0; text-transform:none; letter-spacing:normal; font-weight:400; }
  .section-caption { color:var(--text-dim); font-size:12px; margin:-8px 0 14px; }
  .filters { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
  .filters select, .filters input[type=text] { background:var(--panel-2); color:var(--text); border:1px solid var(--border); border-radius:7px; padding:7px 9px; font-size:13px; }
  .filters select { min-width:140px; }
  .rangebtns { display:flex; gap:4px; }
  .rangebtns button, .threshbtns button { background:var(--panel-2); color:var(--text-dim); border:1px solid var(--border); border-radius:7px; padding:6px 10px; font-size:12px; cursor:pointer; }
  .rangebtns button.active, .threshbtns button.active { background:var(--accent); color:#fff; border-color:var(--accent); box-shadow:0 0 0 3px rgba(91,140,255,.18); }
  .clearbtn { background:transparent; color:var(--bad); border:1px solid var(--bad); border-radius:7px; padding:6px 10px; font-size:12px; cursor:pointer; margin-left:auto; }
  #filterContext { color:var(--accent); font-size:12px; margin-top:10px; min-height:1em; }
  .summary-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:14px; }
  .stat {
    background:var(--panel-2); border:1px solid var(--border); border-radius:11px; padding:15px 15px 13px;
    position:relative; overflow:hidden; box-shadow:0 0 18px rgba(91,140,255,.06), inset 0 1px 0 rgba(255,255,255,.03);
  }
  .stat .n { font-size:23px; font-weight:700; letter-spacing:-.01em; line-height:1.15; }
  .stat .l { color:var(--text-muted); font-size:10.5px; font-weight:600; text-transform:uppercase; letter-spacing:.07em; margin-top:7px; }
  .bar-row { display:grid; grid-template-columns:180px 1fr 60px; gap:10px; align-items:center; padding:5px 0; cursor:pointer; }
  .bar-row:hover .bar-label { color:var(--accent); }
  .bar-label { font-size:12.5px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .bar-track { background:var(--panel); border:1px solid var(--border); border-radius:4px; height:16px; overflow:hidden; }
  .bar-fill { background:linear-gradient(90deg,var(--accent),var(--accent-2)); height:100%; border-radius:4px; }
  .bar-n { text-align:right; font-size:12px; color:var(--text-dim); }
  .timeline { display:flex; align-items:flex-end; gap:2px; height:120px; overflow-x:auto; padding-bottom:4px; }
  .tl-bar { min-width:8px; flex:1 0 8px; background:linear-gradient(180deg,var(--accent-2),var(--accent)); border-radius:3px 3px 0 0; position:relative; opacity:.9; }
  .tl-bar.edge { opacity:.45; }
  .tl-bar:hover { opacity:1; }
  .tl-bar .tip { display:none; position:absolute; bottom:100%; left:50%; transform:translateX(-50%); background:#000; color:#fff; padding:3px 6px; border-radius:4px; font-size:10px; white-space:nowrap; margin-bottom:4px; }
  .tl-bar:hover .tip { display:block; }
  .acc-block { border-bottom:1px solid var(--border); padding:7px 0; }
  .acc-block:last-child { border-bottom:none; }
  .acc-head { display:flex; align-items:center; gap:8px; cursor:pointer; font-weight:600; }
  .acc-head .chev { transition:transform .15s; color:var(--text-dim); }
  .acc-head.open .chev { transform:rotate(90deg); }
  .camp-list { display:none; padding-left:20px; margin-top:6px; }
  .camp-list.open { display:block; }
  table { width:100%; border-collapse:collapse; font-size:12.5px; }
  th, td { text-align:left; padding:8px 9px; border-bottom:1px solid var(--border); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:220px; }
  th { color:var(--text-muted); font-weight:700; font-size:11px; text-transform:uppercase; letter-spacing:.05em; cursor:pointer; user-select:none; position:sticky; top:0; background:var(--panel); }
  th.sorted::after { content:" ▾"; color:var(--accent); }
  tr.datarow { cursor:pointer; }
  tr.datarow:hover { background:var(--panel-2); }
  .pill { display:inline-block; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:600; background:var(--panel); border:1px solid var(--border); color:var(--text-dim); }
  .pager { display:flex; gap:8px; align-items:center; margin-top:10px; font-size:12px; color:var(--text-dim); }
  .pager button { background:var(--panel-2); color:var(--text); border:1px solid var(--border); border-radius:7px; padding:4px 10px; cursor:pointer; }
  .pager button:disabled { opacity:.4; cursor:default; }
  .explorer-search { width:100%; margin-bottom:10px; padding:9px 11px; }
  .detail-overlay { display:none; position:fixed; inset:0; background:rgba(3,5,10,.65); align-items:center; justify-content:center; z-index:50; backdrop-filter:blur(2px); }
  .detail-overlay.open { display:flex; }
  .detail-card { background:var(--panel); border:1px solid var(--border); border-radius:14px; padding:24px; width:min(520px,90vw); max-height:85vh; overflow:auto; box-shadow:0 20px 60px rgba(0,0,0,.45); }
  .detail-card h3 { margin-top:0; }
  .detail-card dl { display:grid; grid-template-columns:120px 1fr; row-gap:8px; column-gap:10px; margin:0; }
  .detail-card dt { color:var(--text-dim); }
  .detail-card .close { float:right; cursor:pointer; color:var(--text-dim); }
  .note { color:var(--text-dim); font-size:11.5px; margin-top:8px; }
  .warn-box { background:rgba(242,184,75,.1); border:1px solid var(--warn); color:var(--warn); border-radius:9px; padding:9px 13px; font-size:12.5px; margin:0 0 4px; }
  .empty { color:var(--text-muted); padding:18px 0; text-align:center; }
  @media (max-width: 640px) {
    header { padding:18px 16px 8px; }
    main { padding:4px 16px 32px; gap:16px; }
    section { padding:15px 14px; border-radius:11px; }
    .summary-grid { grid-template-columns:repeat(2,1fr); gap:10px; }
    th, td { max-width:140px; }
  }
</style>
</head>
<body>
<header>
  <h1>Ads Change History</h1>
  <div class="meta" id="metaLine"></div>
  <div id="apiCaveat"></div>
  <!-- Permanent, run-independent disclosure — unlike apiCaveat above (which
       only fires for conditions specific to THIS run), this always shows.
       Borrowed idea (2026-08-18): a public Google Ads audit skill always
       ships a "what this audit can't see" footer regardless of that run's
       findings. Adapted here to stay strictly factual, no dollar/quality
       claims — this tool doesn't judge, so the disclosure doesn't either. -->
  <details class="note" style="margin-top:6px;">
    <summary style="cursor:pointer;">What this report can't do</summary>
    <ul style="margin:6px 0 0 18px;padding:0;">
      <li>Only knows what's in the input file — doesn't call the Google Ads API live, doesn't know about changes made after the export or outside its date range.</li>
      <li>Reports what changed and when — never whether a change was good, risky, or should have happened differently. That judgment isn't in scope for this tool.</li>
      <li>If the input includes Google Ads ChangeEvent API data: that API keeps only 30 days of history, caps at 10,000 rows per query, can lag up to ~3 minutes behind a very recent change, and may not include every entry the Google Ads UI's own Change History page shows.</li>
    </ul>
  </details>
</header>
<main>
  <section id="filtersSection">
    <h2>Filters</h2>
    <div class="filters">
      <select id="fAccount" multiple size="1"></select>
      <select id="fCampaign" multiple size="1"></select>
      <select id="fUser" multiple size="1"></select>
      <select id="fActor" size="1">
        <option value="">All actors</option>
        <option value="human">Human</option>
        <option value="automation">Automation</option>
        <option value="unknown">Unknown</option>
      </select>
      <select id="fCategory" multiple size="1"></select>
      <select id="fOperation" multiple size="1"></select>
      <select id="fSource" multiple size="1"></select>
      <div class="rangebtns" id="rangeBtns">
        <button data-range="all" class="active">All</button>
        <button data-range="30">30d</button>
        <button data-range="7">7d</button>
        <button data-range="1">Last 24h</button>
      </div>
      <input type="text" id="fSearch" placeholder="Search changes…" style="min-width:200px;">
      <button class="clearbtn" id="clearFilters">Clear filters</button>
    </div>
    <!-- Borrowed idea (2026-08-18): a public dashboard-generation skill's
         convention of a stated filter-context line. Only meaningful for us
         because our filters are LIVE (unlike that skill's frozen-snapshot
         default, kept deliberately here — see AUDIT round-3 discussion) —
         this line exists so a screenshot mid-filter is self-explanatory. -->
    <div id="filterContext"></div>
  </section>
  <section id="summarySection">
    <h2>Summary</h2>
    <div class="section-caption">Totals reflect the filters and date range currently applied above.</div>
    <div class="summary-grid" id="summaryGrid"></div>
  </section>
  <section>
    <h2>Activity Timeline <span class="note">(UTC day buckets)</span></h2>
    <div class="section-caption">Bar height is relative to the busiest day shown — hover a bar for its exact date and count.</div>
    <div class="timeline" id="timeline"></div>
  </section>
  <section>
    <h2>User Activity</h2>
    <div class="section-caption">Split by actor type — human (named user), automation (script/rule/recommendation), or unknown (no identity in the source data).</div>
    <div id="userActivity"></div>
  </section>
  <section>
    <h2>Account / Campaign Activity</h2>
    <div class="section-caption">Each account expands to the campaigns it contains, ranked by number of changes.</div>
    <div id="accountActivity"></div>
  </section>
  <section>
    <h2>Category Distribution</h2>
    <div class="section-caption">Categories come from rule matches against resource type, field, and operation. "Other" means no rule matched yet — see coverage.txt.</div>
    <div id="categoryDist"></div>
  </section>
  <section>
    <h2>Campaign Last Changes</h2>
    <div class="section-caption">"Days since" is computed against the current time in your browser, not the moment this file was generated — it grows the longer this file stays open.</div>
    <div class="threshbtns" id="threshBtns" style="margin-bottom:10px;">
      <button data-min="0" class="active">All</button>
      <button data-min="7">7+ days</button>
      <button data-min="14">14+ days</button>
      <button data-min="30">30+ days</button>
      <button data-min="60">60+ days</button>
    </div>
    <div id="untouched"></div>
  </section>
  <section>
    <h2>Change Explorer</h2>
    <div class="section-caption">Click any row for full before/after detail. Click a column header to sort by it.</div>
    <input type="text" class="explorer-search" id="explorerSearch" placeholder="Search within Explorer…">
    <div style="overflow-x:auto;">
    <table id="explorerTable">
      <thead><tr>
        <th data-key="timestamp_iso">Date</th><th data-key="user_name">User</th><th data-key="account_name">Account</th>
        <th data-key="campaign_name">Campaign</th><th data-key="ad_group_name">Ad Group</th><th data-key="category">Category</th>
        <th data-key="field_name">Change</th><th data-key="old_value">Old Value</th><th data-key="new_value">New Value</th><th data-key="source">Source</th>
      </tr></thead>
      <tbody id="explorerBody"></tbody>
    </table>
    </div>
    <div class="pager">
      <button id="pagerPrev">Prev</button><span id="pagerInfo"></span><button id="pagerNext">Next</button>
    </div>
  </section>
</main>
<div class="detail-overlay" id="detailOverlay">
  <div class="detail-card">
    <span class="close" id="detailClose">✕</span>
    <h3>Change Detail</h3>
    <dl id="detailBody"></dl>
  </div>
</div>
<script>
const DASH_DATA = /*__DASH_DATA__*/null;
// Fixed 2026-08-18: campaign/account/user/field names come from the source
// file, not from a closed set — nothing here was escaped before, so a
// campaign named e.g. "<img src=x onerror=...>" would have executed. Every
// place below that interpolates source-derived text into innerHTML now goes
// through this.
function escapeHtml(s){
  if (s === null || s === undefined) return '';
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
// Fixed 2026-08-19 (2nd audit): mirrors the Python-side identity() helpers —
// prefer the *_id field when present, fall back to name only when a source
// never provides one. Two different campaign_ids sharing a display name (or
// a campaign renamed mid-window) previously collapsed into one bar/entry
// here even after the Python side was fixed, because this file used
// c.campaign_name directly everywhere.
function accountIdentity(c){ return c.account_id || c.account_name; }
function campaignIdentity(c){ return c.campaign_id || c.campaign_name; }
function adGroupIdentity(c){ return c.ad_group_id || c.ad_group_name; }
function userIdentity(c){ return c.user_email || c.user_name; }
// Distinct accent per category — purely a visual identity aid (which pill is
// which at a glance), not a good/bad judgment; every category gets a color,
// none is "better." "Other" stays neutral on purpose — it's not a real
// category, coloring it like one would misrepresent it as such.
const CATEGORY_COLORS = {
  Budget:'#5b8cff', Bidding:'#b48aff', Keyword:'#3ecfb2', Ad:'#f5834a', Asset:'#f0c040',
  Targeting:'#ff6b9d', Audience:'#5bc8ff', Status:'#35c98f', Campaign:'#8f6bff',
  AdGroup:'#6bafff', Conversion:'#e8a33d', Feed:'#7fd992',
};
function categoryPillStyle(cat){
  const c = CATEGORY_COLORS[cat];
  return c ? ` style="color:${c};border-color:${c}55;background:${c}18;"` : '';
}
// General-purpose accent cycle for things with no fixed category identity —
// KPI cards, per-account bars. Same visual-only rationale as CATEGORY_COLORS.
const HUES = ['#5b8cff','#3ecfb2','#b48aff','#ff6b9d','#f0c040','#5bc8ff','#35c98f','#f5834a'];
function barFillStyle(color, pct){
  return `width:${Math.max(2,pct)}%;background:linear-gradient(90deg,${color}cc,${color});box-shadow:0 0 8px ${color}40;`;
}
const state = { accounts:new Set(), campaigns:new Set(), users:new Set(), actor:'', categories:new Set(), operations:new Set(), sources:new Set(), range:'all', search:'', page:1, pageSize:200, sortKey:'timestamp_iso', sortDir:-1 };
function uniqSorted(arr){ return [...new Set(arr)].filter(Boolean).sort(); }
function fillMultiSelect(el, values, allLabel){
  el.innerHTML = '';
  const opt0 = document.createElement('option'); opt0.value=''; opt0.textContent = allLabel; el.appendChild(opt0);
  values.forEach(v=>{ const o=document.createElement('option'); o.value=v; o.textContent=v; el.appendChild(o); });
}
// Fixed 2026-08-19 (2nd audit): value = identity (id-preferred), label =
// display name — so two different ids sharing a display name filter as the
// two distinct things they are, not one merged option.
function fillMultiSelectPairs(el, pairs, allLabel){
  el.innerHTML = '';
  const opt0 = document.createElement('option'); opt0.value=''; opt0.textContent = allLabel; el.appendChild(opt0);
  const seen = new Set();
  pairs.filter(Boolean).sort((a,b)=> (a[1]||'').localeCompare(b[1]||'')).forEach(([v,l])=>{
    if(seen.has(v)) return; seen.add(v);
    const o=document.createElement('option'); o.value=v; o.textContent=l; el.appendChild(o);
  });
}
function initFilters(){
  fillMultiSelectPairs(document.getElementById('fAccount'), DASH_DATA.changes.map(c=> accountIdentity(c) ? [accountIdentity(c), c.account_name || `Account ${c.account_id}`] : null), 'All accounts');
  fillMultiSelectPairs(document.getElementById('fCampaign'), DASH_DATA.changes.map(c=> campaignIdentity(c) ? [campaignIdentity(c), c.campaign_name || `Campaign ${c.campaign_id}`] : null), 'All campaigns');
  fillMultiSelectPairs(document.getElementById('fUser'), DASH_DATA.changes.map(c=> c.actor_type==='human' && userIdentity(c) ? [userIdentity(c), c.user_name || c.user_email] : null), 'All users');
  fillMultiSelect(document.getElementById('fCategory'), uniqSorted(DASH_DATA.changes.map(c=>c.category)), 'All categories');
  fillMultiSelect(document.getElementById('fOperation'), uniqSorted(DASH_DATA.changes.map(c=>c.operation)), 'All types');
  fillMultiSelect(document.getElementById('fSource'), uniqSorted(DASH_DATA.changes.map(c=>c.source)), 'All sources');
  ['fAccount','fCampaign','fUser','fCategory','fOperation','fSource'].forEach(id=>{
    document.getElementById(id).addEventListener('change', e=>{
      const sel = new Set([...e.target.selectedOptions].map(o=>o.value).filter(Boolean));
      const map = {fAccount:'accounts', fCampaign:'campaigns', fUser:'users', fCategory:'categories', fOperation:'operations', fSource:'sources'};
      state[map[id]] = sel; state.page=1; render();
    });
  });
  document.getElementById('fActor').addEventListener('change', e=>{ state.actor = e.target.value; state.page=1; render(); });
  document.getElementById('rangeBtns').addEventListener('click', e=>{
    if(e.target.tagName!=='BUTTON') return;
    [...e.target.parentElement.children].forEach(b=>b.classList.remove('active'));
    e.target.classList.add('active'); state.range = e.target.dataset.range; state.page=1; render();
  });
  document.getElementById('fSearch').addEventListener('input', e=>{ state.search=e.target.value.toLowerCase(); state.page=1; render(); });
  document.getElementById('explorerSearch').addEventListener('input', e=>{ state.search=e.target.value.toLowerCase(); document.getElementById('fSearch').value=e.target.value; state.page=1; render(); });
  document.getElementById('clearFilters').addEventListener('click', ()=>{
    state.accounts=new Set(); state.campaigns=new Set(); state.users=new Set(); state.actor=''; state.categories=new Set(); state.operations=new Set(); state.sources=new Set();
    state.range='all'; state.search=''; state.page=1;
    document.getElementById('fSearch').value=''; document.getElementById('explorerSearch').value=''; document.getElementById('fActor').value='';
    document.querySelectorAll('#fAccount,#fCampaign,#fUser,#fCategory,#fOperation,#fSource').forEach(s=>[...s.options].forEach(o=>o.selected=false));
    [...document.getElementById('rangeBtns').children].forEach(b=>b.classList.remove('active'));
    document.getElementById('rangeBtns').children[0].classList.add('active');
    render();
  });
}
function withinRange(iso){
  if(state.range==='all' || !iso) return true;
  const ref = DASH_DATA.meta.data_window_end ? new Date(DASH_DATA.meta.data_window_end) : new Date();
  const days = parseFloat(state.range);
  const cutoff = new Date(ref.getTime() - days*86400000);
  return new Date(iso) >= cutoff;
}
function getFiltered(){
  // Fixed 2026-08-19 (2nd audit): filters now match on identity (id-
  // preferred), matching what the dropdowns now populate as `value` —
  // previously matched on raw display name, so two different ids sharing a
  // name both matched any filter selection meant for just one of them.
  return DASH_DATA.changes.filter(c=>{
    if(state.accounts.size && !state.accounts.has(accountIdentity(c))) return false;
    if(state.campaigns.size && !state.campaigns.has(campaignIdentity(c))) return false;
    if(state.users.size){ const ident = c.actor_type==='human' ? userIdentity(c) : null; if(!ident || !state.users.has(ident)) return false; }
    if(state.actor && c.actor_type !== state.actor) return false;
    if(state.categories.size && !state.categories.has(c.category)) return false;
    if(state.operations.size && !state.operations.has(c.operation)) return false;
    if(state.sources.size && !state.sources.has(c.source)) return false;
    if(!withinRange(c.timestamp_iso)) return false;
    if(state.search){
      const hay = [c.campaign_name,c.account_name,c.user_name,c.user_email,c.category,c.subcategory,
        c.field_name,c.old_value,c.new_value,c.ad_group_name,c.operation,c.source,c.resource_type,
        c.client_type,c.change_id].join(' ').toLowerCase();
      if(!hay.includes(state.search)) return false;
    }
    return true;
  });
}
function renderSummary(filtered){
  // Fixed 2026-08-19 (2nd audit): every identity below upgraded from raw
  // display name to *Identity() (id-preferred) — same class of fix as
  // 2026-08-18's account-scoping, this time covering same-account
  // duplicate/renamed campaign names, ad group names, and same-display-name
  // different-email users, none of which the first fix caught.
  const humanCount = new Set(filtered.filter(c=>c.actor_type==='human').map(userIdentity)).size;
  const accountSet = new Set(filtered.map(accountIdentity).filter(Boolean));
  const accounts = accountSet.size;
  const campaigns = new Set(filtered.filter(c=>campaignIdentity(c)).map(c=>accountIdentity(c)+'||'+campaignIdentity(c))).size;
  const adgroups = new Set(filtered.filter(c=>adGroupIdentity(c)).map(c=>accountIdentity(c)+'||'+campaignIdentity(c)+'||'+adGroupIdentity(c))).size;
  const last = filtered.reduce((m,c)=> (c.timestamp_iso && (!m || c.timestamp_iso>m)) ? c.timestamp_iso : m, null);
  const userCounts = {}, userDisplay = {};
  filtered.forEach(c=>{ if(c.actor_type==='human'){ const k=userIdentity(c); userCounts[k]=(userCounts[k]||0)+1; userDisplay[k]=c.user_name||c.user_email; }});
  const topUserKey = Object.entries(userCounts).sort((a,b)=>b[1]-a[1])[0];
  const topUser = topUserKey ? [userDisplay[topUserKey[0]]] : null;
  const campCounts = {}, campDisplay = {};
  filtered.forEach(c=>{
    if(!campaignIdentity(c)) return;
    const key = accountIdentity(c)+'||'+campaignIdentity(c);
    campCounts[key] = (campCounts[key]||0)+1;
    const name = c.campaign_name || `Campaign ${c.campaign_id}`;
    campDisplay[key] = accountSet.size <= 1 ? name : `${name} (${c.account_name||''})`;
  });
  const topCampKey = Object.entries(campCounts).sort((a,b)=>b[1]-a[1])[0];
  const topCamp = topCampKey ? campDisplay[topCampKey[0]] : null;
  const stats = [
    ['Total Changes', filtered.length.toLocaleString()], ['Active Accounts', accounts], ['Active Users', humanCount],
    ['Changed Campaigns', campaigns], ['Changed Ad Groups', adgroups],
    ['Last Change', last ? new Date(last).toLocaleString() : '—'],
    ['Most Active User', topUser ? topUser[0] : '—'], ['Most Active Campaign', topCamp || '—'],
  ];
  // Each card cycles through HUES — a colorful strip reads faster than 8
  // identical blue cards, and no card's color means anything (no card is
  // "the good one") — same rationale as CATEGORY_COLORS above.
  document.getElementById('summaryGrid').innerHTML = stats.map(([l,n],i)=>{
    const hue = HUES[i % HUES.length];
    return `<div class="stat" style="box-shadow:0 0 18px ${hue}18, inset 0 1px 0 rgba(255,255,255,.03);"><div style="position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,${hue},${hue}66);"></div><div class="n" style="color:${hue};">${escapeHtml(n)}</div><div class="l">${l}</div></div>`;
  }).join('');
}
function renderTimeline(filtered){
  const counts = {}; let excluded = 0;
  filtered.forEach(c=>{ if(c.timestamp_utc){ const d=c.timestamp_utc.slice(0,10); counts[d]=(counts[d]||0)+1; } else excluded++; });
  const days = Object.keys(counts).sort();
  const max = Math.max(1,...days.map(d=>counts[d]));
  const el = document.getElementById('timeline');
  if(!days.length){ el.innerHTML = '<div class="empty">No timezone-resolved rows to plot.</div>'; return; }
  el.innerHTML = days.map((d,i)=>{
    const edge = i===0 || i===days.length-1;
    const rel = counts[d]/max;
    const h = Math.max(4, Math.round(rel*100));
    // Pure magnitude encoding (cooler/quieter -> warmer/busier), not a
    // good/bad read — a busy day isn't "bad," it's just busier.
    const hue = Math.round(190 - rel*140);
    const col = `hsl(${hue<0?hue+360:hue},80%,62%)`;
    return `<div class="tl-bar${edge?' edge':''}" style="height:${h}%;background:linear-gradient(180deg,${col},${col});box-shadow:0 0 8px ${col.replace('hsl(','hsla(').replace(')',',.35)')};"><div class="tip">${d}: ${counts[d]}</div></div>`;
  }).join('') + (excluded ? `<div class="note" style="width:100%;margin-top:6px;">${excluded} row(s) excluded — timezone unknown.</div>` : '');
}
// item.key (identity, used for filtering) defaults to item.name when a
// caller doesn't have a separate identity (e.g. categories, which have no
// id/name split). Fixed 2026-08-19 (2nd audit): previously data-label always
// held the *display* name, which two different identities can share.
// `color` (default) / `i.color` (per-item override) — same visual-identity
// rationale as CATEGORY_COLORS/HUES: distinguishes, never ranks.
function barList(container, items, onClick, color){
  const max = Math.max(1, ...items.map(i=>i.count));
  container.innerHTML = items.length ? items.map(i=>{
    const col = i.color || color || '#5b8cff';
    return `
    <div class="bar-row" data-label="${escapeHtml(i.key ?? i.name)}">
      <div class="bar-label" title="${escapeHtml(i.name)}">${escapeHtml(i.name)}</div>
      <div class="bar-track"><div class="bar-fill" style="${barFillStyle(col, i.count/max*100)}"></div></div>
      <div class="bar-n">${i.count.toLocaleString()}</div>
    </div>`;
  }).join('') : '<div class="empty">No data for current filters.</div>';
  if(onClick) container.querySelectorAll('.bar-row').forEach(row=> row.addEventListener('click', ()=> onClick(row.dataset.label)));
}
function renderUsers(filtered){
  // Fixed 2026-08-19 (2nd audit): human grouping key upgraded to userIdentity
  // (email-preferred) — two different people sharing a display name
  // previously merged into one bar/one filter selection.
  const human = {}, humanDisplay = {}, auto = {};
  filtered.forEach(c=>{
    if(c.actor_type==='human'){ const k=userIdentity(c)||'Unknown'; human[k]=(human[k]||0)+1; humanDisplay[k]=c.user_name||c.user_email||'Unknown'; }
    else if(c.actor_type==='automation'){ const k=c.client_type||c.user_name||'Automation'; auto[k]=(auto[k]||0)+1; }
  });
  const humanList = Object.entries(human).sort((a,b)=>b[1]-a[1]).map(([key,count])=>({key,name:humanDisplay[key],count}));
  const autoList = Object.entries(auto).sort((a,b)=>b[1]-a[1]).map(([name,count])=>({name,count}));
  const el = document.getElementById('userActivity');
  el.innerHTML = '<div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#3ecfb2;margin-bottom:6px;">Human</div><div id="humanBars"></div><div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#f0c040;margin:14px 0 6px;">Automation</div><div id="autoBars"></div>';
  barList(document.getElementById('humanBars'), humanList, label=>{ state.actor='human'; document.getElementById('fActor').value='human'; state.users = state.users.has(label) && state.users.size===1 ? new Set() : new Set([label]); render(); }, '#3ecfb2');
  barList(document.getElementById('autoBars'), autoList, label=>{
    state.actor = state.actor==='automation' && state.search===label.toLowerCase() ? '' : 'automation';
    document.getElementById('fActor').value = state.actor;
    state.search = state.actor ? label.toLowerCase() : '';
    document.getElementById('fSearch').value = state.actor ? label : '';
    document.getElementById('explorerSearch').value = state.actor ? label : '';
    render();
  }, '#f0c040');
}
function renderAccounts(filtered){
  // Fixed 2026-08-19 (2nd audit): nested campaign grouping upgraded to
  // campaignIdentity — two different campaign_ids sharing a name within the
  // SAME account previously merged into one bar with a combined count.
  const accMap = {}, campDisplay = {};
  filtered.forEach(c=>{
    if(!c.account_name) return;
    accMap[c.account_name] = accMap[c.account_name] || {};
    const key = campaignIdentity(c) || '(no campaign)';
    accMap[c.account_name][key] = (accMap[c.account_name][key]||0)+1;
    campDisplay[key] = c.campaign_name || (c.campaign_id ? `Campaign ${c.campaign_id}` : '(no campaign)');
  });
  const accounts = Object.entries(accMap).map(([name,camps])=>({name, camps, total: Object.values(camps).reduce((a,b)=>a+b,0)})).sort((a,b)=>b.total-a.total);
  const el = document.getElementById('accountActivity');
  if(!accounts.length){ el.innerHTML='<div class="empty">No data for current filters.</div>'; return; }
  const maxAcc = Math.max(1, ...accounts.map(a=>a.total));
  el.innerHTML = accounts.map((a,idx)=>{
    const accCol = HUES[idx % HUES.length];
    return `
    <div class="acc-block">
      <div class="acc-head" data-idx="${idx}">
        <span class="chev">▶</span>
        <div class="bar-label" style="flex:0 0 180px;">${escapeHtml(a.name)}</div>
        <div class="bar-track" style="flex:1;"><div class="bar-fill" style="${barFillStyle(accCol, a.total/maxAcc*100)}"></div></div>
        <div class="bar-n" style="flex:0 0 50px;">${a.total}</div>
      </div>
      <div class="camp-list" id="camp-${idx}">
        ${Object.entries(a.camps).sort((x,y)=>y[1]-x[1]).map(([key,n])=>{
          const cmax = Math.max(...Object.values(a.camps));
          const label = campDisplay[key] || key;
          return `<div class="bar-row" data-label="${escapeHtml(label)}"><div class="bar-label">${escapeHtml(label)}</div><div class="bar-track"><div class="bar-fill" style="${barFillStyle(accCol, n/cmax*100)}opacity:.75;"></div></div><div class="bar-n">${n}</div></div>`;
        }).join('')}
      </div>
    </div>`;
  }).join('');
  el.querySelectorAll('.acc-head').forEach(h=> h.addEventListener('click', ()=>{ h.classList.toggle('open'); document.getElementById('camp-'+h.dataset.idx).classList.toggle('open'); }));
  el.querySelectorAll('.camp-list .bar-row').forEach(row=> row.addEventListener('click', e=>{
    e.stopPropagation();
    state.search = row.dataset.label.toLowerCase();
    document.getElementById('fSearch').value = row.dataset.label;
    document.getElementById('explorerSearch').value = row.dataset.label;
    render();
  }));
}
function renderCategories(filtered){
  const counts = {}; filtered.forEach(c=> counts[c.category]=(counts[c.category]||0)+1);
  const list = Object.entries(counts).sort((a,b)=>b[1]-a[1]).map(([name,count])=>({name,count,color:CATEGORY_COLORS[name]||'#8b93a8'}));
  barList(document.getElementById('categoryDist'), list, label=>{ state.categories = state.categories.has(label) && state.categories.size===1 ? new Set() : new Set([label]); render(); });
}
function renderUntouched(filtered){
  // Fixed 2026-08-18: was reading DASH_DATA.untouched directly (a static,
  // unfiltered, Python-precomputed list) — the top filters visibly did
  // nothing here even though every other section respected them. Now
  // recomputed from `filtered` on every render, same (account, campaign)
  // keying as the Python side. "Days since" is computed against the real
  // browser clock (new Date()) at render time, not the data snapshot's end —
  // that "0 days" no longer means "0 days as of whenever this was generated".
  // Fixed 2026-08-19 (2nd audit): key upgraded to campaignIdentity (id-
  // preferred) — same-account same-name-different-id campaigns previously
  // merged. Comparison upgraded to prefer timestamp_utc when resolved, same
  // as the Python side's chrono_key() — a naive-local comparison across
  // rows from genuinely different resolved timezones could pick the wrong
  // "last" change.
  const campaignLast = {};
  filtered.forEach(c=>{
    const cid = campaignIdentity(c);
    if(!cid || !c.timestamp_iso) return;
    const key = accountIdentity(c) + '||' + cid;
    const chrono = c.timestamp_utc || c.timestamp_iso;
    if(!campaignLast[key] || chrono > campaignLast[key]._chrono){
      campaignLast[key] = { campaign_name: c.campaign_name || `Campaign ${c.campaign_id}`, account_name: c.account_name, last_change_ts: c.timestamp_iso, _chrono: chrono };
    }
  });
  const now = new Date();
  const untouched = Object.values(campaignLast).map(u=>{
    const days = Math.floor((now - new Date(u.last_change_ts)) / 86400000);
    return {...u, days_since: days};
  }).sort((a,b)=> b.days_since - a.days_since);

  const activeBtn = document.querySelector('#threshBtns button.active');
  const min = activeBtn ? parseInt(activeBtn.dataset.min) : 0;
  const rows = untouched.filter(u=>u.days_since >= min);
  const el = document.getElementById('untouched');
  if(!rows.length){ el.innerHTML='<div class="empty">No campaigns match this threshold (or current filters).</div>'; return; }
  el.innerHTML = `<table><thead><tr><th>Campaign</th><th>Account</th><th>Last Change</th><th>Days Since (today)</th></tr></thead><tbody>${
    rows.map(u=>`<tr><td>${escapeHtml(u.campaign_name)}</td><td>${escapeHtml(u.account_name||'')}</td><td>${new Date(u.last_change_ts).toLocaleDateString()}</td><td>${u.days_since}</td></tr>`).join('')
  }</tbody></table><div class="note">${escapeHtml(DASH_DATA.meta.untouched_scope_note)} Reflects current filters and today's date in your browser.</div>`;
}
// Fixed 2026-08-18 (3rd audit round, confirmed): sorting the Old/New Value
// columns compared them as raw strings — old_value/new_value are stored as
// strings in the payload, so "100" sorted before "20" (lexicographic).
// old_value_num/new_value_num are already computed for exactly this — use
// them for those two columns when they parsed as numeric, and fall back to
// the string comparison for any column (or any row) that isn't numeric.
function explorerSortValue(c, k){
  if(k==='old_value' && c.old_value_num!=null) return c.old_value_num;
  if(k==='new_value' && c.new_value_num!=null) return c.new_value_num;
  return c[k] ?? '';
}
function compareExplorerSortValues(av, bv){
  if(typeof av==='number' && typeof bv==='number') return av<bv?-1:av>bv?1:0;
  const as=String(av), bs=String(bv);
  return as<bs?-1:as>bs?1:0;
}
function renderExplorer(filtered){
  const sorted = [...filtered].sort((a,b)=>{ const k = state.sortKey; const cmp = compareExplorerSortValues(explorerSortValue(a,k), explorerSortValue(b,k)); return cmp<0 ? -state.sortDir : cmp>0 ? state.sortDir : 0; });
  const totalPages = Math.max(1, Math.ceil(sorted.length / state.pageSize));
  state.page = Math.min(state.page, totalPages);
  const pageRows = sorted.slice((state.page-1)*state.pageSize, state.page*state.pageSize);
  const body = document.getElementById('explorerBody');
  function displayValue(raw, display, unit){
    // Fixed 2026-08-18 (3rd audit round, confirmed): `display` reached the
    // DOM unescaped here. Not currently exploitable — the only caller that
    // sets `unit` (the micros-conversion branch) always passes a computed
    // number/null, never raw source text — but escaping it defensively costs
    // nothing and removes the gap for whatever sets old_value_display next.
    const prefix = unit ? `${escapeHtml(display ?? '—')} ` : '';
    const suffix = unit ? `(raw: ${escapeHtml(raw ?? '—')})` : escapeHtml(raw ?? '—');
    return unit ? `${prefix}<span class="note" style="margin:0;">${suffix}</span>` : suffix;
  }
  body.innerHTML = pageRows.length ? pageRows.map(c=>`
    <tr class="datarow" data-id="${escapeHtml(c.change_id)}">
      <td>${c.timestamp_iso ? new Date(c.timestamp_iso).toLocaleString() : escapeHtml(c.timestamp)}</td>
      <td>${c.actor_type==='human' ? escapeHtml(c.user_name||c.user_email||'—') : `<span class="pill" style="color:#f0c040;border-color:#f0c04055;background:#f0c04018;">${escapeHtml(c.client_type||'automation')}</span>`}</td>
      <td>${escapeHtml(c.account_name||'')}</td><td>${escapeHtml(c.campaign_name||'—')}</td><td>${escapeHtml(c.ad_group_name||'—')}</td>
      <td><span class="pill"${categoryPillStyle(c.category)}>${escapeHtml(c.category)}</span></td><td>${escapeHtml(c.field_name || c.subcategory || '—')}</td>
      <td style="color:var(--bad);">${c.value_confidence==='parsed_from_summary' ? '~' : ''}${displayValue(c.old_value, c.old_value_display, c.value_unit)}</td>
      <td style="color:var(--good);">${c.value_confidence==='parsed_from_summary' ? '~' : ''}${displayValue(c.new_value, c.new_value_display, c.value_unit)}</td>
      <td>${escapeHtml(c.source)}</td>
    </tr>`).join('') : `<tr><td colspan="10" class="empty">No changes match current filters.</td></tr>`;
  body.querySelectorAll('tr.datarow').forEach(row=> row.addEventListener('click', ()=> openDetail(row.dataset.id)));
  document.getElementById('pagerInfo').textContent = `Page ${state.page} / ${totalPages} — ${sorted.length.toLocaleString()} rows`;
  document.getElementById('pagerPrev').disabled = state.page<=1;
  document.getElementById('pagerNext').disabled = state.page>=totalPages;
  document.querySelectorAll('#explorerTable th').forEach(th=>{ th.classList.toggle('sorted', th.dataset.key===state.sortKey); });
}
function openDetail(changeId){
  const c = DASH_DATA.changes.find(x=>x.change_id===changeId);
  if(!c) return;
  const prevVal = c.value_unit ? `${c.old_value_display ?? '—'} ${c.value_unit} (raw: ${c.old_value ?? '—'})` : (c.old_value ?? '—');
  const newVal = c.value_unit ? `${c.new_value_display ?? '—'} ${c.value_unit} (raw: ${c.new_value ?? '—'})` : (c.new_value ?? '—');
  const rows = [
    ['Date', c.timestamp_iso ? new Date(c.timestamp_iso).toLocaleString() : c.timestamp],
    ['User', c.actor_type==='human' ? (c.user_name||c.user_email||'—') : `${c.client_type||'Automation'} (automation)`],
    ['Account', c.account_name||'—'], ['Campaign', c.campaign_name||'—'], ['Ad Group', c.ad_group_name||'—'],
    ['Category', `${c.category} — ${c.subcategory}`], ['Field', c.field_name || '—'],
    ['Previous', prevVal], ['New', newVal],
    ['Operation', c.operation_confidence==='inferred' ? `${c.operation} (inferred, not stated by source)` : c.operation],
    ['Source', c.source], ['Confidence', c.value_confidence],
    ['Resource', c.resource_path || c.resource_id || '—'],
    ['Event ID', c.source_event_id || `${c.change_id} (derived, not from source)`],
  ];
  const rowColor = { Previous:'var(--bad)', New:'var(--good)' };
  document.getElementById('detailBody').innerHTML = rows.map(([k,v])=>`<dt>${escapeHtml(k)}</dt><dd${rowColor[k] ? ` style="color:${rowColor[k]};"` : ''}>${escapeHtml(v)}</dd>`).join('');
  document.getElementById('detailOverlay').classList.add('open');
}
// Borrowed idea (2026-08-18, dashboard-builder skill): a stated filter
// context line, always present — not just when something's active — so a
// screenshot or a paste of this dashboard mid-filter is self-explanatory
// without the reader having to reconstruct which controls are set.
function renderFilterContext(){
  const parts = [];
  if(state.range!=='all') parts.push(state.range==='1' ? 'last 24h' : `last ${state.range}d`);
  if(state.accounts.size) parts.push(`${state.accounts.size} account${state.accounts.size>1?'s':''}`);
  if(state.campaigns.size) parts.push(`${state.campaigns.size} campaign${state.campaigns.size>1?'s':''}`);
  if(state.users.size) parts.push(`${state.users.size} user${state.users.size>1?'s':''}`);
  if(state.actor) parts.push(`actor: ${state.actor}`);
  if(state.categories.size) parts.push(`${state.categories.size} categor${state.categories.size>1?'ies':'y'}`);
  if(state.operations.size) parts.push(`${state.operations.size} operation type${state.operations.size>1?'s':''}`);
  if(state.sources.size) parts.push(`${state.sources.size} source${state.sources.size>1?'s':''}`);
  if(state.search) parts.push(`search: "${state.search}"`);
  document.getElementById('filterContext').textContent = parts.length ? `Showing: ${parts.join(' · ')}` : 'Showing: all changes, no filters applied';
}
function render(){
  const filtered = getFiltered();
  renderFilterContext();
  renderSummary(filtered); renderTimeline(filtered); renderUsers(filtered);
  renderAccounts(filtered); renderCategories(filtered); renderUntouched(filtered); renderExplorer(filtered);
}
document.addEventListener('DOMContentLoaded', ()=>{
  const m = DASH_DATA.meta;
  document.getElementById('metaLine').textContent =
    `${m.row_count.toLocaleString()} changes · ${m.data_window_start ? new Date(m.data_window_start).toLocaleDateString() : '—'} – ${m.data_window_end ? new Date(m.data_window_end).toLocaleDateString() : '—'} · Sources: ${m.sources.join(', ')} · Other: ${m.other_pct}%${m.masked_users ? ' · Users masked' : ''} · Generated ${new Date(m.generated_at).toLocaleString()}`;
  // Detecting "this came from the API" by source_label was unreliable — Layer
  // 2 (alias-table) matches all get the generic label "alias_match" regardless
  // of which known_sources pattern they resembled, so a label-string check
  // silently never fired. source_event_id is only ever populated by the API's
  // own event-identity field, so it's used as the actual signal instead.
  const caveats = [];
  if(DASH_DATA.changes.some(c=>c.source_event_id)){
    caveats.push('Includes data from the Google Ads ChangeEvent API. ChangeEvent may not include every entry from the Google Ads UI\'s Change History — treat counts from this source as a lower bound, not a complete history.');
  }
  if(m.rows_skipped_unparseable_date_pct > 5){
    caveats.push(`${m.rows_skipped_unparseable_date} row(s) (${m.rows_skipped_unparseable_date_pct}% of input) were silently excluded — their date column didn't parse under the format this run resolved. See coverage.txt.`);
  }
  if(caveats.length){
    document.getElementById('apiCaveat').innerHTML = caveats.map(c=>`<div class="warn-box">${escapeHtml(c)}</div>`).join('');
  }
  initFilters();
  document.getElementById('threshBtns').addEventListener('click', e=>{
    if(e.target.tagName!=='BUTTON') return;
    [...e.target.parentElement.children].forEach(b=>b.classList.remove('active'));
    e.target.classList.add('active'); renderUntouched(getFiltered());
  });
  document.getElementById('pagerPrev').addEventListener('click', ()=>{ state.page--; renderExplorer(getFiltered()); });
  document.getElementById('pagerNext').addEventListener('click', ()=>{ state.page++; renderExplorer(getFiltered()); });
  document.querySelectorAll('#explorerTable th').forEach(th=> th.addEventListener('click', ()=>{
    if(state.sortKey===th.dataset.key) state.sortDir *= -1; else { state.sortKey=th.dataset.key; state.sortDir=-1; }
    state.page=1; renderExplorer(getFiltered());
  }));
  document.getElementById('detailClose').addEventListener('click', ()=> document.getElementById('detailOverlay').classList.remove('open'));
  document.getElementById('detailOverlay').addEventListener('click', e=>{ if(e.target.id==='detailOverlay') e.target.classList.remove('open'); });
  render();
});
</script>
</body>
</html>
"""


def build_dashboard(rows, out_path, mask_users=False, generated_at=None, json_out=None, rows_skipped_unparseable_date=0):
    sources = {r.get("source", "unknown") for r in rows}
    other_pct = round(100 * sum(1 for r in rows if r["category"] == "Other") / len(rows), 1) if rows else 0.0
    generated_at = generated_at or datetime.now().isoformat()
    if len(rows) > SCALE_WARN_THRESHOLD:
        print(f"WARNING: {len(rows)} rows exceeds the {SCALE_WARN_THRESHOLD} single-file guideline. Consider a date-range split.")
    data = build_aggregation(rows, sources, other_pct, mask_users, generated_at, date_parse_stats={"rows_skipped_unparseable_date": rows_skipped_unparseable_date})
    marker = "/*__DASH_DATA__*/null"
    # Fixed 2026-08-19 (2nd audit, script-context XSS): escapeHtml() only ever
    # protected the JS side's innerHTML rendering — it did nothing for THIS
    # embedding point, where the JSON blob is written directly, unescaped,
    # into the body of a real <script> tag. json.dumps() does not escape "/",
    # so a value containing the literal text "</script>" (e.g. a malicious
    # campaign_name) closes the real script tag early; the HTML parser then
    # treats whatever follows as ordinary markup. Verified exploitable in a
    # live browser this session: DASH_DATA came back `undefined` and 5 extra
    # <img> tags appeared on the page from a single crafted campaign name.
    # Standard fix (same technique Django's json_script uses): escape the
    # three characters that can open/close an HTML tag to their JS unicode
    # escapes. This is safe *inside* a JSON string literal — < is valid
    # JSON and decodes back to "<" at JS parse time — so DASH_DATA's actual
    # value is unaffected; only the raw HTML text can no longer contain a
    # literal "<", ">", or "&".
    payload = json.dumps(data, ensure_ascii=False)
    payload = payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    html = DASHBOARD_TEMPLATE.replace(marker, f"/*__DASH_DATA__*/{payload}")
    Path(out_path).write_text(html, encoding="utf-8")
    if json_out:
        Path(json_out).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return data


# =====================================================================
# QUERY (deterministic filter — don't eyeball-count rows)
# =====================================================================
def query_changes(rows, user=None, account=None, campaign=None, category=None, operation=None, source=None, since=None):
    """user/account/campaign are substring matches (grep-style — --user
    "User A" also matches "User AB"), deliberately more forgiving than the
    dashboard's exact-match filter dropdowns, which are populated from a
    closed picklist and don't need that leniency. This is a documented,
    intentional difference between the two interfaces, not an oversight
    (flagged as an inconsistency in the 2026-08-19 audit; kept as-is rather
    than made exact, since command-line partial matching is normal/expected
    CLI ergonomics — see SKILL.md)."""
    cutoff = None
    if since:
        n = int(since.rstrip("dwm"))
        unit = since[-1]
        days = n if unit == "d" else n * 7 if unit == "w" else n * 30
        ref_times = [r["timestamp_iso"] for r in rows if r.get("timestamp_iso")]
        ref = max(ref_times) if ref_times else datetime.now().isoformat()
        from datetime import timedelta
        cutoff = datetime.fromisoformat(ref) - timedelta(days=days)

    def keep(r):
        if user and user.lower() not in " ".join(filter(None, [r.get("user_name"), r.get("user_email")])).lower():
            return False
        if account and account.lower() not in (r.get("account_name") or "").lower():
            return False
        if campaign and campaign.lower() not in (r.get("campaign_name") or "").lower():
            return False
        if category and category.lower() != (r.get("category") or "").lower():
            return False
        if operation and operation.upper() != (r.get("operation") or "").upper():
            return False
        if source and source.lower() not in (r.get("source") or "").lower():
            return False
        if cutoff and (not r.get("timestamp_iso") or datetime.fromisoformat(r["timestamp_iso"]) < cutoff):
            return False
        return True

    matched = [r for r in rows if keep(r)]
    matched.sort(key=lambda r: r.get("timestamp_iso") or "", reverse=True)
    return matched


# =====================================================================
# ORCHESTRATION
# =====================================================================
def run_pipeline(input_path, out_dir, mapping_file=None, date_format=None, decimal_style=None,
                  tz="unknown", mask_users=False, allow_unknown_categories=False, force_review=False,
                  extra_rules=None, open_browser=False, generated_at=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    v = validate_source(input_path, mapping_file=mapping_file, force=force_review)
    if v["status"] != "ok":
        return v
    (out_dir / "resolved_mapping.json").write_text(json.dumps(v, ensure_ascii=False), encoding="utf-8")

    n = normalize_changes(input_path, v["mapping"], v["source_label"], date_format=date_format, decimal_style=decimal_style, tz=tz)
    if n["status"] != "ok":
        return n

    deduped_rows, duplicate_count = dedupe_rows(n["rows"])
    n["rows"] = deduped_rows

    c = categorize_changes(n["rows"], allow_unknown=allow_unknown_categories, extra_rules=extra_rules)
    if c["status"] != "ok":
        (out_dir / "unknown-fields.json").write_text(json.dumps(c, indent=2, ensure_ascii=False), encoding="utf-8")
        return c

    # Fixed 2026-08-18 (audit F-04): masked BEFORE any output file is written,
    # so changes.jsonl and the dashboard are never inconsistent and neither
    # can leak an identity --mask-users was asked to hide.
    c["rows"] = mask_rows(c["rows"], mask_users)

    with open(out_dir / "changes.jsonl", "w", encoding="utf-8") as f:
        for row in c["rows"]:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    with open(out_dir / "coverage.txt", "w", encoding="utf-8") as f:
        f.write(f"Total rows: {len(c['rows'])}\n")
        if duplicate_count:
            f.write(f"Duplicates removed (identical change_id): {duplicate_count}\n")
        if n["rows_skipped_unparseable_date"]:
            input_total = len(c["rows"]) + n["rows_skipped_unparseable_date"]
            skip_pct = round(100 * n["rows_skipped_unparseable_date"] / input_total, 1) if input_total else 0.0
            f.write(f"Rows skipped (unparseable date): {n['rows_skipped_unparseable_date']} ({skip_pct}%)\n")
            if skip_pct > 5:
                f.write("WARNING: unparseable-date skip rate exceeds 5% — check the source's date column for a format this run didn't expect.\n")
        f.write(f"Other: {c['other_pct']}%\n")
        if c["other_pct"] > 10:
            f.write("WARNING: Other exceeds 10% — check for a structured_rules gap.\n")
        f.write("\nCategory breakdown:\n")
        for cat, cnt in Counter({k: v2 for k, v2 in c["category_breakdown"].items()}).most_common():
            f.write(f"  {cat}: {cnt}\n")

    data = build_dashboard(c["rows"], out_dir / "dashboard.html", mask_users=mask_users, generated_at=generated_at,
                            json_out=out_dir / "change_history.json", rows_skipped_unparseable_date=n["rows_skipped_unparseable_date"])

    result = {
        "status": "ok", "out": str(out_dir / "dashboard.html"), "row_count": len(c["rows"]),
        "other_pct": c["other_pct"], "accounts": len(data["accounts"]),
        "human_users": len(data["users"]["human"]), "automation_actors": len(data["users"]["automation"]),
        "rows_skipped_unparseable_date": n["rows_skipped_unparseable_date"],
        "duplicates_removed": duplicate_count,
        "date_format_used": n["date_format_used"], "decimal_style_used": n["decimal_style_used"],
    }
    if open_browser:
        webbrowser.open(f"file://{(out_dir / 'dashboard.html').resolve()}")
    return result


# =====================================================================
# BUILT-IN SAMPLE FIXTURES (for self-test — no examples/ folder needed)
# All names are synthetic placeholders: Account A/B, Campaign Alpha/Beta/
# Gamma/Delta, User A/B/C. No real company, client, or person appears here.
# =====================================================================
SAMPLE_LEGACY_SUMMARY_TR_CSV = """Hesap Adı,Hesap ID,Tarih,Kullanıcı,Değişiklik Tipi,Kampanya,Reklam Grubu,Değişiklik Özeti (Nereden -> Nereye),Eski Teknik Veri,Yeni Teknik Veri
Account A,111-111-1111,2026-08-01 09:12:03,User A,CAMPAIGN_BUDGET,Campaign Alpha,,Bütçe Değişti: 150.000 -> 200.000,150.000,200.000
Account A,111-111-1111,2026-08-01 09:15:41,User A,AD_GROUP_CRITERION,Campaign Alpha,Ad Group 1,"TBM Değişti: 3,50 -> 4,20","3,50","4,20"
Account A,111-111-1111,2026-08-02 11:03:22,User B,AD_GROUP_CRITERION,Campaign Alpha,Ad Group 1,Yeni Kelime Eklendi: koşu ayakkabısı,,koşu ayakkabısı
Account A,111-111-1111,2026-08-02 11:04:10,User B,AD_GROUP_CRITERION,Campaign Alpha,Ad Group 1,Negatif Kelime Eklendi: ücretsiz,,ücretsiz
Account A,111-111-1111,2026-08-03 14:22:00,ads-budget-system,CAMPAIGN_BUDGET,Campaign Beta,,Bütçe Değişti: 80.000 -> 100.000,80.000,100.000
Account A,111-111-1111,2026-08-04 08:40:55,User A,CAMPAIGN,Campaign Alpha,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED
Account B,222-222-2222,2026-08-05 16:10:00,User C,AD_GROUP_CRITERION,Campaign Gamma,Ad Group 2,Öğe Silindi: eski anahtar kelime,eski anahtar kelime,
Account B,222-222-2222,2026-08-06 10:00:00,User C,CAMPAIGN_BUDGET,Campaign Gamma,,Bütçe Değişti: 50.000 -> 45.000,50.000,45.000
Account B,222-222-2222,2026-08-07 09:30:00,User B,AD_GROUP_CRITERION,Campaign Delta,Ad Group 3,Yeni Kelime Eklendi: spor ayakkabı,,spor ayakkabı
Account B,222-222-2222,2026-08-17 09:45:00,User A,CAMPAIGN,Campaign Alpha,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED
"""

SAMPLE_API_CHANGE_EVENT_JSON = [
    {"change_date_time": "2026-08-01T10:00:00+03:00", "customer_id": "9990001", "resource_name": "customers/9990001/changeEvents/1785657600000000~1~0", "user_email": "user.a@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN_BUDGET", "change_resource_name": "customers/9990001/campaignBudgets/1001", "resource_change_operation": "UPDATE", "changed_fields": "amount_micros", "old_resource": "150000000", "new_resource": "200000000"},
    {"change_date_time": "2026-08-02 09:12:00", "customer_id": "9990001", "resource_name": "customers/9990001/changeEvents/1785744720000000~2~0", "user_email": "user.b@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "AD_GROUP_CRITERION", "change_resource_name": "customers/9990001/adGroupCriteria/3001~keyword", "resource_change_operation": "CREATE", "changed_fields": "keyword.text", "old_resource": "", "new_resource": "running shoes"},
    {"change_date_time": "2026-08-03 14:00:00", "customer_id": "9990002", "resource_name": "customers/9990002/changeEvents/1785844800000000~3~0", "user_email": None, "client_type": "GOOGLE_ADS_RECOMMENDATIONS", "change_resource_type": "CAMPAIGN_BUDGET", "change_resource_name": "customers/9990002/campaignBudgets/1002", "resource_change_operation": "UPDATE", "changed_fields": "amount_micros", "old_resource": "80000000", "new_resource": "110000000"},
    {"change_date_time": "2026-08-04 08:40:00", "customer_id": "9990001", "resource_name": "customers/9990001/changeEvents/1785918000000000~4~0", "user_email": "user.a@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/9990001/campaigns/2001", "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
    # CAMPAIGN_CRITERION/proximity_target: chosen because FEED_ITEM (this
    # fixture's original example here) gained real category coverage as part
    # of the 2026-08-18 audit fixes (F-11) and stopped exercising the
    # ask-don't-guess path this fixture exists to test. proximity_target is
    # not among CAMPAIGN_CRITERION's covered field names (location, device,
    # ad_schedule, language, user_list, bid_modifier, negative).
    {"change_date_time": "2026-08-17 08:30:00", "customer_id": "9990002", "resource_name": "customers/9990002/changeEvents/1786998600000000~5~0", "user_email": None, "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN_CRITERION", "change_resource_name": "customers/9990002/campaignCriteria/5001", "resource_change_operation": "UPDATE", "changed_fields": "proximity_target", "old_resource": "5km", "new_resource": "10km"},
]

SAMPLE_UNKNOWN_FORMAT_CSV = """Modified On,Modified By Email,Ads Account,Camp Name,AdGroup Name,What Changed,Was,Now
2026-08-01 09:00:00,user.a@example.test,Account A,Campaign Alpha,Ad Group 1,Bid changed,3.50,4.20
2026-08-02 10:00:00,user.b@example.test,Account A,Campaign Alpha,Ad Group 1,Keyword added,,running shoes
2026-08-03 11:00:00,user.c@example.test,Account B,Campaign Gamma,Ad Group 2,Budget changed,50000,45000
"""


def self_test():
    import tempfile
    global PROFILES_DIR
    ok = True
    real_profiles_dir = PROFILES_DIR

    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # Isolate mapping-profiles/ for the duration of the test — otherwise
        # a saved profile from one run leaks into the next and self-test
        # results depend on prior runs instead of being reproducible.
        PROFILES_DIR = td / "mapping-profiles"
        # --- fixture 1: known legacy_summary_tr, full run in one call ---
        f1 = td / "legacy.csv"
        f1.write_text(SAMPLE_LEGACY_SUMMARY_TR_CSV, encoding="utf-8")
        r1 = run_pipeline(f1, td / "out1", decimal_style="TR", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r1["status"] == "ok", f"fixture1 failed: {r1}"
        assert r1["row_count"] == 10, f"fixture1 row_count={r1['row_count']}"
        assert (td / "out1" / "dashboard.html").exists()
        print(f"[PASS] legacy_summary_tr: {r1['row_count']} rows, {r1['other_pct']}% Other, "
              f"{r1['human_users']} human users, {r1['automation_actors']} automation actor(s)")

        # --- fixture 2: API JSON, includes one unmapped category (CAMPAIGN_CRITERION/proximity_target) ---
        f2 = td / "api.json"
        f2.write_text(json.dumps(SAMPLE_API_CHANGE_EVENT_JSON), encoding="utf-8")
        r2 = run_pipeline(f2, td / "out2", generated_at="2026-08-17T00:00:00")
        assert r2["status"] == "needs_category_review", f"fixture2 should need category review, got: {r2['status']}"
        assert any(u["field_name"] == "proximity_target" for u in r2["unknown_combinations"]), "fixture2: expected CAMPAIGN_CRITERION/proximity_target to be flagged unknown"
        print(f"[PASS] api_change_event: correctly stopped for unknown category (CAMPAIGN_CRITERION/proximity_target) instead of guessing")

        r2b = run_pipeline(f2, td / "out2b", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r2b["status"] == "ok", f"fixture2b failed: {r2b}"
        assert r2b["row_count"] == 5
        print(f"[PASS] api_change_event with --allow-unknown-categories: {r2b['row_count']} rows built")

        # regression guard: ISO offset ('+03:00') must resolve to UTC WITHOUT
        # a --timezone flag — this silently broke before the 2026-08-18 fix.
        with open(td / "out2b" / "changes.jsonl", encoding="utf-8") as fh:
            api_rows = [json.loads(line) for line in fh]
        offset_row = next(r for r in api_rows if r["timestamp"].endswith("+03:00"))
        assert offset_row["timestamp_utc"] == "2026-08-01T07:00:00+00:00", f"offset not preserved: {offset_row['timestamp_utc']}"
        assert offset_row["timezone"] == "+03:00"
        print(f"[PASS] ISO offset ('+03:00') resolves to UTC with no --timezone flag needed")

        # regression guard: micros amounts must be human-readable, raw preserved
        micros_row = next(r for r in api_rows if r["field_name"] == "amount_micros" and r["old_value"] == "150000000")
        assert micros_row["old_value_display"] == 150.0, f"micros not converted: {micros_row}"
        assert micros_row["old_value"] == "150000000", "raw value must never be overwritten"
        print(f"[PASS] amount_micros converted to human-readable display value, raw value preserved")

        # regression guard: ChangeEvent's own event identity (resource_name)
        # must be captured as source_event_id and used as event_id, not
        # silently discarded in favor of the content-hash change_id.
        assert offset_row["source_event_id"] == "customers/9990001/changeEvents/1785657600000000~1~0"
        assert offset_row["event_id"] == offset_row["source_event_id"]
        assert offset_row["event_id"] != offset_row["change_id"]
        print(f"[PASS] source_event_id captured from API's own resource_name, used as event_id")

        # regression guard: dedup — a re-imported/duplicate row must collapse,
        # not double-count. change_id was computed but never filtered on before
        # the 2026-08-18 fix.
        dup_path = td / "dup.json"
        dup_data = json.loads(json.dumps(SAMPLE_API_CHANGE_EVENT_JSON[:1] * 2))  # exact duplicate row
        dup_path.write_text(json.dumps(dup_data), encoding="utf-8")
        r_dup = run_pipeline(dup_path, td / "out_dup", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r_dup["status"] == "ok", f"dedup fixture failed: {r_dup}"
        assert r_dup["row_count"] == 1, f"expected dedup to collapse 2 identical rows to 1, got {r_dup['row_count']}"
        assert r_dup["duplicates_removed"] == 1
        print(f"[PASS] identical duplicate rows collapse to 1 (duplicates_removed={r_dup['duplicates_removed']})")

        # regression guard: same campaign name in two different accounts must
        # NOT collapse into one "Campaign Last Changes" entry — this silently
        # lost one of them before the 2026-08-18 fix.
        collision_csv = td / "collision.csv"
        collision_csv.write_text(
            "Hesap Adı,Hesap ID,Tarih,Kullanıcı,Değişiklik Tipi,Kampanya,Reklam Grubu,"
            "Değişiklik Özeti (Nereden -> Nereye),Eski Teknik Veri,Yeni Teknik Veri\n"
            "Account A,1,2026-08-01 09:00:00,User A,CAMPAIGN,Brand,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED\n"
            "Account B,2,2026-08-05 09:00:00,User B,CAMPAIGN,Brand,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED\n",
            encoding="utf-8",
        )
        r_collision = run_pipeline(collision_csv, td / "out_collision", decimal_style="TR", generated_at="2026-08-17T00:00:00")
        assert r_collision["status"] == "ok", f"collision fixture failed: {r_collision}"
        with open(td / "out_collision" / "change_history.json", encoding="utf-8") as fh:
            ch = json.load(fh)
        brand_entries = [u for u in ch["untouched"] if u["campaign_name"] == "Brand"]
        assert len(brand_entries) == 2, f"same-named campaign in 2 accounts must produce 2 entries, got {len(brand_entries)}: {brand_entries}"
        assert {e["account_name"] for e in brand_entries} == {"Account A", "Account B"}
        print(f"[PASS] same-named campaign in 2 different accounts produces 2 separate Campaign Last Changes entries")

        # regression guard: GOOGLE_ADS_API must not be auto-classified human —
        # it's genuinely ambiguous (could be a human's script or a service account).
        assert derive_actor_type(None, "GOOGLE_ADS_API") == "unknown", "GOOGLE_ADS_API must not default to human"
        print(f"[PASS] GOOGLE_ADS_API client_type classified as 'unknown' actor, not assumed human")

        # regression guard: escapeHtml must exist in the shipped dashboard —
        # weak but real guard against a future edit silently removing it and
        # reopening the XSS hole verified manually in the browser this session.
        dashboard_html = (td / "out2b" / "dashboard.html").read_text(encoding="utf-8")
        assert "function escapeHtml" in dashboard_html, "escapeHtml helper missing from shipped dashboard"
        assert dashboard_html.count("escapeHtml(") > 15, "escapeHtml used too rarely — likely missing from a render path"
        print(f"[PASS] escapeHtml present and used broadly in the shipped dashboard")

        # regression guard: Explorer table's numeric-aware sort comparator
        # (3rd audit round fix) must ship in the dashboard. The actual sort
        # LOGIC was verified directly under Node this session — [100, 20]
        # sorted by old_value_num now correctly returns [20, 100], not the
        # previous lexicographic ['100', '20'] — this just guards the
        # function against being silently removed later.
        assert "function explorerSortValue" in dashboard_html, "numeric-aware Explorer sort helper missing from shipped dashboard"
        assert "compareExplorerSortValues" in dashboard_html, "numeric-aware Explorer sort comparator missing from shipped dashboard"
        print(f"[PASS] Explorer table's numeric-aware sort comparator present in shipped dashboard (verified separately under Node: [100,20] sorts to [20,100], not lexicographic)")

        # Found 2026-08-18 via a synthetic-dataset dry run (not a pasted
        # audit): "user_name" was missing from its own alias list.
        mapping, unmatched = try_alias_match(["user_name", "user_email"], [{"user_name": "ads-budget-system"}])
        assert mapping.get("user_name") == "user_name", f"literal 'user_name' column must self-map, got mapping={mapping}"
        f_un = td / "user_name_selfmap.json"
        f_un.write_text(json.dumps([
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "un1",
             "change_resource_type": "CAMPAIGN_BUDGET", "resource_change_operation": "UPDATE", "changed_fields": "amount_micros",
             "old_resource": "10000000", "new_resource": "20000000", "user_name": "ads-budget-system"},
        ]), encoding="utf-8")
        r_un = run_pipeline(f_un, td / "out_un", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_un["status"] == "ok", f"user_name self-map fixture failed: {r_un}"
        with open(td / "out_un" / "changes.jsonl", encoding="utf-8") as fh:
            un_row = json.loads(fh.readline())
        assert un_row["actor_type"] == "automation", f"a literal 'user_name' column carrying 'ads-budget-system' must resolve to actor_type=automation, got {un_row['actor_type']!r} (user_name={un_row['user_name']!r})"
        print(f"[PASS] 'user_name' now self-maps like user_email/client_type already did — a source using that literal column name correctly resolves actor_type instead of silently falling back to 'unknown'")

        # Found 2026-08-18 via a synthetic-dataset dry run (not a pasted
        # audit): read_rows()'s JSON branch derived headers from rows[0].keys()
        # only. A heterogeneous JSON array — human rows with user_email,
        # automation rows with user_name instead, same file — silently lost
        # whichever key wasn't on the first row.
        f_hetero = td / "hetero_json.json"
        f_hetero.write_text(json.dumps([
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "h1",
             "change_resource_type": "CAMPAIGN", "resource_change_operation": "UPDATE", "changed_fields": "status",
             "old_resource": "ENABLED", "new_resource": "PAUSED", "user_email": "a@example.test"},
            {"change_date_time": "2026-08-02 09:00:00", "customer_id": "1", "resource_name": "h2",
             "change_resource_type": "CAMPAIGN_BUDGET", "resource_change_operation": "UPDATE", "changed_fields": "amount_micros",
             "old_resource": "10000000", "new_resource": "20000000", "user_name": "ads-budget-system"},
        ]), encoding="utf-8")
        headers_hetero, rows_hetero = read_rows(f_hetero)
        assert "user_name" in headers_hetero, f"a key present only on a later row must still appear in headers, got {headers_hetero}"
        r_hetero = run_pipeline(f_hetero, td / "out_hetero", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_hetero["status"] == "ok", f"heterogeneous-JSON fixture failed: {r_hetero}"
        with open(td / "out_hetero" / "changes.jsonl", encoding="utf-8") as fh:
            hetero_rows = [json.loads(l) for l in fh]
        auto_row = next(r for r in hetero_rows if r["timestamp"] == "2026-08-02 09:00:00")
        assert auto_row["actor_type"] == "automation", f"row-2's user_name (present only on row 2, not row 1) must still map and resolve actor_type=automation, got {auto_row['actor_type']!r}"
        print(f"[PASS] read_rows(): JSON headers now come from the union of ALL rows' keys, not just row 0 — a key only present on a later row is no longer silently invisible to mapping")

        # Found 2026-08-18 via a synthetic-dataset dry run (not a pasted
        # audit): check_hard_required() only ran on the freshly-computed
        # alias-match path — a --mapping-file that genuinely omits a
        # hard-required field (e.g. its source has no resource_type column at
        # all) used to sail through as status "ok" with 100% "Other"
        # categorization and 0 tracked accounts, no warning at all.
        f_incomplete = td / "p3_incomplete_source.csv"
        f_incomplete.write_text(
            "Mod_Date,User_Mail,Camp_Title,Old_Val,New_Val\n"
            "2026-08-01 09:00:00,a@example.test,Campaign Alpha,ENABLED,PAUSED\n",
            encoding="utf-8",
        )
        f_incomplete_map = td / "p3_incomplete_mapping.json"
        f_incomplete_map.write_text(json.dumps({
            "source_label": "test_incomplete",
            "mapping": {"timestamp": "Mod_Date", "user_email": "User_Mail", "campaign_name": "Camp_Title", "old_value": "Old_Val", "new_value": "New_Val"},
            # deliberately no account_name/account_id, no resource_type
        }), encoding="utf-8")
        r_incomplete = validate_source(f_incomplete, mapping_file=f_incomplete_map, save_profile=False)
        assert r_incomplete["status"] == "needs_mapping", f"a --mapping-file missing a hard-required field must still stop with needs_mapping, got: {r_incomplete}"
        assert any("resource_type" in m for m in r_incomplete["missing_required"]), f"missing_required must name resource_type: {r_incomplete['missing_required']}"
        assert any("account_ref" in m for m in r_incomplete["missing_required"]), f"missing_required must name the account ref: {r_incomplete['missing_required']}"
        print(f"[PASS] check_hard_required() now runs for --mapping-file and cached-fingerprint mappings too, not just the fresh alias-match path — an incomplete mapping stops with needs_mapping instead of silently producing 100% 'Other' output")

        # =================================================================
        # Regression guards for the 2026-08-18 forensic audit's findings.
        # =================================================================

        # F-01: two DIFFERENT real events (distinct source_event_id) sharing
        # every other hash-relevant field must NOT collapse into one row.
        collision_rows = [
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "customers/1/changeEvents/1~1~0",
             "user_email": "a@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN_BUDGET",
             "change_resource_name": "customers/1/campaignBudgets/100", "resource_change_operation": "UPDATE",
             "changed_fields": "amount_micros", "old_resource": "1000000", "new_resource": "2000000"},
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "customers/1/changeEvents/1~2~0",
             "user_email": "a@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN_BUDGET",
             "change_resource_name": "customers/1/campaignBudgets/100", "resource_change_operation": "UPDATE",
             "changed_fields": "amount_micros", "old_resource": "1000000", "new_resource": "2000000"},
        ]
        f_collision = td / "f01_collision.json"
        f_collision.write_text(json.dumps(collision_rows), encoding="utf-8")
        r_collision2 = run_pipeline(f_collision, td / "out_f01", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r_collision2["status"] == "ok", f"F-01 fixture failed: {r_collision2}"
        assert r_collision2["row_count"] == 2, f"F-01: two distinct events (different source_event_id) must both survive, got row_count={r_collision2['row_count']}"
        assert r_collision2["duplicates_removed"] == 0, f"F-01: these are not duplicates, duplicates_removed should be 0, got {r_collision2['duplicates_removed']}"
        print(f"[PASS] F-01: two distinct real events (different source_event_id, identical other fields) both survive dedup")

        # F-02: changed_fields as a real JSON list (repeated FieldMask) must
        # not crash — joined into one field_name string, multi_field flagged.
        fieldmask_rows = [
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "customers/1/changeEvents/2~1~0",
             "user_email": "a@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN",
             "change_resource_name": "customers/1/campaigns/200", "resource_change_operation": "UPDATE",
             "changed_fields": ["status", "name"], "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_fieldmask = td / "f02_fieldmask.json"
        f_fieldmask.write_text(json.dumps(fieldmask_rows), encoding="utf-8")
        r_fieldmask = run_pipeline(f_fieldmask, td / "out_f02", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r_fieldmask["status"] == "ok", f"F-02 fixture failed: {r_fieldmask}"
        with open(td / "out_f02" / "changes.jsonl", encoding="utf-8") as fh:
            fieldmask_row = json.loads(fh.readline())
        assert fieldmask_row["multi_field"] is True
        assert fieldmask_row["field_name"] == "status, name"
        print(f"[PASS] F-02: changed_fields as a JSON list no longer crashes (multi_field=True, field_name='{fieldmask_row['field_name']}')")

        # F-03: Google's real top-level ChangeEvent.campaign / .ad_group
        # fields (resource-name/path strings) must be captured correctly —
        # into campaign_id/ad_group_id (path tail) and campaign_resource/
        # ad_group_resource (full path), never into campaign_name/
        # ad_group_name (reserved for actual display names).
        attributed_rows = [
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "customers/1/changeEvents/3~1~0",
             "user_email": "a@example.test", "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "AD_GROUP_CRITERION",
             "change_resource_name": "customers/1/adGroupCriteria/1~kw",
             "campaign": "customers/1/campaigns/500", "ad_group": "customers/1/adGroups/900",
             "resource_change_operation": "CREATE", "changed_fields": "keyword.text",
             "old_resource": "", "new_resource": "running shoes"},
        ]
        f_attr = td / "f03_attributed.json"
        f_attr.write_text(json.dumps(attributed_rows), encoding="utf-8")
        r_attr = run_pipeline(f_attr, td / "out_f03", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_attr["status"] == "ok", f"F-03 fixture failed: {r_attr}"
        with open(td / "out_f03" / "changes.jsonl", encoding="utf-8") as fh:
            attr_row = json.loads(fh.readline())
        assert attr_row["campaign_id"] == "500", f"F-03: campaign_id should be '500', got {attr_row['campaign_id']!r}"
        assert attr_row["campaign_resource"] == "customers/1/campaigns/500"
        assert attr_row["campaign_name"] is None, f"F-03: campaign_name must NOT hold a resource path, got {attr_row['campaign_name']!r}"
        assert attr_row["ad_group_id"] == "900", f"F-03: ad_group_id should be '900', got {attr_row['ad_group_id']!r}"
        assert attr_row["ad_group_resource"] == "customers/1/adGroups/900"
        print(f"[PASS] F-03: ChangeEvent's real campaign/ad_group fields captured correctly (campaign_id={attr_row['campaign_id']}, ad_group_id={attr_row['ad_group_id']})")

        # F-04: --mask-users must mask changes.jsonl too, not just the dashboard.
        mask_rows_test = [
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "customers/1/changeEvents/4~1~0",
             "user_email": "secret.user@realcompany.example", "client_type": "GOOGLE_ADS_WEB_CLIENT",
             "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_mask = td / "f04_mask.json"
        f_mask.write_text(json.dumps(mask_rows_test), encoding="utf-8")
        r_mask = run_pipeline(f_mask, td / "out_f04", allow_unknown_categories=True, mask_users=True, generated_at="2026-08-17T00:00:00")
        assert r_mask["status"] == "ok", f"F-04 fixture failed: {r_mask}"
        jsonl_text = (td / "out_f04" / "changes.jsonl").read_text(encoding="utf-8")
        dashboard_text = (td / "out_f04" / "dashboard.html").read_text(encoding="utf-8")
        assert "secret.user@realcompany.example" not in jsonl_text, "F-04: --mask-users must not leak the raw email in changes.jsonl"
        assert "secret.user@realcompany.example" not in dashboard_text
        assert '"user_name": "User A"' in jsonl_text
        print(f"[PASS] F-04: --mask-users masks changes.jsonl (previously leaked the raw email there)")

        # F-05: same campaign name in 2 accounts must not collapse in the
        # Summary panel's changed_campaigns count / most_active_campaign —
        # same defect class as campaign_last (F-11-adjacent, fixed earlier),
        # found unfixed in this sibling aggregation path.
        f_f05 = td / "f05_summary.csv"
        f_f05.write_text(
            "Hesap Adı,Hesap ID,Tarih,Kullanıcı,Değişiklik Tipi,Kampanya,Reklam Grubu,"
            "Değişiklik Özeti (Nereden -> Nereye),Eski Teknik Veri,Yeni Teknik Veri\n"
            "Account A,1,2026-08-01 09:00:00,User A,CAMPAIGN,Brand,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED\n"
            "Account A,1,2026-08-02 09:00:00,User A,CAMPAIGN,Brand,,Durum Değişti: PAUSED -> ENABLED,PAUSED,ENABLED\n"
            "Account B,2,2026-08-03 09:00:00,User B,CAMPAIGN,Brand,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED\n",
            encoding="utf-8",
        )
        r_f05 = run_pipeline(f_f05, td / "out_f05", decimal_style="TR", generated_at="2026-08-17T00:00:00")
        assert r_f05["status"] == "ok", f"F-05 fixture failed: {r_f05}"
        with open(td / "out_f05" / "change_history.json", encoding="utf-8") as fh:
            ch_f05 = json.load(fh)
        assert ch_f05["summary"]["changed_campaigns"] == 2, f"F-05: same-named campaign in 2 accounts must count as 2, got {ch_f05['summary']['changed_campaigns']}"
        assert "(Account A)" in ch_f05["summary"]["most_active_campaign"], f"F-05: most_active_campaign should disambiguate by account, got {ch_f05['summary']['most_active_campaign']!r}"
        print(f"[PASS] F-05: Summary.changed_campaigns/most_active_campaign correctly account-scoped ({ch_f05['summary']['changed_campaigns']} campaigns, top={ch_f05['summary']['most_active_campaign']!r})")

        # F-07: operation="" (present but empty) must be flagged
        # operation_confidence='inferred'; explicit UNKNOWN/UNSPECIFIED must
        # stay 'explicit' and un-coerced.
        op_rows = [
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "e1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-08-02 10:00:00", "customer_id": "1", "resource_name": "e2", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/2",
             "resource_change_operation": "UNKNOWN", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_op = td / "f07_op.json"
        f_op.write_text(json.dumps(op_rows), encoding="utf-8")
        r_op = run_pipeline(f_op, td / "out_f07", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        with open(td / "out_f07" / "changes.jsonl", encoding="utf-8") as fh:
            op_result_rows = [json.loads(l) for l in fh]
        empty_op_row = next(r for r in op_result_rows if r["source_event_id"] == "e1")
        explicit_unknown_row = next(r for r in op_result_rows if r["source_event_id"] == "e2")
        assert empty_op_row["operation"] == "UPDATE" and empty_op_row["operation_confidence"] == "inferred"
        assert explicit_unknown_row["operation"] == "UNKNOWN" and explicit_unknown_row["operation_confidence"] == "explicit"
        print(f"[PASS] F-07: operation_confidence distinguishes inferred UPDATE from explicit UNKNOWN")

        # F-08/F-09: SEARCH_ADS_360_POST must classify the same as its sibling
        # SEARCH_ADS_360_SYNC (both automation) — was inconsistent before.
        assert derive_actor_type(None, "SEARCH_ADS_360_SYNC") == "automation"
        assert derive_actor_type(None, "SEARCH_ADS_360_POST") == "automation", "F-08/F-09: SEARCH_ADS_360_POST must match its SA360 sibling"
        print(f"[PASS] F-08/F-09: SEARCH_ADS_360_SYNC and SEARCH_ADS_360_POST classify consistently (both automation)")

        # F-11: the 6 previously-zero-coverage real ChangeEventResourceType
        # values must now resolve to a real category, not Other/unmatched.
        for rt, expected_category in [("FEED", "Feed"), ("FEED_ITEM", "Feed"), ("CAMPAIGN_FEED", "Feed"),
                                       ("AD_GROUP_FEED", "Feed"), ("AD_GROUP_BID_MODIFIER", "Bidding"),
                                       ("CAMPAIGN_ASSET_SET", "Asset")]:
            hit = match_structured({"resource_type": rt, "field_name": "x", "operation": "UPDATE", "new_value": ""})
            assert hit is not None and hit[0] == expected_category, f"F-11: {rt} should categorize as {expected_category}, got {hit}"
        print(f"[PASS] F-11: all 6 previously-uncovered resource types (FEED, FEED_ITEM, CAMPAIGN_FEED, AD_GROUP_FEED, AD_GROUP_BID_MODIFIER, CAMPAIGN_ASSET_SET) now categorize")

        # F-16/F-17/F-18: malformed input must return a structured error, not
        # crash with a raw Python exception.
        f_empty = td / "f16_empty.csv"; f_empty.write_text("", encoding="utf-8")
        assert run_pipeline(f_empty, td / "out_f16", generated_at="2026-08-17T00:00:00")["status"] == "error"
        f_badjson = td / "f17_bad.json"; f_badjson.write_text("{not valid json", encoding="utf-8")
        assert run_pipeline(f_badjson, td / "out_f17", generated_at="2026-08-17T00:00:00")["status"] == "error"
        assert run_pipeline(td / "f18_missing.csv", td / "out_f18", generated_at="2026-08-17T00:00:00")["status"] == "error"
        print(f"[PASS] F-16/F-17/F-18: empty CSV, invalid JSON, missing file all return structured errors (no uncaught exceptions)")

        # F-19: duplicate column headers must be detected and warned about.
        f_dup = td / "f19_dup.csv"
        f_dup.write_text("Date,Date,User,Account,Campaign,Change,Was,Now\n2026-08-01,2026-08-01,U1,A,C1,status,ENABLED,PAUSED\n", encoding="utf-8")
        dup_mapping = {"source_label": "t", "mapping": {"timestamp": "Date", "user_name": "User", "account_name": "Account",
                       "campaign_name": "Campaign", "raw_summary": "Change", "old_value": "Was", "new_value": "Now", "resource_type": "Change"}}
        f_dup_map = td / "f19_dup_mapping.json"; f_dup_map.write_text(json.dumps(dup_mapping), encoding="utf-8")
        v_dup = validate_source(f_dup, mapping_file=f_dup_map, save_profile=False)
        assert any("Duplicate column header" in w for w in v_dup.get("warnings", [])), f"F-19: duplicate header warning missing: {v_dup.get('warnings')}"
        print(f"[PASS] F-19: duplicate column headers are detected and warned about")

        # =================================================================
        # Regression guards for the 2026-08-19 (2nd) forensic audit's findings.
        # =================================================================

        # P0: script-context XSS. json.dumps() embedded raw into a <script>
        # tag never protected against a value containing the literal text
        # "</script>" — verified exploitable in a live browser (DASH_DATA
        # came back `undefined`, 5 extra <img> tags appeared on the page).
        xss_csv = td / "p0_xss.csv"
        xss_csv.write_text(
            "Hesap Adı,Hesap ID,Tarih,Kullanıcı,Değişiklik Tipi,Kampanya,Reklam Grubu,"
            "Değişiklik Özeti (Nereden -> Nereye),Eski Teknik Veri,Yeni Teknik Veri\n"
            'Account A,1,2026-08-01 09:00:00,User A,CAMPAIGN_BUDGET,"</script><img src=x onerror=alert(1)>",'
            ',Bütçe Değişti: 100.000 -> 150.000,100.000,150.000\n',
            encoding="utf-8",
        )
        r_xss = run_pipeline(xss_csv, td / "out_p0xss", decimal_style="TR", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r_xss["status"] == "ok", f"P0 XSS fixture failed: {r_xss}"
        xss_html = (td / "out_p0xss" / "dashboard.html").read_text(encoding="utf-8")
        assert "</script><img" not in xss_html, "P0: raw '</script><img' must never appear unescaped in the shipped HTML"
        assert "\\u003c/script\\u003e\\u003cimg" in xss_html, "P0: the payload should appear as its escaped-unicode form instead"
        print(f"[PASS] P0: script-context XSS — payload appears only as escaped unicode, never as raw '</script>'")

        # P1: campaign identity within the SAME account, by id not name —
        # two different campaign_ids sharing a name in one account previously
        # merged into one Campaign Last Changes / Summary entry.
        camp_id_rows = [
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "ce1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/100",
             "campaign": "customers/1/campaigns/100", "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-08-02 09:00:00", "customer_id": "1", "resource_name": "ce2", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/200",
             "campaign": "customers/1/campaigns/200", "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_campid = td / "p1_campaign_id.json"
        f_campid.write_text(json.dumps(camp_id_rows), encoding="utf-8")
        r_campid = run_pipeline(f_campid, td / "out_p1campid", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_campid["status"] == "ok", f"P1 campaign-id fixture failed: {r_campid}"
        with open(td / "out_p1campid" / "change_history.json", encoding="utf-8") as fh:
            ch_campid = json.load(fh)
        assert ch_campid["summary"]["changed_campaigns"] == 2, f"P1: two different campaign_ids (100, 200), same account, no display name, must count as 2, got {ch_campid['summary']['changed_campaigns']}"
        assert len(ch_campid["untouched"]) == 2, f"P1: Campaign Last Changes must list 2 separate entries, got {len(ch_campid['untouched'])}"
        print(f"[PASS] P1: two different campaign_ids in the same account (no display name) count as 2 campaigns, not 1")

        # P1: change_history.json's untouched entries use the renamed,
        # explicitly-static field name — the dashboard's own live JS
        # computation is a DIFFERENT number by design (real browser clock vs
        # this file's own data-window snapshot) and must not share a field
        # name that implies they're the same thing.
        with open(td / "out_p1campid" / "change_history.json", encoding="utf-8") as fh:
            ch_rename = json.load(fh)
        assert "days_since_last_change_at_generation" in ch_rename["untouched"][0], "P1: untouched entries must use the renamed, explicitly-static field"
        assert "days_since_last_change" not in ch_rename["untouched"][0], "P1: the old ambiguous field name must be gone"
        print(f"[PASS] P1: change_history.json's untouched field renamed to days_since_last_change_at_generation (no longer implies it matches the dashboard's live figure)")

        # P1: unparseable-date rows are now visible in meta + coverage.txt,
        # not just a JSON field nobody is required to check.
        mixed_date_rows = [
            {"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "resource_name": "md1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-13-45 10:00:00", "customer_id": "1", "resource_name": "md2", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/2",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_mixdate = td / "p1_mixed_date.json"
        f_mixdate.write_text(json.dumps(mixed_date_rows), encoding="utf-8")
        r_mixdate = run_pipeline(f_mixdate, td / "out_p1mixdate", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r_mixdate["rows_skipped_unparseable_date"] == 1
        coverage_text = (td / "out_p1mixdate" / "coverage.txt").read_text(encoding="utf-8")
        assert "Rows skipped (unparseable date): 1" in coverage_text, f"P1: coverage.txt must report the skip: {coverage_text}"
        with open(td / "out_p1mixdate" / "change_history.json", encoding="utf-8") as fh:
            ch_mixdate = json.load(fh)
        assert ch_mixdate["meta"]["rows_skipped_unparseable_date"] == 1
        print(f"[PASS] P1: unparseable-date rows reported in coverage.txt and change_history.json meta, not just a silently-dropped count")

        # P2: ISO trailing junk must be rejected, not silently truncated —
        # and must not silently drop a valid offset by falling through to
        # the (previously lenient) fallback parser.
        assert True  # covered directly by parse_timestamp unit checks below
        try:
            parse_timestamp("2026-08-01T10:00:00+03:00JUNK", "ISO")
            raise AssertionError("P2: trailing-junk ISO timestamp with an offset must be rejected, not silently truncated")
        except ValueError:
            pass
        print(f"[PASS] P2: ISO timestamp with trailing junk after a valid offset is rejected (previously silently truncated, losing the offset)")

        # P2: DMY format now preserves a trailing offset, matching the ISO
        # branch's existing behavior (previously DMY never even tried).
        dmy_dt = parse_timestamp("01/08/2026 10:00:00+03:00", "DMY")
        assert dmy_dt.tzinfo is not None, "P2: DMY timestamp with an explicit offset must preserve it"
        assert dmy_dt.utcoffset().total_seconds() == 3 * 3600
        print(f"[PASS] P2: DMY-format timestamp with an explicit offset now preserves it (previously always dropped)")

        # P2: CAMPAIGN/AD_GROUP REMOVE must categorize as Campaign
        # removed/Ad group removed, not be shadowed by the generic
        # operation=REMOVE -> Status/Removed catch-all.
        assert match_structured({"resource_type": "CAMPAIGN", "field_name": "status", "operation": "REMOVE", "new_value": ""}) == ("Campaign", "Campaign removed")
        assert match_structured({"resource_type": "AD_GROUP", "field_name": "status", "operation": "REMOVE", "new_value": ""}) == ("AdGroup", "Ad group removed")
        print(f"[PASS] P2: CAMPAIGN/AD_GROUP REMOVE categorize as Campaign removed/Ad group removed, not swallowed by the generic Status/Removed rule")

        # P2: account identity — two different account_ids sharing an
        # account_name must count as 2 active accounts, matching how
        # campaign identity was already fixed to prefer id over name.
        acc_id_rows = [
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "111", "resource_name": "ai1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/111/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-08-02 09:00:00", "customer_id": "222", "resource_name": "ai2", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/222/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_accid = td / "p2_account_id.json"
        f_accid.write_text(json.dumps(acc_id_rows), encoding="utf-8")
        r_accid = run_pipeline(f_accid, td / "out_p2accid", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        with open(td / "out_p2accid" / "change_history.json", encoding="utf-8") as fh:
            ch_accid = json.load(fh)
        assert ch_accid["summary"]["active_accounts"] == 2, f"P2: two different account_ids must count as 2 active accounts, got {ch_accid['summary']['active_accounts']}"
        print(f"[PASS] P2: two different account_ids count as 2 active accounts (account_id-based, not account_name-based)")

        # P2: user identity — two different emails sharing a display name
        # must count as 2 active users, not merge into 1.
        user_id_rows = [
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "ui1", "user_email": "alex1@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-08-02 09:00:00", "customer_id": "1", "resource_name": "ui2", "user_email": "alex2@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/2",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_userid = td / "p2_user_id.json"
        f_userid.write_text(json.dumps(user_id_rows), encoding="utf-8")
        r_userid = run_pipeline(f_userid, td / "out_p2userid", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        with open(td / "out_p2userid" / "change_history.json", encoding="utf-8") as fh:
            ch_userid = json.load(fh)
        assert ch_userid["summary"]["active_users"] == 2, f"P2: two different emails, both actor_type human, must count as 2 active users, got {ch_userid['summary']['active_users']}"
        print(f"[PASS] P2: two different user emails count as 2 active users (email-based identity, not display-name-based)")

        # P3: known_sources is now actually consulted — a matching header set
        # gets a specific source_label instead of the generic "alias_match".
        f_ks_legacy = td / "p3_ks_legacy.csv"
        f_ks_legacy.write_text(SAMPLE_LEGACY_SUMMARY_TR_CSV, encoding="utf-8")
        v_ks_legacy = validate_source(f_ks_legacy, save_profile=False)
        assert v_ks_legacy["source_label"] == "legacy_summary_tr", f"P3: known_sources should identify this as legacy_summary_tr, got {v_ks_legacy['source_label']}"
        f_ks_api = td / "p3_ks_api.json"
        f_ks_api.write_text(json.dumps(SAMPLE_API_CHANGE_EVENT_JSON), encoding="utf-8")
        v_ks_api = validate_source(f_ks_api, save_profile=False)
        assert v_ks_api["source_label"] == "google_ads_api_change_event_json", f"P3: known_sources should identify this as google_ads_api_change_event_json, got {v_ks_api['source_label']}"
        print(f"[PASS] P3: known_sources now actually consulted — source_label names the real format instead of generic 'alias_match'")

        # SUMMARY_CHANGED_FROM_TO_RE: second free-text fallback for summaries
        # phrased as "... changed/increased/decreased from X to Y" (no arrow,
        # so SUMMARY_OLD_NEW_RE wouldn't catch it). No old_value/new_value
        # columns in this fixture on purpose — forces the fallback path.
        f_cft = td / "changed_from_to.csv"
        f_cft.write_text(
            "Account,Account ID,Date,Changed by,Change type,Campaign,Summary\n"
            "Account A,111-111-1111,2026-08-01 09:12:03,User A,CAMPAIGN_BUDGET,Campaign Alpha,Budget changed from 150.000 to 200.000\n"
            "Account A,111-111-1111,2026-08-02 10:00:00,User A,CAMPAIGN,Campaign Alpha,Status increased from ENABLED to PAUSED\n",
            encoding="utf-8",
        )
        r_cft = run_pipeline(f_cft, td / "out_cft", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_cft["status"] == "ok", f"changed-from-to fixture failed: {r_cft}"
        with open(td / "out_cft" / "changes.jsonl", encoding="utf-8") as fh:
            cft_rows = [json.loads(line) for line in fh]
        cft_budget = next(r for r in cft_rows if r["account_name"] == "Account A" and r["old_value"] == "150.000")
        assert cft_budget["new_value"] == "200.000", f"changed-from-to: new_value not parsed: {cft_budget}"
        assert cft_budget["value_confidence"] == "parsed_from_summary", f"changed-from-to: value_confidence wrong: {cft_budget}"
        print(f"[PASS] SUMMARY_CHANGED_FROM_TO_RE: 'X changed/increased/decreased from A to B' summaries now parsed as a second fallback (previously only the '->' arrow phrasing was)")

        # Permanent "what this report can't do" disclosure: unlike apiCaveat's
        # run-specific warn-boxes, this must appear in every dashboard, even
        # one with clean data and no API rows — it's about the tool's own
        # structural limits, not this run's data quality.
        dash_html = (td / "out_cft" / "dashboard.html").read_text(encoding="utf-8")
        assert "What this report can't do" in dash_html, "permanent limitations disclosure missing from dashboard"
        assert "never whether a change was good, risky" in dash_html, "limitations disclosure must stay factual, not judgmental"
        print(f"[PASS] Permanent 'what this report can't do' disclosure present in every dashboard, independent of this run's data")

        # 3rd audit round (2026-08-18), confirmed findings only — every claim
        # below was independently reproduced against this file's actual code
        # before being accepted; most of that round's other claims (dedup
        # collisions, DST crashes, missing pagination, etc.) were refuted on
        # inspection and are NOT fixed here because they don't describe real
        # behavior of this codebase.

        # normalize_number: a negative value with a single separator type
        # (TR or US style) used to match neither DIGIT_GROUP_SHORT nor
        # DIGIT_GROUP_3 (both required a leading digit), fell to the
        # catch-all branch, and had its separator stripped instead of
        # converted — corrupting the magnitude, not just the sign.
        assert normalize_number("-150.00", None) == -150.0, f"negative 2-decimal value corrupted: {normalize_number('-150.00', None)}"
        assert normalize_number("-3,50", "TR") == -3.5, f"negative TR-decimal value corrupted: {normalize_number('-3,50', 'TR')}"
        assert normalize_number("-150.000", "TR") == -150000.0, f"negative TR-thousands value corrupted: {normalize_number('-150.000', 'TR')}"
        print(f"[PASS] normalize_number: negative values with a single separator no longer have their magnitude corrupted (previously '-150.00' became -15000.0)")

        # try_alias_match: a literal lowercase "campaign"/"ad_group" header is
        # only the API's own resource-path field when its VALUES look like
        # one (contain '/'). A plain display-name column that happens to be
        # named "campaign" (lowercase, e.g. a generic snake_case JSON export)
        # must still resolve to campaign_name, not get shadowed into
        # campaign_resource and lose its identity entirely.
        plain_campaign_rows = [
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "user_email": "a@example.test",
             "change_resource_type": "CAMPAIGN", "resource_change_operation": "UPDATE", "changed_fields": "status",
             "old_resource": "ENABLED", "new_resource": "PAUSED", "campaign": "Campaign Alpha"},
        ]
        f_plaincamp = td / "p3_plain_campaign.json"
        f_plaincamp.write_text(json.dumps(plain_campaign_rows), encoding="utf-8")
        r_plaincamp = run_pipeline(f_plaincamp, td / "out_plaincamp", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_plaincamp["status"] == "ok", f"plain-campaign fixture failed: {r_plaincamp}"
        with open(td / "out_plaincamp" / "changes.jsonl", encoding="utf-8") as fh:
            plaincamp_row = json.loads(fh.readline())
        assert plaincamp_row["campaign_name"] == "Campaign Alpha", f"plain 'campaign' column must map to campaign_name, got campaign_name={plaincamp_row['campaign_name']!r} campaign_resource={plaincamp_row['campaign_resource']!r}"
        assert plaincamp_row["campaign_resource"] is None, f"plain 'campaign' column must NOT be forced into campaign_resource, got {plaincamp_row['campaign_resource']!r}"
        print(f"[PASS] try_alias_match: a plain 'campaign' display-name column (values with no '/') resolves to campaign_name, no longer shadowed into campaign_resource by case alone")

        # make_change_id: two rows describing the exact same real event, one
        # with a space-separated raw timestamp and one T-separated, must
        # dedupe to 1 row — previously hashed off raw_ts and never collapsed.
        crossformat_rows = [
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "ev1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-08-01T09:00:00", "customer_id": "1", "resource_name": "ev1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        f_crossfmt = td / "p3_crossformat_ts.json"
        f_crossfmt.write_text(json.dumps(crossformat_rows), encoding="utf-8")
        r_crossfmt = run_pipeline(f_crossfmt, td / "out_crossfmt", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_crossfmt["status"] == "ok", f"cross-format timestamp fixture failed: {r_crossfmt}"
        assert r_crossfmt["row_count"] == 1, f"same event with differently-formatted raw timestamps must dedupe to 1 row, got {r_crossfmt['row_count']}"
        assert r_crossfmt["duplicates_removed"] == 1
        print(f"[PASS] make_change_id: same event with a space-separated vs T-separated raw timestamp now dedupes correctly (previously hashed off raw_ts and never collapsed)")

        # parse_timestamp: a slash-separated ISO-shaped date must parse under
        # explicit --date-format ISO, matching DMY/MDY (which already accept
        # both '/' and '.').
        slash_iso_dt = parse_timestamp("2026/08/01 10:00:00", "ISO")
        assert slash_iso_dt.year == 2026 and slash_iso_dt.month == 8 and slash_iso_dt.day == 1, f"slash-separated ISO date not parsed correctly: {slash_iso_dt}"
        print(f"[PASS] parse_timestamp: slash-separated ISO dates ('2026/08/01') now parse under --date-format ISO (previously always raised unparseable)")

        # read_rows: a JSON array of non-object values must raise a clear
        # ValueError (surfaced as a structured error status), not an
        # unhandled AttributeError from rows[0].keys().
        f_prim = td / "p3_primitives.json"
        f_prim.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
        r_prim = run_pipeline(f_prim, td / "out_prim", generated_at="2026-08-17T00:00:00")
        assert r_prim["status"] == "error", f"primitive-array JSON must return a structured error, got: {r_prim}"
        print(f"[PASS] read_rows: a JSON array of non-object values ([1, 2, 3]) now returns a structured error instead of crashing with AttributeError")

        # read_rows: a JSON object without a recognized wrapper key names the
        # actual problem instead of a generic "no headers found".
        f_unwrapped = td / "p3_unwrapped.json"
        f_unwrapped.write_text(json.dumps({"changes": [{"a": 1}]}), encoding="utf-8")
        r_unwrapped = run_pipeline(f_unwrapped, td / "out_unwrapped", generated_at="2026-08-17T00:00:00")
        assert r_unwrapped["status"] == "error", f"unrecognized dict shape must return a structured error, got: {r_unwrapped}"
        assert "results" in r_unwrapped["message"] or "changeEvents" in r_unwrapped["message"], f"error message should name the missing wrapper key: {r_unwrapped['message']}"
        print(f"[PASS] read_rows: a JSON object without 'results'/'changeEvents' now names the actual problem, not just 'no headers found'")

        # mask_rows: labels must be stable across SEPARATE runs (persisted),
        # not just within one run — previously pure per-run encounter order,
        # so the same person could be "User A" in one report and "User B" in
        # the next depending on row order.
        run_a_rows = [
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "m1", "user_email": "alex@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
            {"change_date_time": "2026-08-02 09:00:00", "customer_id": "1", "resource_name": "m2", "user_email": "blair@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/1",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]
        # Reversed order in a SEPARATE later run — blair now appears first.
        run_b_rows = list(reversed(run_a_rows))
        f_mask_a = td / "p3_mask_a.json"
        f_mask_a.write_text(json.dumps(run_a_rows), encoding="utf-8")
        run_pipeline(f_mask_a, td / "out_mask_a", allow_unknown_categories=True, force_review=True, mask_users=True, generated_at="2026-08-17T00:00:00")
        with open(td / "out_mask_a" / "changes.jsonl", encoding="utf-8") as fh:
            mask_a_rows = [json.loads(l) for l in fh]
        alex_label_a = next(r["user_name"] for r in mask_a_rows if r["change_id"] and r.get("user_email") is None)  # masked: user_email is cleared too
        f_mask_b = td / "p3_mask_b.json"
        f_mask_b.write_text(json.dumps(run_b_rows), encoding="utf-8")
        run_pipeline(f_mask_b, td / "out_mask_b", allow_unknown_categories=True, force_review=True, mask_users=True, generated_at="2026-08-17T00:00:00")
        with open(td / "out_mask_b" / "changes.jsonl", encoding="utf-8") as fh:
            mask_b_rows = [json.loads(l) for l in fh]
        # alex's change is identifiable by its resource_name-derived event_id being stable across both runs
        alex_event_id = next(r["event_id"] for r in mask_a_rows if r["timestamp"] == "2026-08-01 09:00:00")
        alex_label_run_a = next(r["user_name"] for r in mask_a_rows if r["event_id"] == alex_event_id)
        alex_label_run_b = next(r["user_name"] for r in mask_b_rows if r["event_id"] == alex_event_id)
        assert alex_label_run_a == alex_label_run_b, f"same real identity got different mask labels across separate runs: {alex_label_run_a!r} vs {alex_label_run_b!r}"
        print(f"[PASS] mask_rows: the same real identity keeps the same mask label ('{alex_label_run_a}') across separate runs, even when row order differs (previously per-run encounter order only)")

        # regression guard (audit F-13): the .tsv code path existed but was
        # never exercised by any fixture until now. Re-serialized properly
        # via csv (not a blind comma->tab replace, which would corrupt the
        # embedded TR-decimal commas inside quoted fields like "3,50").
        import io
        csv_reader = csv.reader(io.StringIO(SAMPLE_LEGACY_SUMMARY_TR_CSV))
        f_tsv = td / "legacy.tsv"
        with open(f_tsv, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh, delimiter="\t").writerows(csv_reader)
        r_tsv = run_pipeline(f_tsv, td / "out_tsv", decimal_style="TR", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r_tsv["status"] == "ok", f"TSV fixture failed: {r_tsv}"
        assert r_tsv["row_count"] == 10, f"TSV fixture row_count={r_tsv['row_count']}"
        print(f"[PASS] TSV input: {r_tsv['row_count']} rows built (previously untested code path)")

        # regression guard (audit F-23): the dict-wrapped JSON shapes
        # ({"results": [...]}, {"changeEvents": [...]}) existed in read_rows()
        # but were never exercised by any fixture until now.
        f_wrapped = td / "wrapped.json"
        f_wrapped.write_text(json.dumps({"results": SAMPLE_API_CHANGE_EVENT_JSON[:2]}), encoding="utf-8")
        # force_review=True: this 2-row slice includes one CREATE row with a
        # legitimately empty old_resource (nothing existed before a keyword
        # was added), tripping the >30%-empty needs_review gate on a sample
        # this small — same known, correct gate behavior as fixture3b above.
        r_wrapped = run_pipeline(f_wrapped, td / "out_wrapped", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_wrapped["status"] == "ok", f"wrapped-JSON fixture failed: {r_wrapped}"
        assert r_wrapped["row_count"] == 2, f"wrapped-JSON fixture row_count={r_wrapped['row_count']}"
        print(f"[PASS] dict-wrapped JSON ({{'results': [...]}}) input: {r_wrapped['row_count']} rows built (previously untested code path)")

        # --- fixture 3: genuinely unknown header format -> Layer 3 ---
        f3 = td / "unknown.csv"
        f3.write_text(SAMPLE_UNKNOWN_FORMAT_CSV, encoding="utf-8")
        r3 = run_pipeline(f3, td / "out3", generated_at="2026-08-17T00:00:00")
        assert r3["status"] == "needs_mapping", f"fixture3 should need mapping, got: {r3['status']}"
        print(f"[PASS] unknown_format: correctly stopped for column mapping (unmapped: {r3['unmapped_headers']})")

        # confirm a mapping (simulating the Claude+user chat step) and re-run
        confirmed = {
            "source_label": "test_ui_variant",
            "mapping": {
                "timestamp": "Modified On", "user_email": "Modified By Email", "account_name": "Ads Account",
                "campaign_name": "Camp Name", "ad_group_name": "AdGroup Name", "raw_summary": "What Changed",
                "old_value": "Was", "new_value": "Now", "resource_type": "What Changed",
            },
        }
        mf = td / "confirmed_mapping.json"
        mf.write_text(json.dumps(confirmed), encoding="utf-8")
        # this tiny 3-row fixture has one CREATE row with a legitimately empty
        # "old value" (nothing existed before a keyword was added) -> trips the
        # >30%-empty needs_review gate on a 3-row sample. That gate firing is
        # correct behavior (see Soru 1 in the design log) — force_review=True
        # here simulates the user confirming "yes, that sparsity is real".
        r3b = run_pipeline(f3, td / "out3b", mapping_file=mf, allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r3b["status"] == "ok", f"fixture3b failed: {r3b}"
        print(f"[PASS] unknown_format with confirmed mapping: {r3b['row_count']} rows built, profile saved for next time")

        # prove the fingerprint (Layer 1) now hits without needing --mapping-file again
        r3c = run_pipeline(f3, td / "out3c", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r3c["status"] == "ok", f"fixture3c (fingerprint reuse) failed: {r3c}"
        print(f"[PASS] unknown_format second run, no --mapping-file needed: fingerprint match reused the saved profile")

    PROFILES_DIR = real_profiles_dir
    print("\nAll self-tests passed." if ok else "\nSome self-tests FAILED.")
    return ok


# =====================================================================
# CLI
# =====================================================================
def main():
    ap = argparse.ArgumentParser(description="Ads Change History — single-file skill. Run `--help` on a subcommand for details.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Full pipeline: validate -> normalize -> categorize -> build dashboard.")
    p_run.add_argument("input")
    p_run.add_argument("--out-dir", default="./out")
    p_run.add_argument("--mapping-file")
    p_run.add_argument("--date-format", choices=["DMY", "MDY", "ISO"])
    p_run.add_argument("--decimal-style", choices=["TR", "US"])
    p_run.add_argument("--timezone", default="unknown")
    p_run.add_argument("--mask-users", action="store_true")
    p_run.add_argument("--allow-unknown-categories", action="store_true")
    p_run.add_argument("--force-review", action="store_true")
    p_run.add_argument("--extra-rules", help="JSON file: list of extra structured_rules entries to apply on top of CATEGORY_RULES.")
    p_run.add_argument("--open", action="store_true")

    p_query = sub.add_parser("query", help="Deterministic filter over a changes.jsonl.")
    p_query.add_argument("input")
    p_query.add_argument("--user")
    p_query.add_argument("--account")
    p_query.add_argument("--campaign")
    p_query.add_argument("--category")
    p_query.add_argument("--operation")
    p_query.add_argument("--source")
    p_query.add_argument("--since", help="e.g. 7d, 2w, 1m")
    p_query.add_argument("--format", choices=["table", "json"], default="table")

    sub.add_parser("self-test", help="Run the built-in fixtures end-to-end and assert expected behavior.")

    args = ap.parse_args()

    if args.cmd == "self-test":
        ok = self_test()
        sys.exit(0 if ok else 1)

    if args.cmd == "run":
        extra_rules = None
        if args.extra_rules:
            with open(args.extra_rules, encoding="utf-8") as f:
                extra_rules = json.load(f)
        result = run_pipeline(
            args.input, args.out_dir, mapping_file=args.mapping_file, date_format=args.date_format,
            decimal_style=args.decimal_style, tz=args.timezone, mask_users=args.mask_users,
            allow_unknown_categories=args.allow_unknown_categories, force_review=args.force_review,
            extra_rules=extra_rules, open_browser=args.open,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        sys.exit(0 if result.get("status") == "ok" else 2)

    if args.cmd == "query":
        rows = []
        with open(args.input, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        matched = query_changes(rows, user=args.user, account=args.account, campaign=args.campaign,
                                 category=args.category, operation=args.operation, source=args.source, since=args.since)
        if args.format == "json":
            print(json.dumps({"count": len(matched), "rows": matched}, indent=2, ensure_ascii=False))
        else:
            print(f"{len(matched)} matching change(s)\n")
            for r in matched:
                actor = r.get("user_name") or r.get("user_email") or (f"[{r.get('client_type')}]" if r.get("actor_type") == "automation" else "unknown")
                print(f"{r.get('timestamp')}  {actor:<20}  {r.get('account_name') or '':<20}  {r.get('campaign_name') or '—':<25}  {r.get('category')}/{r.get('subcategory')}  {r.get('old_value')} -> {r.get('new_value')}")


if __name__ == "__main__":
    main()
