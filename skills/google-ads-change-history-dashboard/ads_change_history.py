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
  A source event that changed several fields (Google's FieldMask is a
  repeated field) becomes one row PER FIELD whenever its old/new values are
  the resource snapshot itself (a JSON object, or its JSON string) — each
  row gets that field's own before/after, all sharing one source_event_id
  (expand_multi_field_rows()). When old/new are flat strings there is
  nothing to split by: the event stays one row, field_name a comma-joined
  string, old_value/new_value the WHOLE change, and multi_field is true.
  campaign_name/ad_group_name are dropped (and counted) on a row whose own
  campaign/ad-group reference column is empty — a name can't attach a row to
  an entity the source says it doesn't belong to. resource_name is currently
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
    No currency is invented — no source says which one. --currency TRY states
    it for the whole file: budget/bid/target-CPA rows then carry it in
    `currency` and show it as their unit (ratios and multipliers don't).
  - A value is a number only when ALL of it is one number, optionally with a
    currency marker or short unit ("₺150,00", "150.000 TL", "%20", "5km").
    Text that merely contains a digit ("Search | Generic | v1") is text.
  - Dates: DD/MM vs MM/DD is ambiguous unless some value's day exceeds 12.
    Ambiguous columns require --date-format DMY|MDY|ISO — never guessed.
    Every row is examined, not a sample. A year-first (ISO) row parses under
    DMY/MDY too, so a file mixing both shapes loses neither.
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
  --mask-users defaults OFF (raw user identity shown). With it, every
  output file loses its e-mail addresses: a human actor becomes "User A"/
  "User B" (stable across runs — the identity->label table is kept locally
  in mapping-profiles/mask-labels.json, which is gitignored and must never
  be shared), and an address sitting on an automation row is removed.
  NOT masked, with or without the flag: account, campaign and ad group
  names, account/campaign/ad-group IDs, resource paths, event IDs, and the
  old/new values themselves. Showing those is the point of the tool; a
  report that must hide the client's identity needs more than this flag.

USAGE
  python3 ads_change_history.py run <input.csv|.json> --out-dir ./out \\
      [--timezone Europe/Istanbul] [--date-format DMY] [--decimal-style TR] \\
      [--currency TRY] \\
      [--mapping-file confirmed_mapping.json | --map FIELD="Header" ...] \\
      [--mask-users] [--open]

  python3 ads_change_history.py query <out/changes.jsonl> --user "User A" --since 7d

  python3 ads_change_history.py self-test
      Writes the built-in synthetic fixtures to a temp dir, runs the full
      pipeline on each, asserts expected shape. This is the test suite —
      no external test framework needed. A failing case is reported and
      the run continues, so one run lists every failure; exit code 1 if
      any case failed.
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
        # Fixed 2026-10-10 (6th audit round): account_name/campaign_name/
        # ad_group_name were each missing their own literal snake_case name —
        # the same gap "user_name" had below. A ChangeEvent export joined
        # with display names (customer_id + account_name, campaign +
        # campaign_name, ...) mapped the ID/path columns and silently dropped
        # all three name columns, so every row rendered as "Campaign <id>".
        "account_name": ["account_name", "Hesap Adı", "Account", "Account name", "Client", "Hesap"],
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
        "campaign_name": ["campaign_name", "Campaign", "Kampanya", "campaign.name"],
        "ad_group_id": ["Ad group ID", "Reklam Grubu ID", "ad_group.id"],
        "ad_group_name": ["ad_group_name", "Ad group", "Reklam Grubu", "ad_group.name"],
        "operation": ["resource_change_operation", "Change", "Değişiklik", "Operation"],
        "field_name": ["changed_fields", "Field", "Alan", "Değişen Alan"],
        "old_value": ["old_resource", "Eski Teknik Veri", "Before", "Old value", "Önceki", "Old"],
        "new_value": ["new_resource", "Yeni Teknik Veri", "After", "New value", "Yeni", "New"],
        # "Changes" added 2026-08-18 — the real Google Ads UI's own
        # "Change history report" CSV export (Campaigns > Change history >
        # Download) uses this exact column name, verified against a real
        # export this session. It's a multi-line free-text cell that can
        # bundle several distinct changes into one row (see the module
        # docstring's note on this format's limits).
        "raw_summary": ["Değişiklik Özeti (Nereden -> Nereye)", "Değişiklik Özeti", "Change summary", "Summary", "Description", "Changes"],
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
        "AdGroup": ["Ad group created", "Ad group name changed", "Ad group settings changed", "Ad group removed"],
        "Conversion": ["Conversion action created", "Conversion action changed", "Conversion tracking changed"],
        "Feed": ["Feed created", "Feed changed", "Feed removed", "Feed item added", "Feed item changed", "Feed item removed", "Feed attached to campaign", "Feed attached to ad group"],
        # Added 2026-08-18, found via a second real native UI export — none
        # of Google's own real ChangeEventResourceType values map to
        # account/customer-administration events (identity verification,
        # security settings, user access, manager-account links,
        # notification preferences), so there was nowhere for these to go
        # but "Other" even though they're common, real, and clearly not
        # ad-campaign activity. Distinct from "Campaign", which is
        # specifically campaign-scoped.
        "Account": ["Customer identity changed", "Security settings changed", "Access changed", "Manager account changed", "Notification settings changed", "Account settings changed"],
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
        # Fixed 2026-10-10: this rule was written with "field_name_contains"
        # twice in one dict literal ("keyword", then "negative") — Python
        # keeps only the last, so it matched ANY negative ad-group criterion
        # (a negative placement, a negative audience) as a negative keyword.
        # A list means every entry must be present.
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": ["keyword", "negative"], "operation": "CREATE", "category": "Keyword", "subcategory": "Negative keyword"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "keyword", "operation": "REMOVE", "category": "Keyword", "subcategory": "Keyword removed"},
        {"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "negative", "category": "Keyword", "subcategory": "Negative keyword"},
        {"resource_type_in": ["AD", "AD_GROUP_AD"], "operation": "CREATE", "category": "Ad", "subcategory": "Ad created"},
        # Fixed 2026-10-10 (6th audit round): these two sat with the other
        # status rules further down, after the generic AD UPDATE rule below —
        # first-match-wins, so pausing/enabling an ad was always "Ad changed"
        # and never reached Status, unlike the same change on a campaign, ad
        # group or keyword. After CREATE on purpose: a new ad stays "Ad created".
        {"resource_type": "AD_GROUP_AD", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "AD_GROUP_AD", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
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
        # Moved above the status rules 2026-10-10 (same first-match-wins trap
        # as the AD_GROUP_AD rules above): a campaign or ad group CREATED with
        # "status" among its changed fields was reported as Status/Enabled —
        # a state change of something that didn't exist a moment earlier.
        {"resource_type": "CAMPAIGN", "operation": "CREATE", "category": "Campaign", "subcategory": "Campaign created"},
        {"resource_type": "AD_GROUP", "operation": "CREATE", "category": "AdGroup", "subcategory": "Ad group created"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        {"resource_type": "AD_GROUP", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "AD_GROUP", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "status", "new_value_equals": "ENABLED", "category": "Status", "subcategory": "Enabled"},
        {"resource_type": "AD_GROUP_CRITERION", "field_name_contains": "status", "new_value_equals": "PAUSED", "category": "Status", "subcategory": "Paused"},
        # These two must come BEFORE the generic REMOVE catch-all below —
        # first-match-wins, and the catch-all previously intercepted every
        # CAMPAIGN/AD_GROUP removal into Status/Removed, making the
        # "Campaign removed"/"Ad group removed" categories declared in
        # CATEGORY_RULES["categories"] unreachable for structured data.
        # Fixed 2026-08-19 (2nd audit).
        {"resource_type": "CAMPAIGN", "operation": "REMOVE", "category": "Campaign", "subcategory": "Campaign removed"},
        {"resource_type": "AD_GROUP", "operation": "REMOVE", "category": "AdGroup", "subcategory": "Ad group removed"},
        # Same shadowing, found 2026-10-10 (6th audit round): FEED/FEED_ITEM
        # removals were declared with the other feed rules below the
        # catch-all and never reached.
        {"resource_type": "FEED", "operation": "REMOVE", "category": "Feed", "subcategory": "Feed removed"},
        {"resource_type": "FEED_ITEM", "operation": "REMOVE", "category": "Feed", "subcategory": "Feed item removed"},
        {"operation": "REMOVE", "category": "Status", "subcategory": "Removed"},
        {"resource_type": "CAMPAIGN", "field_name_contains": "name", "operation": "UPDATE", "category": "Campaign", "subcategory": "Campaign name changed"},
        {"resource_type": "CAMPAIGN", "operation": "UPDATE", "category": "Campaign", "subcategory": "Campaign settings changed"},
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
        {"resource_type": "FEED", "operation": "UPDATE", "category": "Feed", "subcategory": "Feed changed"},
        {"resource_type": "FEED_ITEM", "operation": "CREATE", "category": "Feed", "subcategory": "Feed item added"},
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
        # Added 2026-08-18, found via a real native Google Ads UI export
        # (Campaigns > Change history > Download) — that CSV has no
        # field_name/resource_type column at all, only a free-text
        # "Changes" cell whose first line is a headline like "Campaign
        # changed" or "12 budget amount decreased". These patterns are
        # taken from the actual first-line phrasings observed across 210
        # real rows in that export (generalized — no product-specific text),
        # covering ~94% of them; the remainder (rare account-admin events
        # like "Customer manager changed", "Access level for user changed")
        # are deliberately left uncovered rather than force-fit into an
        # advertising category they don't belong to — they fall to Other,
        # correctly.
        {"pattern": r"^\d+ campaigns? paused", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^\d+ campaigns? active", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^\d+ ad groups? paused", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^\d+ ad groups? active", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^Campaign created", "category": "Campaign", "subcategory": "Campaign created"},
        {"pattern": r"^Campaign removed", "category": "Campaign", "subcategory": "Campaign removed"},
        {"pattern": r"^Campaign changed", "category": "Campaign", "subcategory": "Campaign settings changed"},
        {"pattern": r"^Ad group created", "category": "AdGroup", "subcategory": "Ad group created"},
        {"pattern": r"^Ad group removed", "category": "AdGroup", "subcategory": "Ad group removed"},
        {"pattern": r"^Ad group changed", "category": "AdGroup", "subcategory": "Ad group settings changed"},
        {"pattern": r"^Budget created", "category": "Budget", "subcategory": "Budget created"},
        {"pattern": r"^Budget removed", "category": "Budget", "subcategory": "Budget removed"},
        {"pattern": r"^(Budget changed|Budget set to|\d+ budget amount (increased|decreased))", "category": "Budget", "subcategory": "Budget changed"},
        {"pattern": r"^\d+ (universal app (engagement )?ad|responsive search ad)s? created", "category": "Ad", "subcategory": "Ad created"},
        {"pattern": r"^\d+ (universal app (engagement )?ad|responsive search ad)s? removed", "category": "Ad", "subcategory": "Ad removed"},
        {"pattern": r"^\d+ (universal app (engagement )?ad|responsive search ad)s? changed", "category": "Ad", "subcategory": "Ad changed"},
        # Fixed 2026-08-18 (second real export): the match-type word varies
        # (broad/phrase/exact) — only "phrase" was covered before, missing
        # every broad-match keyword event, the more common match type in
        # practice.
        {"pattern": r"^\d+ negative (broad|phrase|exact) match keywords? added", "category": "Keyword", "subcategory": "Negative keyword"},
        {"pattern": r"^\d+ (broad|phrase|exact) match keywords? added", "category": "Keyword", "subcategory": "Keyword added"},
        {"pattern": r"^\d+ (broad|phrase|exact) match keywords? removed", "category": "Keyword", "subcategory": "Keyword removed"},
        {"pattern": r"^\d+ (broad|phrase|exact) match keywords? paused", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^\d+ data segments? added", "category": "Audience", "subcategory": "Audience added"},
        {"pattern": r"^\d+ languages? (added|removed)", "category": "Targeting", "subcategory": "Language targeting changed"},
        {"pattern": r"^\d+ (countr(y|ies)|regions?) (added|removed)", "category": "Targeting", "subcategory": "Location targeting changed"},
        {"pattern": r"^(Conversion|Standard goals changed|Account-default goals)", "category": "Conversion", "subcategory": "Conversion tracking changed"},
        # Account/customer-administration events — see the "Account" entry
        # in CATEGORY_RULES["categories"] above for why these have nowhere
        # else to go. Each pattern's literal continuation after "Customer "
        # is distinct (identity/security/manager/created/changed/asset),
        # so match order between them doesn't matter.
        {"pattern": r"^(Customer asset|Campaign asset|Asset) created", "category": "Asset", "subcategory": "Asset added"},
        {"pattern": r"^Customer identity (created|changed)", "category": "Account", "subcategory": "Customer identity changed"},
        {"pattern": r"^Customer security settings changed", "category": "Account", "subcategory": "Security settings changed"},
        {"pattern": r"^Customer manager (created|changed)", "category": "Account", "subcategory": "Manager account changed"},
        {"pattern": r"^Customer (created|changed)", "category": "Account", "subcategory": "Account settings changed"},
        {"pattern": r"^(Access level for user|Access is activated|Sent invitation to access account)", "category": "Account", "subcategory": "Access changed"},
        {"pattern": r"^Updated .+'s notification setting", "category": "Account", "subcategory": "Notification settings changed"},
        {"pattern": r"^Location sync (created|changed)", "category": "Account", "subcategory": "Account settings changed"},
        # Added 2026-08-19, found via a real native Google Ads UI export in
        # TURKISH (Kampanyalar > Değişiklik geçmişi > İndir): same shape as
        # the English block above (no field_name/resource_type column, only
        # a free-text "Değişiklik" cell whose first line is a headline), but
        # the UI's own locale phrasing is different from both this block's
        # English patterns and the "legacy_summary_tr" block above (which was
        # a prior project's spreadsheet convention, not this native export's
        # wording). Patterns taken from the actual first-line phrasings
        # observed across ~98 real rows in that export, generalized — no
        # account/campaign/keyword-list-specific text. The negative-
        # keyword-list rule intentionally has no ^ anchor since the source
        # prefixes it with a variable, quoted list name.
        {"pattern": r"^Kampanya değiştirildi", "category": "Campaign", "subcategory": "Campaign settings changed"},
        {"pattern": r"^Kampanya oluşturuldu", "category": "Campaign", "subcategory": "Campaign created"},
        {"pattern": r"^Kampanya kaldırıldı", "category": "Campaign", "subcategory": "Campaign removed"},
        {"pattern": r"^Kampanya öğesi oluşturuldu", "category": "Asset", "subcategory": "Asset added"},
        {"pattern": r"^Kampanya öğesi değiştirildi", "category": "Asset", "subcategory": "Asset changed"},
        {"pattern": r"^Kampanya öğesi kaldırıldı", "category": "Asset", "subcategory": "Asset removed"},
        {"pattern": r"^\d+ kampanya etkin", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^\d+ kampanya duraklat", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^Bütçe oluşturuldu", "category": "Budget", "subcategory": "Budget created"},
        {"pattern": r"^Bütçe kaldırıldı", "category": "Budget", "subcategory": "Budget removed"},
        {"pattern": r"^\d+ bütçenin tutarı (azaltıldı|artırıldı)", "category": "Budget", "subcategory": "Budget changed"},
        {"pattern": r"^\d+ teklif ayarlama değişikliği", "category": "Bidding", "subcategory": "Bid changed"},
        {"pattern": r"^Reklam grubu oluşturuldu", "category": "AdGroup", "subcategory": "Ad group created"},
        {"pattern": r"^Reklam grubu değiştirildi", "category": "AdGroup", "subcategory": "Ad group settings changed"},
        {"pattern": r"^Reklam grubu kaldırıldı", "category": "AdGroup", "subcategory": "Ad group removed"},
        {"pattern": r"^\d+ duyarlı arama ağı reklamı oluşturuldu", "category": "Ad", "subcategory": "Ad created"},
        {"pattern": r"^\d+ duyarlı arama ağı reklamı değiştirildi", "category": "Ad", "subcategory": "Ad changed"},
        {"pattern": r"^\d+ duyarlı arama ağı reklamı kaldırıldı", "category": "Ad", "subcategory": "Ad removed"},
        {"pattern": r"^\d+ [Dd]uyarlı görüntülü reklam oluşturuldu", "category": "Ad", "subcategory": "Ad created"},
        {"pattern": r"^\d+ [Dd]uyarlı görüntülü reklam değiştirildi", "category": "Ad", "subcategory": "Ad changed"},
        {"pattern": r"^\d+ [Dd]uyarlı görüntülü reklam kaldırıldı", "category": "Ad", "subcategory": "Ad removed"},
        {"pattern": r"^Öğe oluşturuldu", "category": "Ad", "subcategory": "Ad created"},
        {"pattern": r"negatif (anahtar kelime listesi uygulandı|sıralı eşlemeli anahtar kelime(ler)? (eklendi|listeye eklendi))", "category": "Keyword", "subcategory": "Negative keyword"},
        {"pattern": r"^\d+ (geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? eklendi", "category": "Keyword", "subcategory": "Keyword added"},
        {"pattern": r"^\d+ (geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? duraklatıldı", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^\d+ dil (eklendi|kaldırıldı)", "category": "Targeting", "subcategory": "Language targeting changed"},
        {"pattern": r"^\d+ (ülke(ler)?|bölge(ler)?) (eklendi|kaldırıldı)", "category": "Targeting", "subcategory": "Location targeting changed"},
        {"pattern": r"^Dönüşüm hedefi oluşturuldu", "category": "Conversion", "subcategory": "Conversion action created"},
        {"pattern": r"^Dönüşüm hedefi değiştirildi", "category": "Conversion", "subcategory": "Conversion action changed"},
        # Second pass, 2026-08-19: 4 more real phrasings found in the same
        # 98-row export after the first pass above. "yaş grubu" (age group)
        # rows are demographic/audience exclusions, not location — Audience,
        # not Targeting. The "adlı kampanya özel hedefi eklendi" (named
        # custom-intent audience added) and the age-group rules have no fixed
        # prefix (a quoted name or a leading count precedes them), so these
        # use re.search without a ^ anchor, same reasoning as the negative-
        # keyword-list rule above.
        {"pattern": r"negatif yaş grubu (eklendi|değiştirildi|kaldırıldı)", "category": "Audience", "subcategory": "Audience added"},
        {"pattern": r"adlı kampanya özel hedefi (eklendi|değiştirildi|kaldırıldı)", "category": "Audience", "subcategory": "Audience added"},
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
# Third fallback, Turkish native UI export phrasing: "OLD olan FIELD, NEW
# olarak değiştirildi" (budget) / "OLD olan FIELD, NEW değerine artırıldı/
# azaltıldı" (target CPA/ROAS, bids) — verified against a real 98-row export.
# Currency symbol (₺) stripped along with the digit-group leader, same as the
# other two patterns leave to normalize_number().
SUMMARY_TR_OLAN_DEGERINE_RE = re.compile(
    r"₺?(-?[\d.,]+)\s+olan\s+[^,\n]+,\s*₺?(-?[\d.,]+)\s+(?:olarak değiştirildi|değerine (?:artırıldı|azaltıldı))"
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
SINCE_RE = re.compile(r"^\d+[dwm]$")


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
            profile = json.load(f)
        # Fixed 2026-10-10 (6th audit round): every "ok" run used to save a
        # profile, including one derived purely from the alias table. That
        # froze the table's state at the time — a later alias fix never
        # reached a file that had been seen once, because this lookup wins
        # before try_alias_match() runs. Only a human-confirmed mapping is
        # worth caching (re-deriving an alias match costs nothing); profiles
        # left behind by the old behavior are recognizable by their
        # auto-assigned source_label and skipped rather than trusted.
        auto_derived = profile.get("source_label") in ({"alias_match"} | set(HEADER_ALIASES["known_sources"]))
        if profile.get("confirmed_by") or not auto_derived:
            return profile
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


def check_hard_required(mapping, default_account_name=None):
    """Fixed 2026-08-18 (found via a real native Google Ads UI export —
    Campaigns > Change history > Download): that export has no account
    column at all — you're already scoped into one account when you export
    it, so Google doesn't repeat it per row. That's a real, common shape,
    not a malformed file — --account-name lets the caller supply the one
    account this whole file belongs to as an explicit, human-confirmed
    constant (never guessed silently: the flag has to be passed)."""
    missing = []
    if "timestamp" not in mapping:
        missing.append("timestamp")
    if "account_name" not in mapping and "account_id" not in mapping and not default_account_name:
        missing.append("account_ref (account_name or account_id, or pass --account-name if the source has none)")
    has_structured = "old_value" in mapping and "new_value" in mapping
    has_summary = "raw_summary" in mapping
    if not has_structured and not has_summary:
        missing.append("change_info ((old_value+new_value) or raw_summary)")
    # Fixed 2026-08-18 (found via the same real UI export): resource_type is
    # only ever CONSUMED by match_structured(), which categorize_changes()
    # only calls when a row has field_name — every text-only row (no
    # field_name) already routes through match_summary() instead, which
    # reads raw_summary and never touches resource_type. Requiring it
    # unconditionally blocked a genuinely text-only source (raw_summary
    # mapped, no field_name/resource_type column at all — the Google Ads
    # UI's own native export is exactly this shape) from something its own
    # categorization path doesn't need. Still required whenever raw_summary
    # ISN'T mapped either — with neither, no row can be categorized as
    # anything but "Other" regardless, so failing hard stays correct there
    # (same "don't produce known-useless output" reasoning as the review
    # gates elsewhere in this function).
    if "resource_type" not in mapping and not has_summary:
        missing.append("resource_type")
    return missing


def soft_warnings(mapping):
    warnings = []
    if "user_name" not in mapping and "user_email" not in mapping:
        warnings.append("No user identity column found — actor_type will default to 'unknown' unless client_type infers automation.")
    if "operation" not in mapping:
        warnings.append("No structured 'operation' column — inferred from raw_summary text, else defaults to UPDATE.")
    # campaign_resource counts too (6th audit round, 2026-10-10): campaign_id
    # is derived from its path tail, so campaign-level views are populated.
    if not any(c in mapping for c in ("campaign_name", "campaign_id", "campaign_resource")):
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


def parse_column_map(entries):
    """["timestamp=Modified On", ...] (the --map flag's values) -> dict.
    Raises ValueError on an entry that isn't canonical_field=Column header."""
    out = {}
    for entry in entries or []:
        field, sep, header = str(entry).partition("=")
        if not sep or not field.strip() or not header.strip():
            raise ValueError(f"Invalid --map value: {entry!r}. Expected canonical_field=Column header, e.g. --map timestamp=\"Modified On\".")
        out[field.strip()] = header.strip()
    return out


def validate_source(input_path, mapping_file=None, force=False, save_profile=True, default_account_name=None, column_map=None):
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
        # Fixed 2026-08-18 (4th audit round, confirmed): read_rows() turns
        # every malformed-INPUT case into a structured error, but this file
        # — a config the caller supplies, not the data being analyzed — had
        # no equivalent: a missing --mapping-file or invalid JSON in it
        # crashed with a raw FileNotFoundError/JSONDecodeError instead of the
        # same structured status every other failure mode in this function
        # already uses.
        try:
            with open(mapping_file, encoding="utf-8") as f:
                confirmed = json.load(f)
        except FileNotFoundError:
            return {"status": "error", "message": f"Mapping file not found: {mapping_file}"}
        except json.JSONDecodeError as e:
            return {"status": "error", "message": f"Mapping file is not valid JSON: {mapping_file} ({e})"}
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

    # Added 2026-10-10: answering needs_mapping used to mean hand-writing a
    # {"source_label": ..., "mapping": {...}} JSON file. --map field=Header
    # gives the same answers on the command line, laid over whatever was
    # auto-mapped. It is a human-confirmed mapping exactly like
    # --mapping-file (and is cached the same way) — so it gets the same
    # strictness: a field name this script doesn't have, or a header the
    # file doesn't have, is an error, never a silent no-op.
    if column_map:
        mappable = set(HEADER_ALIASES["aliases"])
        bad_fields = sorted(set(column_map) - mappable)
        if bad_fields:
            return {"status": "error", "message": f"--map: unknown canonical field(s) {bad_fields}. Mappable fields: {sorted(mappable)}"}
        bad_headers = sorted(h for h in column_map.values() if h not in headers)
        if bad_headers:
            return {"status": "error", "message": f"--map: column header(s) {bad_headers} are not in this file. Its headers: {headers}"}
        mapping = {**mapping, **column_map}
        unmatched = [h for h in unmatched if h not in column_map.values()]
        if source_label in ({"alias_match", "unmapped_confirmed"} | set(HEADER_ALIASES["known_sources"])):
            source_label = "user_confirmed"

    missing = check_hard_required(mapping, default_account_name=default_account_name)
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
            "instructions": "Show unmapped_headers + sample_values to the user in chat. Ask which canonical field each maps to (see module docstring: CANONICAL SCHEMA). Re-run with one --map canonical_field=\"Column header\" per answer (or put them in a JSON file, {'source_label': ..., 'mapping': {canonical: header}}, and pass --mapping-file <file>). If the source genuinely has no column for a missing field (e.g. no resource_type at all), that field cannot be satisfied by mapping alone — this input can't proceed until the data itself provides it.",
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
            "message": "More than 30% of rows are missing a value in a required field even though the column exists — usually a wrong mapping, not genuinely sparse data. Confirm with the user; re-run with --force-review if the data really is this sparse.",
            "issues": review_issues,
            "mapping": mapping,
        }

    if save_profile and (mapping_file or column_map):
        PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        fp = fingerprint(headers)
        with open(PROFILES_DIR / f"{fp}.json", "w", encoding="utf-8") as f:
            json.dump({"source_label": source_label, "confirmed_by": "mapping_file" if mapping_file else "map_flag", "mapping": mapping, "headers_seen": headers}, f, indent=2, ensure_ascii=False)

    return {
        "status": "ok", "input": str(input_path), "row_count": len(rows),
        "fingerprint": fingerprint(headers), "source_label": source_label,
        "mapping": mapping,
        # Fixed 2026-10-10 (6th audit round): unmapped_headers was only ever
        # reported on the needs_mapping path. On an "ok" run a source column
        # no alias matched just vanished — which is how three fully-populated
        # name columns went missing with nothing anywhere saying so.
        "ignored_headers": [h for h in headers if h not in set(mapping.values())],
        "warnings": soft_warnings(mapping) + ([f"Duplicate column header(s) detected: {dup_headers} — only the last occurrence of each is used (stdlib csv.DictReader behavior), earlier columns of the same name are silently discarded."] if dup_headers else []),
    }


# =====================================================================
# STAGE 3: NORMALIZATION
# =====================================================================
# Fixed 2026-10-10 (6th audit round): normalize_number() used to delete every
# non-numeric character and parse whatever digits were left, so any text with
# a digit in it became a number — a campaign named "Search | Generic | v1"
# got old_value_num 1.0, "feed/100" got 100.0, a resource path got its IDs
# concatenated. Those values feed table sorting and old/new_value_display. A
# value is numeric only when the WHOLE of it is one number, optionally
# wrapped in a currency marker and/or a short unit ("₺150,00", "150.000 TL",
# "%20", "TRY100.00/day", "5km"). A letter directly before the digits
# ("v1", "Q3") is an identifier, not a quantity, so no free-form prefix.
NUMERIC_VALUE_RE = re.compile(
    r"^([+-])?\s*(?:[₺$€£¥%]|TRY|TL|USD|EUR|GBP)?\s*(-?\d[\d.,]*)\s*(?:%|[^\W\d_]{1,4})?(?:/[^\W\d_]{1,5})?$"
)


def _numeric_core(raw):
    """The bare digits/separators/sign of a numeric-looking value, or None
    when the value is text that merely contains digits."""
    if raw is None:
        return None
    m = NUMERIC_VALUE_RE.match(str(raw).strip())
    if not m:
        return None
    return ("-" if m.group(1) == "-" else "") + m.group(2)


def normalize_number(raw, decimal_style):
    s = _numeric_core(raw)
    if s is None:
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
        s = _numeric_core(v)
        if s is None:
            continue
        has_dot, has_comma = "." in s, "," in s
        if has_dot and has_comma:
            continue
        if (has_dot or has_comma) and DIGIT_GROUP_3.match(s) and not DIGIT_GROUP_SHORT.match(s):
            return True
    return False


GOOGLE_UI_EN_DATETIME_RE = re.compile(r"^[A-Za-z]{3} \d{1,2}, \d{4}, \d{1,2}:\d{2}:\d{2} (AM|PM)$")
# U+202F (narrow no-break space) and U+00A0 (no-break space) — confirmed
# present in a real Google Ads UI export, sitting between the time and
# AM/PM ("10:42:26 PM") instead of a plain ASCII space. Neither a
# regex expecting " " nor strptime's %p match across these, so both get
# normalized to a plain space before parsing. General enough (locale-
# specific space variants are a known export quirk, not unique to this one
# format) that it's applied unconditionally, not just for UI_EN.
_NARROW_SPACES = (" ", " ")


def _normalize_spaces(s):
    for ch in _NARROW_SPACES:
        s = s.replace(ch, " ")
    return s


YEAR_FIRST_DATE_RE = re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}")
SLASH_DATE_RE = re.compile(r"^(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})")


def detect_date_format(values):
    """ISO / UI_EN / DMY / MDY when the column itself settles it, else None
    (stop and ask).

    Fixed 2026-10-10 (follow-up to the 6th audit round), two gaps:
    (1) only the first 200 values were sampled. A file whose slash dates
        start at row 201 resolved as "ISO" without asking, and every slash
        row was then skipped as unparseable.
    (2) ISO/UI_EN required EVERY value to match, so a single unreadable cell
        ("N/A") in an otherwise clean ISO column fell through to the "DD/MM
        vs MM/DD?" question — about a column with no slash date in it. Only
        slash-shaped values raise that question now; a stray unreadable
        value is left for parse_timestamp() to count as skipped
        (run_pipeline still stops when that passes 20%).

    UI_EN is the Google Ads UI's own "Change history report" CSV shape
    ("Aug 16, 2026, 10:42:26 PM", added 2026-08-18 from a real export): a
    spelled-out month has no day/month order to be ambiguous about, so —
    like ISO — it auto-detects rather than asking."""
    sample = [_normalize_spaces(str(v)) for v in values if v]
    if not sample:
        return "ISO"
    slash = [m for m in (SLASH_DATE_RE.match(s) for s in sample) if m]
    if not slash:
        iso = sum(1 for s in sample if YEAR_FIRST_DATE_RE.match(s))
        ui_en = sum(1 for s in sample if GOOGLE_UI_EN_DATETIME_RE.match(s))
        if not (iso or ui_en):
            return None
        return "ISO" if iso >= ui_en else "UI_EN"
    for m in slash:
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
    s = _normalize_spaces(str(raw).strip())
    # Fixed 2026-10-10 (6th audit round): --date-format DMY/MDY only answers
    # "which of the first two numbers is the day" — a year-first string has
    # no such question. A file mixing ISO rows with slash-date rows used to
    # be a dead end: ISO dropped the slash rows, DMY/MDY dropped every ISO
    # row, and no flag value kept both.
    if fmt == "ISO" or (fmt in ("DMY", "MDY") and YEAR_FIRST_DATE_RE.match(s)):
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
    if fmt == "UI_EN":
        try:
            return datetime.strptime(s, "%b %d, %Y, %I:%M:%S %p")
        except ValueError:
            raise ValueError(f"unparseable UI_EN timestamp: {raw}")
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


_MISSING = object()


def _resource_snapshot(value):
    """A changed-resource snapshot as a dict (given as a dict or as its JSON
    string), else None — a flat value like "ENABLED" is not a snapshot."""
    if isinstance(value, str):
        text = value.strip()
        if not text.startswith("{"):
            return None
        try:
            value = json.loads(text)
        except ValueError:
            return None
    return value if isinstance(value, dict) else None


def _walk_field_path(node, path):
    """The value at a FieldMask path ("campaign_budget.amount_micros"), or
    _MISSING. Tries snake_case and camelCase at each step (which one a
    serialized protobuf uses depends on the client), and looks one level
    down as well: ChangedResource is a oneof, so the real resource usually
    sits under a single key named after its type ({"campaign": {...}})."""
    def walk(start):
        cur = start
        for part in path.split("."):
            if not isinstance(cur, dict):
                return _MISSING
            head, *rest = part.split("_")
            camel = head + "".join(w.capitalize() for w in rest)
            if part in cur:
                cur = cur[part]
            elif camel in cur:
                cur = cur[camel]
            else:
                return _MISSING
        return cur
    found = walk(node)
    if found is _MISSING and len(node) == 1 and isinstance(next(iter(node.values())), dict):
        found = walk(next(iter(node.values())))
    return found


def expand_multi_field_rows(rows, mapping):
    """Added 2026-10-10. One ChangeEvent that changed several fields used to
    stay one row, its field_name the paths joined ("status, name") and its
    old/new value the whole serialized resource — nothing downstream could
    categorize, sort or threshold it per field. That was deliberate while
    the per-field values weren't available: splitting would have stamped the
    same whole-resource blob onto every field (see audit F-02).

    They ARE available whenever old/new arrive as the resource snapshot
    itself (tools/fetch_live_data.py emits exactly that for a multi-field
    event). Such a row becomes one row per changed field, each with that
    field's own old/new value, all sharing the event's source_event_id. A
    row is split only if EVERY path resolves in at least one of the two
    snapshots (a CREATE has no "old") — otherwise it is left exactly as it
    was, joined and flagged multi_field, rather than half-split on a guess."""
    fh, oh, nh = mapping.get("field_name"), mapping.get("old_value"), mapping.get("new_value")
    if not (fh and oh and nh):
        return rows
    out = []
    for r in rows:
        paths = r.get(fh)
        if isinstance(paths, str) and "," in paths:
            paths = [x.strip() for x in paths.split(",") if x.strip()]
        old_node, new_node = _resource_snapshot(r.get(oh)), _resource_snapshot(r.get(nh))
        if not isinstance(paths, list) or len(paths) < 2 or (old_node is None and new_node is None):
            out.append(r)
            continue
        pairs = [tuple(_MISSING if node is None else _walk_field_path(node, str(path)) for node in (old_node, new_node)) for path in paths]
        if any(o is _MISSING and n is _MISSING for o, n in pairs):
            out.append(r)
            continue
        text = lambda v: "" if v is _MISSING or v is None else v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
        out.extend({**r, fh: str(path), oh: text(o), nh: text(n)} for path, (o, n) in zip(paths, pairs))
    return out


def derive_resource_id_from_path(resource_path):
    """'customers/1/campaignBudgets/456' -> '456'. Best-effort only — used
    when no separate bare-ID column exists."""
    if not resource_path or "/" not in str(resource_path):
        return None
    return str(resource_path).rsplit("/", 1)[-1]


def normalize_changes(input_path, mapping, source_label, date_format=None, decimal_style=None, tz="unknown", default_account_name=None):
    """Returns dict: status 'ok' with 'rows' (list of canonical dicts), or a
    needs_date_format / needs_decimal_style status to stop and ask."""
    try:
        headers, rows = read_rows(input_path)
    except ValueError as e:
        # Defense in depth for callers that invoke this directly without
        # going through validate_source() first — see F-16/F-17/F-18 fix note there.
        return {"status": "error", "message": str(e)}

    source_row_count = len(rows)
    rows = expand_multi_field_rows(rows, mapping)
    ts_header = mapping.get("timestamp")
    ts_values = [r.get(ts_header) for r in rows] if ts_header else []
    resolved_date_fmt = date_format or detect_date_format(ts_values)
    if resolved_date_fmt is None:
        # Fixed 2026-10-10 (6th audit round): sample_values was the first 5
        # timestamps, whatever they were. In a mostly-ISO file with a handful
        # of slash dates, the human was asked "DD/MM or MM/DD?" while being
        # shown five ISO strings that have no such ambiguity. Same for the
        # decimal-style question below.
        ambiguous = [v for v in ts_values if v and not YEAR_FIRST_DATE_RE.match(_normalize_spaces(str(v)))]
        return {"status": "needs_date_format", "message": "Timestamp column is ambiguous (DD/MM vs MM/DD). Ask the user, then re-run with --date-format DMY|MDY|ISO.", "sample_values": (ambiguous or [v for v in ts_values if v])[:5]}

    numeric_cols = [mapping[c] for c in ("old_value", "new_value") if c in mapping]
    numeric_values = [r.get(h) for h in numeric_cols for r in rows]
    if decimal_style is None and decimal_ambiguity_present(numeric_values):
        return {"status": "needs_decimal_style", "message": "Numeric columns have an ambiguous separator (e.g. '150.000'). Ask the user, then re-run with --decimal-style TR|US.", "sample_values": [v for v in numeric_values if decimal_ambiguity_present([v])][:5]}
    resolved_decimal_style = decimal_style or "TR"

    # Fixed 2026-08-18 (4th audit round, confirmed): an invalid --timezone
    # (a typo like "Europe/Istanbull") wasn't validated anywhere — every row
    # hit ZoneInfo(tz) inside the loop below, the exception was caught and
    # silently swallowed per-row (ts_utc left None), and the row's own
    # "timezone" field kept the invalid string as if it were real, with
    # status "ok" and no warning anywhere. Validated once, upfront, the same
    # way an ambiguous date format or decimal separator already stops the
    # pipeline instead of guessing through it.
    if tz != "unknown":
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(tz)
        except Exception as e:
            return {"status": "error", "message": f"Unknown timezone: {tz!r} ({e}). Pass a valid IANA timezone name (e.g. 'Europe/Istanbul') via --timezone, or omit it."}

    # Added 2026-10-10. When the source HAS a campaign / ad-group reference
    # column and it is empty on a row, that row belongs to no campaign / ad
    # group as far as the source itself says. A display name sitting on the
    # same row then can't be allowed to invent one: on a 25k-row export,
    # campaign- and budget-level rows carrying a stray ad_group_name turned
    # 300 real ad groups into 2,381 "changed ad groups" (identity falls back
    # to the name when there is no id). The name is dropped and counted. A
    # source with no reference column at all (names only, e.g. the legacy TR
    # export) is untouched — there the name IS the identity.
    campaign_ref_mapped = "campaign_id" in mapping or "campaign_resource" in mapping
    ad_group_ref_mapped = "ad_group_id" in mapping or "ad_group_resource" in mapping
    out_rows, unparseable, orphan_names = [], 0, 0
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
                else:
                    m3 = SUMMARY_TR_OLAN_DEGERINE_RE.search(str(raw_summary))
                    if m3:
                        old_value, new_value = m3.group(1), m3.group(2)
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
        # than split into per-field rows: a row that reaches this point with
        # several paths has old/new values that are flat strings, not a
        # per-field breakdown, so splitting would force the same value onto
        # each field — an invented per-field value. multi_field flags this
        # rather than silently presenting it as a single clean field. (A row
        # whose old/new DO carry per-field values never gets here as a list:
        # expand_multi_field_rows() already split it, before this loop.)
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
        account_name = g("account_name") or account_id or default_account_name
        resource_type = g("resource_type")
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

        # Fixed 2026-08-18 (found via a real API-connected user's export —
        # not a pasted audit): a real ChangeEvent row whose changed resource
        # IS the campaign/ad group itself (resource_type CAMPAIGN/AD_GROUP)
        # doesn't always carry a separate "campaign"/"ad_group" attributed
        # pointer — Google's own field only makes sense for a resource
        # BELOW the campaign (a keyword, an ad, a budget), since a campaign
        # doesn't point to itself. Confirmed dogfooding symptom: real rows
        # correctly categorized as "Campaign"/"AdGroup" still showed
        # campaign_id/ad_group_id empty, so "Changed Campaigns"/"Changed Ad
        # Groups" undercounted despite dozens of real campaign/ad-group-level
        # changes being present. The resource's own identity (already
        # resolved above from change_resource_name) IS its campaign/ad-group
        # identity in exactly this case.
        rt_upper = str(resource_type or "").upper()
        if not campaign_id and rt_upper == "CAMPAIGN":
            campaign_id = resource_id
        if not ad_group_id and rt_upper == "AD_GROUP":
            ad_group_id = resource_id
        campaign_name, ad_group_name = g("campaign_name") or None, g("ad_group_name") or None
        if campaign_name and campaign_ref_mapped and not campaign_id:
            campaign_name, orphan_names = None, orphan_names + 1
        if ad_group_name and ad_group_ref_mapped and not ad_group_id:
            ad_group_name, orphan_names = None, orphan_names + 1

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

        # Fixed 2026-10-10 (6th audit round): an empty source cell came out
        # as "" for these identity/reference fields while the *_id derived
        # from the very same cell came out None (user_email "" next to
        # user_id None, ad_group_resource "" next to ad_group_id None) — a
        # consumer had to test for both. Normalized here, at output only:
        # everything above already treats "" and None alike via `or`, so
        # change_id hashes and derived values are unchanged.
        out_rows.append({
            "change_id": change_id, "event_id": event_id, "source_event_id": source_event_id, "timestamp": str(raw_ts),
            "timestamp_iso": dt.isoformat(), "timezone": row_timezone, "timestamp_utc": ts_utc,
            "account_id": account_id, "account_name": account_name, "user_id": None,
            "user_name": user_name or None, "user_email": user_email or None, "client_type": client_type or None,
            "actor_type": actor_type, "resource_type": resource_type, "resource_path": resource_path or None, "resource_id": resource_id,
            "resource_name": resource_name or None, "campaign_id": campaign_id, "campaign_name": campaign_name,
            "campaign_resource": campaign_resource or None, "ad_group_resource": ad_group_resource or None,
            "ad_group_id": ad_group_id, "ad_group_name": ad_group_name, "operation": operation, "operation_confidence": operation_confidence,
            "field_name": field_name, "multi_field": multi_field, "old_value": old_value, "new_value": new_value,
            "old_value_num": old_num, "new_value_num": new_num,
            "old_value_display": old_value_display, "new_value_display": new_value_display,
            "value_unit": "units (converted from micros)" if is_micros_field else None, "currency": None,
            "value_confidence": value_confidence, "category": None, "subcategory": None,
            "category_confidence": None, "source": source_label, "raw_summary": raw_summary,
        })

    return {"status": "ok", "rows": out_rows, "rows_skipped_unparseable_date": unparseable, "orphan_names_dropped": orphan_names, "input_rows": source_row_count, "rows_added_by_field_split": len(rows) - source_row_count, "date_format_used": resolved_date_fmt, "decimal_style_used": resolved_decimal_style}


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
EXTRA_RULE_MATCHER_KEYS = {
    "resource_type", "resource_type_in", "resource_type_contains",
    "field_name_contains", "field_name_not_contains", "operation", "new_value_equals",
}


def validate_extra_rules(rules):
    """Fixed 2026-08-18 (4th audit round, confirmed): match_structured()
    trusted every extra_rules entry's shape completely — a rule missing
    "category"/"subcategory" crashed with a raw KeyError the moment a row
    matched it, deep inside categorization, not at load time. --extra-rules
    is this tool's own documented, official path for resolving
    needs_category_review, so a malformed rule here should stop with a clear
    message up front — the same "never guess past a genuine ambiguity"
    discipline applied to config, not just input data. Returns None if
    valid, or a message describing the first problem found. Deliberately
    does NOT require `category` to be one of CATEGORY_RULES's existing
    names — needs_category_review's own instructions tell the user to
    introduce a new category this way, so restricting to a closed set would
    break the documented workflow."""
    if not isinstance(rules, list):
        return f"--extra-rules must be a JSON list of rule objects, got {type(rules).__name__}"
    allowed_keys = EXTRA_RULE_MATCHER_KEYS | {"category", "subcategory"}
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            return f"--extra-rules[{i}] must be an object, got {type(rule).__name__}"
        if not isinstance(rule.get("category"), str) or not rule.get("category"):
            return f"--extra-rules[{i}] is missing a non-empty 'category' string"
        if not isinstance(rule.get("subcategory"), str) or not rule.get("subcategory"):
            return f"--extra-rules[{i}] is missing a non-empty 'subcategory' string"
        if not (EXTRA_RULE_MATCHER_KEYS & rule.keys()):
            return f"--extra-rules[{i}] has no matcher key — needs at least one of {sorted(EXTRA_RULE_MATCHER_KEYS)}"
        unknown = set(rule.keys()) - allowed_keys
        if unknown:
            return f"--extra-rules[{i}] has unsupported key(s) {sorted(unknown)} — supported: {sorted(allowed_keys)}"
    return None


# Added 2026-10-10. No source this script reads says which currency an
# amount is in (ChangeEvent carries micros, not a currency; the account's
# currency lives on the customer resource), so none is ever assumed. When
# the caller states it (--currency TRY), the rows that hold money get it as
# their unit; ratios and multipliers (target ROAS, bid modifiers) don't.
MONETARY_SUBCATEGORIES = {"Budget changed", "Budget created", "Budget removed", "Bid changed", "Target CPA"}
CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")


def apply_currency(rows, currency):
    for row in rows:
        monetary = "micros" in str(row.get("field_name") or "").lower() or row.get("subcategory") in MONETARY_SUBCATEGORIES
        if monetary and (row.get("old_value_num") is not None or row.get("new_value_num") is not None):
            row["currency"] = row["value_unit"] = currency
    return rows


def match_structured(row, extra_rules=None):
    rules = (extra_rules or []) + CATEGORY_RULES["structured_rules"]
    rt = (row.get("resource_type") or "").upper()
    fn = (row.get("field_name") or "").lower()
    op = (row.get("operation") or "").upper()
    nv = str(row.get("new_value") or "").upper()
    for rule in rules:
        if "resource_type" in rule and rule["resource_type"] != rt:
            continue
        if "resource_type_in" in rule and rt not in rule["resource_type_in"]:
            continue
        if "resource_type_contains" in rule and rule["resource_type_contains"].upper() not in rt:
            continue
        if "field_name_contains" in rule:
            needles = rule["field_name_contains"]
            if not all(n.lower() in fn for n in ([needles] if isinstance(needles, str) else needles)):
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
        else:
            # Fixed 2026-10-10: only human-actor rows were touched, but a
            # ChangeEvent made through a script, a bulk upload or a rule
            # still names the person it ran as in user_email. Those rows
            # kept the raw address in every output of a --mask-users run.
            # The row stays what it is (an automation row, shown by its
            # client_type); the address is removed, and so is a user_name
            # that is itself an address. A system name ("ads-budget-system")
            # is not a person and is kept.
            r["user_email"] = None
            if "@" in str(r.get("user_name") or ""):
                r["user_name"] = None
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
            "untouched_scope_note": "Lists only campaigns with at least one change in this export. A campaign that doesn't appear in the export is not listed: it may have had no change in the export's date range, or may not exist — this report can't tell which.",
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
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ads Change History</title>
<style>
/* lab-ui:begin */
/* Lab UI 1.0.0 - the visual language of alidemirbas.com.tr/lab, for the HTML
   every Lab tool writes (claude-lifecycle, dashboard-builder, the Google Ads
   change-history dashboard, ab-test-playbook).
   Source: design-system/lab-ui/ in the site repo. Tools keep a vendored copy
   and inline it into each self-contained output between the lab-ui markers;
   edit the source, then re-vendor - never edit an inlined copy.
   Values mirror the site's src/app/globals.css (stone ink, white paper, brand
   blue #2f6bff, Tailwind's hue ramps as the site builds them).
   No network: Inter is used where it is installed, the system sans elsewhere,
   because a Lab output must render offline and load nothing third-party. */

:root {
  --lab-font: Inter, "Inter Variable", ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --lab-font-code: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;

  /* Ink: stone, text and quiet fills */
  --lab-ink-950: #1f1c1b; --lab-ink-900: #292524; --lab-ink-800: #35312f; --lab-ink-700: #45403e;
  --lab-ink-600: #5c5755; --lab-ink-500: #77716f; --lab-ink-400: #98918f; --lab-ink-300: #bfb8b5;
  --lab-ink-200: #e2dad6; --lab-ink-100: #f5ede9;

  /* Brand blue */
  --lab-primary-50: #f3f7ff; --lab-primary-100: #e6efff; --lab-primary-200: #cee0ff; --lab-primary-300: #aecbff;
  --lab-primary-500: #5089ff; --lab-primary-600: #2f6bff; --lab-primary-700: #1c4dc9; --lab-primary-800: #123796;
  --lab-primary-950: #02113d;

  /* Hues, as the site builds them (Tailwind 4 ramps) */
  --lab-violet-50: #f5f3ff; --lab-violet-200: #ddd6ff; --lab-violet-500: #8d54ff; --lab-violet-700: #7008e7; --lab-violet-800: #5d0ec0;
  --lab-sky-50: #f0f9ff; --lab-sky-300: #77d4ff; --lab-sky-500: #00a5ef; --lab-sky-700: #0069a4; --lab-sky-800: #005986;
  --lab-teal-50: #f0fdfa; --lab-teal-200: #96f7e4; --lab-teal-500: #00baa7; --lab-teal-600: #009588; --lab-teal-700: #00776e; --lab-teal-800: #005f5a;
  --lab-amber-50: #fffbeb; --lab-amber-200: #fee685; --lab-amber-500: #f99c00; --lab-amber-700: #b75000; --lab-amber-800: #953d00;
  --lab-emerald-50: #ecfdf5; --lab-emerald-100: #d0fae5; --lab-emerald-200: #a4f4cf; --lab-emerald-300: #5ee9b5; --lab-emerald-500: #00bb7f; --lab-emerald-600: #009767; --lab-emerald-700: #007956; --lab-emerald-800: #005f46;
  --lab-rose-50: #fff1f2; --lab-rose-200: #ffccd3; --lab-rose-300: #ffa2ae; --lab-rose-500: #ff2357; --lab-rose-600: #e70044; --lab-rose-700: #c20039; --lab-rose-800: #a30037;
  --lab-orange-50: #fff7ed; --lab-orange-100: #ffedd5; --lab-orange-500: #fe6e00; --lab-orange-700: #c53c00; --lab-orange-800: #9f2d00;
  --lab-cyan-50: #ecfeff; --lab-cyan-200: #a2f4fd; --lab-cyan-700: #007492;
  --lab-indigo-50: #eef2ff; --lab-indigo-200: #c7d2ff; --lab-indigo-500: #625fff; --lab-indigo-700: #432dd7; --lab-indigo-800: #372aac;

  /* Surfaces and lines */
  --lab-ground: #f7f8fb;            /* page, = site paper-soft */
  --lab-surface: #ffffff;           /* tiles and cards, = site paper */
  --lab-surface-soft: #f7f8fb;      /* a tile inside a tile */
  --lab-text: var(--lab-ink-900);
  --lab-text-strong: var(--lab-ink-950);
  --lab-text-muted: var(--lab-ink-600);
  --lab-text-subtle: var(--lab-ink-500);
  --lab-line: #eef0f5;
  --lab-line-strong: #dfe3ec;
  --lab-ring: rgb(31 28 27 / 0.06);  /* the site's ring-ink-950/[0.06] */
  --lab-ring-strong: rgb(31 28 27 / 0.1);
  --lab-dot: rgb(10 16 32 / 0.22);

  --lab-brand: var(--lab-primary-600);
  --lab-brand-ink: var(--lab-primary-700);
  --lab-brand-soft: var(--lab-primary-50);
  --lab-brand-soft-2: var(--lab-primary-100);
  --lab-on-brand: #ffffff;
  --lab-button-hover: #1c1917;

  /* Status: one vocabulary for every tool (good / watch / warn / critical / info) */
  --lab-good: var(--lab-emerald-700);   --lab-good-soft: var(--lab-emerald-50);   --lab-good-line: var(--lab-emerald-200);
  --lab-watch: var(--lab-amber-800);    --lab-watch-soft: var(--lab-amber-50);    --lab-watch-line: var(--lab-amber-200);
  --lab-warn: var(--lab-orange-700);    --lab-warn-soft: var(--lab-orange-50);    --lab-warn-line: var(--lab-orange-100);
  --lab-bad: var(--lab-rose-700);       --lab-bad-soft: var(--lab-rose-50);       --lab-bad-line: var(--lab-rose-200);
  --lab-info: var(--lab-primary-700);   --lab-info-soft: var(--lab-primary-50);   --lab-info-line: var(--lab-primary-200);

  /* Channels, as the site's journey pages colour them */
  --lab-ch-email: var(--lab-violet-500);    --lab-ch-email-soft: var(--lab-violet-50);    --lab-ch-email-ink: var(--lab-violet-800);
  --lab-ch-push: var(--lab-sky-500);        --lab-ch-push-soft: var(--lab-sky-50);        --lab-ch-push-ink: var(--lab-sky-800);
  --lab-ch-sms: var(--lab-teal-500);        --lab-ch-sms-soft: var(--lab-teal-50);        --lab-ch-sms-ink: var(--lab-teal-800);
  --lab-ch-inapp: var(--lab-amber-500);     --lab-ch-inapp-soft: var(--lab-amber-50);     --lab-ch-inapp-ink: var(--lab-amber-800);
  --lab-ch-whatsapp: var(--lab-emerald-500); --lab-ch-whatsapp-soft: var(--lab-emerald-50); --lab-ch-whatsapp-ink: var(--lab-emerald-800);
  --lab-ch-human: var(--lab-rose-500);      --lab-ch-human-soft: var(--lab-rose-50);      --lab-ch-human-ink: var(--lab-rose-800);

  /* Series: charts draw in this order */
  --lab-series-1: var(--lab-primary-600);
  --lab-series-2: var(--lab-violet-500);
  --lab-series-3: var(--lab-teal-500);
  --lab-series-4: var(--lab-amber-500);
  --lab-series-5: var(--lab-rose-500);
  --lab-series-6: var(--lab-sky-500);

  --lab-radius-sm: 8px;
  --lab-radius-md: 12px;
  --lab-radius-card: 16px;
  --lab-radius-tile: 24px;
  --lab-radius-pill: 9999px;
  --lab-shadow-hairline: 0 1px 2px rgb(10 16 32 / 0.04);
  --lab-shadow-card: 0 1px 2px rgb(28 25 23 / 0.06), 0 4px 10px -8px rgb(28 25 23 / 0.45);
  --lab-shadow-lift: 0 18px 40px -24px rgb(10 16 32 / 0.35);
  --lab-shadow-float: 0 12px 30px -16px rgb(10 16 32 / 0.35);
  --lab-ease: cubic-bezier(0.22, 1, 0.36, 1);
  --lab-fast: 120ms;
  --lab-base: 250ms;
  color-scheme: light;
}

/* Dark: the same roles on the stone floor. Tints are mixed from each hue so a
   soft fill stays a fill, never a glare. */
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --lab-ground: #161312; --lab-surface: #1f1c1b; --lab-surface-soft: #292524;
    --lab-text: #f5ede9; --lab-text-strong: #ffffff; --lab-text-muted: #bfb8b5; --lab-text-subtle: #98918f;
    --lab-line: #35312f; --lab-line-strong: #45403e;
    --lab-ring: rgb(255 255 255 / 0.08); --lab-ring-strong: rgb(255 255 255 / 0.14); --lab-dot: rgb(255 255 255 / 0.16);
    --lab-brand: #5089ff; --lab-brand-ink: #aecbff; --lab-brand-soft: color-mix(in oklab, #2f6bff 20%, #1f1c1b); --lab-brand-soft-2: color-mix(in oklab, #2f6bff 30%, #1f1c1b); --lab-button-hover: #f5ede9;
    --lab-good: #5ee9b5; --lab-good-soft: color-mix(in oklab, #00bb7f 18%, #1f1c1b); --lab-good-line: color-mix(in oklab, #00bb7f 40%, #1f1c1b);
    --lab-watch: #fee685; --lab-watch-soft: color-mix(in oklab, #f99c00 18%, #1f1c1b); --lab-watch-line: color-mix(in oklab, #f99c00 40%, #1f1c1b);
    --lab-warn: #ffb86a; --lab-warn-soft: color-mix(in oklab, #fe6e00 18%, #1f1c1b); --lab-warn-line: color-mix(in oklab, #fe6e00 40%, #1f1c1b);
    --lab-bad: #ffa2ae; --lab-bad-soft: color-mix(in oklab, #ff2357 18%, #1f1c1b); --lab-bad-line: color-mix(in oklab, #ff2357 40%, #1f1c1b);
    --lab-info: #aecbff; --lab-info-soft: color-mix(in oklab, #2f6bff 20%, #1f1c1b); --lab-info-line: color-mix(in oklab, #2f6bff 45%, #1f1c1b);
    --lab-ch-email-soft: color-mix(in oklab, #8d54ff 20%, #1f1c1b); --lab-ch-email-ink: #ddd6ff;
    --lab-ch-push-soft: color-mix(in oklab, #00a5ef 20%, #1f1c1b); --lab-ch-push-ink: #77d4ff;
    --lab-ch-sms-soft: color-mix(in oklab, #00baa7 20%, #1f1c1b); --lab-ch-sms-ink: #96f7e4;
    --lab-ch-inapp-soft: color-mix(in oklab, #f99c00 20%, #1f1c1b); --lab-ch-inapp-ink: #fee685;
    --lab-ch-whatsapp-soft: color-mix(in oklab, #00bb7f 20%, #1f1c1b); --lab-ch-whatsapp-ink: #a4f4cf;
    --lab-ch-human-soft: color-mix(in oklab, #ff2357 20%, #1f1c1b); --lab-ch-human-ink: #ffccd3;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --lab-ground: #161312; --lab-surface: #1f1c1b; --lab-surface-soft: #292524;
  --lab-text: #f5ede9; --lab-text-strong: #ffffff; --lab-text-muted: #bfb8b5; --lab-text-subtle: #98918f;
  --lab-line: #35312f; --lab-line-strong: #45403e;
  --lab-ring: rgb(255 255 255 / 0.08); --lab-ring-strong: rgb(255 255 255 / 0.14); --lab-dot: rgb(255 255 255 / 0.16);
  --lab-brand: #5089ff; --lab-brand-ink: #aecbff; --lab-brand-soft: color-mix(in oklab, #2f6bff 20%, #1f1c1b); --lab-brand-soft-2: color-mix(in oklab, #2f6bff 30%, #1f1c1b); --lab-button-hover: #f5ede9;
  --lab-good: #5ee9b5; --lab-good-soft: color-mix(in oklab, #00bb7f 18%, #1f1c1b); --lab-good-line: color-mix(in oklab, #00bb7f 40%, #1f1c1b);
  --lab-watch: #fee685; --lab-watch-soft: color-mix(in oklab, #f99c00 18%, #1f1c1b); --lab-watch-line: color-mix(in oklab, #f99c00 40%, #1f1c1b);
  --lab-warn: #ffb86a; --lab-warn-soft: color-mix(in oklab, #fe6e00 18%, #1f1c1b); --lab-warn-line: color-mix(in oklab, #fe6e00 40%, #1f1c1b);
  --lab-bad: #ffa2ae; --lab-bad-soft: color-mix(in oklab, #ff2357 18%, #1f1c1b); --lab-bad-line: color-mix(in oklab, #ff2357 40%, #1f1c1b);
  --lab-info: #aecbff; --lab-info-soft: color-mix(in oklab, #2f6bff 20%, #1f1c1b); --lab-info-line: color-mix(in oklab, #2f6bff 45%, #1f1c1b);
  --lab-ch-email-soft: color-mix(in oklab, #8d54ff 20%, #1f1c1b); --lab-ch-email-ink: #ddd6ff;
  --lab-ch-push-soft: color-mix(in oklab, #00a5ef 20%, #1f1c1b); --lab-ch-push-ink: #77d4ff;
  --lab-ch-sms-soft: color-mix(in oklab, #00baa7 20%, #1f1c1b); --lab-ch-sms-ink: #96f7e4;
  --lab-ch-inapp-soft: color-mix(in oklab, #f99c00 20%, #1f1c1b); --lab-ch-inapp-ink: #fee685;
  --lab-ch-whatsapp-soft: color-mix(in oklab, #00bb7f 20%, #1f1c1b); --lab-ch-whatsapp-ink: #a4f4cf;
  --lab-ch-human-soft: color-mix(in oklab, #ff2357 20%, #1f1c1b); --lab-ch-human-ink: #ffccd3;
}

/* ---- base ------------------------------------------------------------- */
*, *::before, *::after { box-sizing: border-box; }
body.lab {
  margin: 0; background: var(--lab-ground); color: var(--lab-text);
  font-family: var(--lab-font); font-size: 15px; line-height: 1.6;
  font-feature-settings: "cv11", "ss01"; -webkit-font-smoothing: antialiased; text-rendering: optimizeLegibility;
}
.lab :where(h1, h2, h3, h4, p, dl, dd, figure) { margin: 0; }
.lab :where(h1, h2, h3, h4) { color: var(--lab-text-strong); text-wrap: balance; }
.lab code, .lab kbd, .lab pre { font-family: var(--lab-font-code); }
.lab a { color: var(--lab-brand-ink); text-underline-offset: 2px; }
.lab :focus-visible { outline: 2px solid var(--lab-brand); outline-offset: 2px; }
.lab .num, .lab table { font-variant-numeric: tabular-nums; }
.lab svg { flex: none; }

.lab-page { max-width: 1180px; margin: 0 auto; padding: 40px 20px 96px; }
.lab-page--narrow { max-width: 980px; }
.lab-stack { display: grid; gap: 16px; }
.lab-row { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.lab-grid { display: grid; gap: 16px; grid-template-columns: repeat(auto-fit, minmax(min(100%, 240px), 1fr)); }
.lab-sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }

/* ---- type ------------------------------------------------------------- */
/* Plain case everywhere: no uppercase tracked labels on this site. */
.lab-eyebrow { display: inline-flex; align-items: center; gap: 8px; font-size: 14px; font-weight: 500; line-height: 1.4; color: var(--lab-brand-ink); }
.lab-title { font-size: clamp(1.75rem, 1.1rem + 1.7vw, 2.5rem); line-height: 1.1; letter-spacing: -0.025em; font-weight: 600; }
.lab-h2 { font-size: 1.5rem; line-height: 1.25; letter-spacing: -0.016em; font-weight: 600; }
.lab-h3 { font-size: 1rem; line-height: 1.4; letter-spacing: -0.01em; font-weight: 600; }
.lab-lede { font-size: 1.0625rem; line-height: 1.65; color: var(--lab-text-muted); max-width: 68ch; text-wrap: pretty; }
.lab-muted { color: var(--lab-text-muted); }
.lab-subtle { color: var(--lab-text-subtle); }
.lab-small { font-size: 13px; line-height: 1.5; }
.lab-label { font-size: 12px; font-weight: 500; line-height: 1.4; color: var(--lab-text-subtle); }

/* ---- surfaces --------------------------------------------------------- */
.lab-tile { background: var(--lab-surface); border-radius: var(--lab-radius-tile); box-shadow: 0 0 0 1px var(--lab-ring); padding: 24px; }
.lab-card { background: var(--lab-surface); border-radius: var(--lab-radius-card); box-shadow: 0 0 0 1px var(--lab-ring), var(--lab-shadow-hairline); padding: 16px 18px; }
.lab-soft { background: var(--lab-surface-soft); border-radius: var(--lab-radius-card); padding: 16px 18px; }
.lab-tile--lift, .lab-card--lift { box-shadow: 0 0 0 1px var(--lab-ring), var(--lab-shadow-lift); }
.lab-divider { height: 1px; background: var(--lab-line); border: 0; margin: 16px 0; }

/* The icon tile: the site's tinted square behind every glyph. */
.lab-icon { display: inline-grid; place-items: center; width: 40px; height: 40px; border-radius: 12px; background: var(--lab-brand-soft); color: var(--lab-brand-ink); flex: none; }
.lab-icon > svg { width: 20px; height: 20px; }
.lab-icon--sm { width: 28px; height: 28px; border-radius: 8px; }
.lab-icon--sm > svg { width: 14px; height: 14px; }
.lab-icon--xs { width: 24px; height: 24px; border-radius: 8px; }
.lab-icon--xs > svg { width: 14px; height: 14px; }
.lab-icon--round { border-radius: 9999px; }

/* ---- chips, badges ---------------------------------------------------- */
.lab-chip { display: inline-flex; align-items: center; gap: 8px; min-height: 36px; padding: 4px 14px 4px 4px; border-radius: 9999px; background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); font-size: 14px; font-weight: 500; color: var(--lab-text-strong); }
.lab-chip--plain { padding: 6px 14px; min-height: 32px; font-size: 13px; }
.lab-chip > .lab-icon { width: 28px; height: 28px; border-radius: 9999px; }
.lab-chip > .lab-icon > svg { width: 14px; height: 14px; }
.lab-badge { display: inline-flex; align-items: center; gap: 5px; padding: 2px 10px; border-radius: 9999px; font-size: 12px; font-weight: 600; line-height: 1.6; white-space: nowrap; background: var(--lab-surface-soft); color: var(--lab-text-muted); }
.lab-badge > svg { width: 12px; height: 12px; }
.lab-badge--line { background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring-strong); }
.lab-var { font-family: var(--lab-font-code); font-size: 0.86em; font-weight: 500; padding: 1px 6px; border-radius: 6px; background: var(--lab-brand-soft); color: var(--lab-brand-ink); white-space: nowrap; }
.lab-kbd { font-family: var(--lab-font-code); font-size: 0.86em; padding: 1px 6px; border-radius: 6px; background: rgb(255 255 255 / 0.15); }

/* ---- buttons: the site's pill ---------------------------------------- */
.lab-btn { display: inline-flex; align-items: center; justify-content: center; gap: 8px; height: 36px; padding: 0 16px; border: 0; border-radius: 9999px; font: 500 14px/1 var(--lab-font); cursor: pointer; text-decoration: none; white-space: nowrap; background: var(--lab-brand); color: var(--lab-on-brand); transition: background-color var(--lab-fast) var(--lab-ease), color var(--lab-fast) var(--lab-ease), box-shadow var(--lab-fast) var(--lab-ease); }
.lab-btn > svg { width: 16px; height: 16px; }
.lab-btn:hover { background: var(--lab-button-hover); color: var(--lab-ground); }
.lab-btn--outline { background: var(--lab-surface); color: var(--lab-text-strong); box-shadow: inset 0 0 0 1px var(--lab-line-strong); }
.lab-btn--outline:hover { background: var(--lab-brand); color: var(--lab-on-brand); box-shadow: inset 0 0 0 1px var(--lab-brand); }
.lab-btn--ghost { background: transparent; color: var(--lab-text-muted); }
.lab-btn--ghost:hover { background: var(--lab-surface-soft); color: var(--lab-text-strong); }
.lab-btn--md { height: 44px; padding: 0 20px; }
.lab-btn--lg { height: 48px; padding: 0 24px; font-size: 16px; }

/* ---- tabs ------------------------------------------------------------- */
/* The segmented switch: the site's Info / Canvas tabs. */
.lab-seg { display: inline-flex; flex-wrap: wrap; gap: 4px; padding: 4px; border-radius: 9999px; background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); }
.lab-seg > button, .lab-seg > a { display: inline-flex; align-items: center; gap: 8px; height: 36px; padding: 0 16px; border: 0; border-radius: 9999px; background: transparent; color: var(--lab-text-muted); font: 500 14px/1 var(--lab-font); cursor: pointer; text-decoration: none; white-space: nowrap; transition: background-color var(--lab-fast) var(--lab-ease), color var(--lab-fast) var(--lab-ease); }
.lab-seg > button:hover { background: var(--lab-surface-soft); color: var(--lab-text-strong); }
.lab-seg > [aria-selected="true"], .lab-seg > [aria-pressed="true"], .lab-seg > .is-on { background: var(--lab-ink-950); color: #fff; }
:root[data-theme="dark"] .lab-seg > [aria-selected="true"] { background: #f5ede9; color: #1f1c1b; }
/* Picker tiles: one tile per item, the chosen one ringed in brand blue. */
.lab-picker { display: grid; gap: 10px; grid-template-columns: repeat(auto-fill, minmax(min(100%, 200px), 1fr)); }
.lab-pick { display: flex; flex-direction: column; gap: 4px; text-align: left; padding: 14px 16px; border: 0; border-radius: var(--lab-radius-card); background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); color: var(--lab-text-strong); font: 600 14px/1.35 var(--lab-font); cursor: pointer; transition: box-shadow var(--lab-fast) var(--lab-ease), background-color var(--lab-fast) var(--lab-ease); }
.lab-pick:hover { box-shadow: 0 0 0 1px var(--lab-ring-strong), var(--lab-shadow-card); }
.lab-pick > small { font-size: 12.5px; font-weight: 400; line-height: 1.45; color: var(--lab-text-subtle); }
.lab-pick[aria-selected="true"] { background: var(--lab-brand-soft); box-shadow: 0 0 0 2px var(--lab-brand); }

/* ---- lists, tables, forms -------------------------------------------- */
.lab-dl { display: grid; grid-template-columns: minmax(96px, max-content) 1fr; gap: 8px 16px; font-size: 14px; }
.lab-dl > dt { font-size: 13px; font-weight: 500; color: var(--lab-text-subtle); padding-top: 1px; }
.lab-dl > dd { margin: 0; color: var(--lab-text); min-width: 0; overflow-wrap: anywhere; }
.lab-list { list-style: none; margin: 0; padding: 0; display: grid; gap: 10px; }
.lab-list > li { display: flex; gap: 10px; align-items: flex-start; font-size: 14px; line-height: 1.5; }
.lab-list > li > svg { width: 16px; height: 16px; margin-top: 2px; }
.lab-table-wrap { overflow-x: auto; border-radius: var(--lab-radius-card); background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); }
.lab-table { width: 100%; border-collapse: collapse; font-size: 14px; }
.lab-table th { text-align: left; font-size: 12.5px; font-weight: 500; color: var(--lab-text-subtle); padding: 10px 14px; border-bottom: 1px solid var(--lab-line); white-space: nowrap; }
.lab-table td { padding: 11px 14px; border-bottom: 1px solid var(--lab-line); vertical-align: top; }
.lab-table tr:last-child td { border-bottom: 0; }
.lab-table .r { text-align: right; }
.lab-input, .lab-select { height: 36px; padding: 0 12px; border: 0; border-radius: 10px; background: var(--lab-surface); color: var(--lab-text); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 400 14px/1 var(--lab-font); }
.lab-select { padding-right: 32px; appearance: none; background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%2377716f' stroke-width='2'%3E%3Cpath d='m6 9 6 6 6-6'/%3E%3C/svg%3E"); background-repeat: no-repeat; background-position: right 10px center; }
.lab-input:focus, .lab-select:focus { outline: none; box-shadow: inset 0 0 0 2px var(--lab-brand); }

/* ---- numbers ---------------------------------------------------------- */
.lab-kpi { display: flex; flex-direction: column; gap: 6px; }
.lab-kpi-value { font-size: 2rem; line-height: 1.05; letter-spacing: -0.025em; font-weight: 600; color: var(--lab-text-strong); font-variant-numeric: tabular-nums; }
.lab-kpi-label { font-size: 13px; font-weight: 500; color: var(--lab-text-subtle); }
.lab-delta { display: inline-flex; align-items: center; gap: 4px; align-self: flex-start; padding: 2px 8px; border-radius: 9999px; font-size: 12px; font-weight: 600; font-variant-numeric: tabular-nums; }
.lab-meter { position: relative; height: 8px; border-radius: 9999px; background: var(--lab-line); overflow: hidden; }
.lab-meter > span { position: absolute; inset: 0 auto 0 0; border-radius: inherit; background: var(--lab-brand); }
.lab-meter--good > span { background: var(--lab-emerald-500); }
.lab-meter--watch > span { background: var(--lab-amber-500); }
.lab-meter--bad > span { background: var(--lab-rose-500); }
.lab-count { font-size: 12px; font-weight: 500; font-variant-numeric: tabular-nums; color: var(--lab-text-subtle); white-space: nowrap; }
.lab-count.is-ok { color: var(--lab-good); }
.lab-count.is-over { color: var(--lab-bad); }

/* A note with a status edge: alerts, findings */
.lab-note { display: flex; gap: 12px; align-items: flex-start; padding: 14px 16px; border-radius: var(--lab-radius-card); background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); }
.lab-note > .lab-icon { width: 28px; height: 28px; border-radius: 8px; }
.lab-note > .lab-icon > svg { width: 14px; height: 14px; }

/* ---- the journey canvas: the site's cards on the dot sheet ------------ */
.lab-sheet { position: relative; border-radius: var(--lab-radius-tile); background-color: var(--lab-surface-soft); background-image: radial-gradient(var(--lab-dot) 0.9px, transparent 1.1px); background-size: 24px 24px; box-shadow: 0 0 0 1px var(--lab-ring); }
.lab-node { position: relative; width: 268px; padding: 12px 14px; border-radius: 16px; background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring-strong), var(--lab-shadow-hairline); color: var(--lab-text); font-size: 13px; line-height: 1.45; }
.lab-node-head { display: flex; align-items: center; gap: 8px; min-width: 0; }
.lab-node-kind { font-size: 12px; font-weight: 500; }
.lab-node-tag { margin-left: auto; font-size: 11.5px; color: var(--lab-text-subtle); text-align: right; min-width: 0; }
.lab-node-title { margin-top: 8px; font-size: 14px; line-height: 1.4; font-weight: 600; color: var(--lab-text-strong); }
.lab-node-pill { display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px 3px 4px; border-radius: 9999px; font-size: 12px; font-weight: 600; }
.lab-node-pill > .lab-icon { width: 20px; height: 20px; border-radius: 9999px; background: transparent; color: inherit; }
/* Trigger: the one dark card, with the Entry pin standing on it. */
.lab-node--trigger { background: var(--lab-ink-950); color: #fff; box-shadow: 0 0 0 1px rgb(41 37 36 / 0.7), 0 12px 28px -16px rgb(10 16 32 / 0.65); }
.lab-node--trigger .lab-node-title { color: #fff; }
.lab-node--trigger .lab-node-tag, .lab-node--trigger .lab-node-kind { color: rgb(255 255 255 / 0.8); }
.lab-node-pin { position: absolute; top: -14px; left: 12px; display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px; border-radius: 9999px; background: var(--lab-ink-950); color: #fff; font-size: 12px; font-weight: 600; box-shadow: 0 0 0 2px var(--lab-surface), 0 6px 16px -8px rgb(10 16 32 / 0.6); }
.lab-node-pin > svg { width: 12px; height: 12px; }
/* Message: a paper card with its channel's colour along the top. */
.lab-node--message { border-top: 3px solid var(--lab-node-accent, var(--lab-brand)); }
/* Decision: the green question card. */
.lab-node--decision { background: var(--lab-good-soft); box-shadow: 0 0 0 1px var(--lab-good-line), var(--lab-shadow-hairline); }
.lab-node--decision .lab-node-title { color: var(--lab-emerald-800); }
:root[data-theme="dark"] .lab-node--decision .lab-node-title { color: var(--lab-good); }
/* Wait and exit: pills, not cards. */
.lab-node--pill { width: auto; max-width: 280px; display: inline-flex; align-items: center; gap: 8px; padding: 6px 14px 6px 6px; border-radius: 9999px; font-size: 13px; font-weight: 500; }
.lab-node--wait { box-shadow: 0 0 0 1px var(--lab-teal-200), var(--lab-shadow-hairline); color: var(--lab-text-strong); }
.lab-node--exit { background: var(--lab-surface-soft); box-shadow: none; border: 1.5px dashed var(--lab-line-strong); color: var(--lab-text-muted); }
.lab-node--exit.is-good { background: var(--lab-good-soft); border-color: var(--lab-good-line); color: var(--lab-good); }
/* Rows inside a card: icon + text lines, hairline between. */
.lab-node-rows { margin-top: 8px; display: grid; gap: 0; }
.lab-node-row { display: flex; gap: 8px; align-items: flex-start; padding: 6px 0; border-top: 1px solid var(--lab-line); font-size: 12.5px; line-height: 1.5; }
.lab-node-row:first-child { border-top: 0; padding-top: 2px; }
.lab-node-row > svg { width: 14px; height: 14px; margin-top: 2px; color: var(--lab-text-subtle); }
.lab-node-row > span { min-width: 0; overflow-wrap: anywhere; }
.lab-node--trigger .lab-node-row { border-top-color: rgb(255 255 255 / 0.12); }
.lab-node--trigger .lab-node-row > svg { color: rgb(255 255 255 / 0.7); }
.lab-node--decision .lab-node-row { border-top-color: var(--lab-good-line); }
/* Branch label on an edge: a small pill in the decision's green. */
.lab-edge-label { display: inline-flex; align-items: center; padding: 2px 10px; border-radius: 9999px; background: var(--lab-good-soft); color: var(--lab-good); box-shadow: 0 0 0 1px var(--lab-good-line); font-size: 12px; font-weight: 500; line-height: 1.6; }
.lab-edge { fill: none; stroke: var(--lab-ink-300); stroke-width: 1.5; stroke-linecap: round; stroke-linejoin: round; }
:root[data-theme="dark"] .lab-edge { stroke: var(--lab-ink-600); }

/* Disclosure: a quiet toggle for detail that is not needed at a glance. */
.lab-disclosure { display: inline-flex; align-items: center; gap: 6px; padding: 0; border: 0; background: none; color: var(--lab-brand-ink); font: 500 12.5px/1.4 var(--lab-font); cursor: pointer; }
.lab-disclosure > svg { width: 13px; height: 13px; transition: transform var(--lab-fast) var(--lab-ease); }
[data-open="true"] > .lab-disclosure > svg, .lab-disclosure[aria-expanded="true"] > svg { transform: rotate(90deg); }

/* Tooltip and toast: ink plates */
.lab-tip { position: relative; display: inline; }
.lab-tip-trigger { text-decoration: underline dotted var(--lab-text-subtle); text-underline-offset: 3px; cursor: help; }
.lab-tip-bubble { position: absolute; left: 50%; bottom: calc(100% + 8px); z-index: 30; width: max-content; max-width: 240px; padding: 8px 12px; border-radius: 10px; background: var(--lab-ink-950); color: #fff; font-size: 12.5px; font-weight: 400; line-height: 1.45; box-shadow: var(--lab-shadow-float); opacity: 0; pointer-events: none; transform: translate(-50%, 4px); transition: opacity var(--lab-fast) var(--lab-ease), transform var(--lab-fast) var(--lab-ease); }
.lab-tip:hover .lab-tip-bubble, .lab-tip:focus-within .lab-tip-bubble { opacity: 1; transform: translate(-50%, 0); }
.lab-toast { position: fixed; left: 50%; bottom: 24px; z-index: 999; max-width: 88vw; padding: 10px 18px; border-radius: 9999px; background: var(--lab-ink-950); color: #fff; font: 500 13px/1.4 var(--lab-font); text-align: center; box-shadow: var(--lab-shadow-float); opacity: 0; pointer-events: none; transform: translate(-50%, 6px); transition: opacity var(--lab-base) var(--lab-ease), transform var(--lab-base) var(--lab-ease); }
.lab-toast.is-on { opacity: 1; transform: translate(-50%, 0); }

/* The tool's mark in a page footer */
.lab-foot { margin-top: 56px; display: flex; flex-wrap: wrap; gap: 8px 16px; align-items: center; font-size: 13px; color: var(--lab-text-subtle); }

/* ---- tints ------------------------------------------------------------
   Background + ink pairs for icon tiles, chips, badges, deltas. Last in the
   file on purpose: a tint always wins over a component's resting fill. */
.lab-t-brand { background: var(--lab-brand-soft); color: var(--lab-brand-ink); }
.lab-t-good { background: var(--lab-good-soft); color: var(--lab-good); }
.lab-t-watch { background: var(--lab-watch-soft); color: var(--lab-watch); }
.lab-t-warn { background: var(--lab-warn-soft); color: var(--lab-warn); }
.lab-t-bad { background: var(--lab-bad-soft); color: var(--lab-bad); }
.lab-t-neutral { background: var(--lab-surface-soft); color: var(--lab-text-muted); }
.lab-t-email { background: var(--lab-ch-email-soft); color: var(--lab-ch-email-ink); }
.lab-t-push { background: var(--lab-ch-push-soft); color: var(--lab-ch-push-ink); }
.lab-t-sms { background: var(--lab-ch-sms-soft); color: var(--lab-ch-sms-ink); }
.lab-t-inapp { background: var(--lab-ch-inapp-soft); color: var(--lab-ch-inapp-ink); }
.lab-t-whatsapp { background: var(--lab-ch-whatsapp-soft); color: var(--lab-ch-whatsapp-ink); }
.lab-t-human { background: var(--lab-ch-human-soft); color: var(--lab-ch-human-ink); }

@media (max-width: 640px) {
  .lab-page { padding: 28px 16px 72px; }
  .lab-tile { padding: 18px; border-radius: 20px; }
  .lab-dl { grid-template-columns: 1fr; gap: 2px 0; }
  .lab-picker { grid-template-columns: 1fr 1fr; gap: 8px; }
  .lab-pick { padding: 12px 13px; font-size: 13.5px; }
  .lab-pick > small { font-size: 12px; }
  .lab-dl > dd { margin-bottom: 10px; }
}
@media (prefers-reduced-motion: reduce) {
  .lab *, .lab *::before, .lab *::after { transition: none !important; animation: none !important; }
}
@media print {
  body.lab { background: #fff; }
  .lab-tile, .lab-card, .lab-table-wrap, .lab-note { box-shadow: 0 0 0 1px #dfe3ec; }
  .lab-btn, .lab-seg, .lab-toast, .lab-tip-bubble { display: none !important; }
}
/* lab-ui:end */

  /* ---- the dashboard, on Lab UI (the site's visual language: stone ink on
     white paper, the brand blue, plain-case labels, tiles) -------------- */
  :root { --bad: var(--lab-bad); --good: var(--lab-good); --accent: var(--lab-brand); --text-dim: var(--lab-text-subtle); }
  header { max-width: 1440px; margin: 0 auto; padding: 36px 28px 8px; }
  .lab header h1 { margin: 0 0 6px; font-size: clamp(1.75rem, 1.1rem + 1.7vw, 2.5rem); line-height: 1.1; letter-spacing: -0.025em; font-weight: 600; color: var(--lab-text-strong); }
  header .meta { color: var(--lab-text-subtle); font-size: 13.5px; }
  header details.note { margin-top: 10px !important; }
  header details summary { color: var(--lab-brand-ink); font-weight: 500; }
  main { padding: 12px 28px 56px; display: flex; flex-direction: column; gap: 16px; max-width: 1440px; margin: 0 auto; }
  section { background: var(--lab-surface); border-radius: var(--lab-radius-tile); box-shadow: 0 0 0 1px var(--lab-ring); padding: 22px 24px; }
  .lab section h2 { margin: 0 0 12px; display: flex; flex-wrap: wrap; align-items: baseline; gap: 8px; font-size: 1.0625rem; line-height: 1.35; letter-spacing: -0.01em; font-weight: 600; color: var(--lab-text-strong); }
  section h2 .note { margin: 0; font-size: 13px; font-weight: 400; color: var(--lab-text-subtle); }
  .section-caption { margin: -4px 0 18px; font-size: 13.5px; line-height: 1.5; color: var(--lab-text-subtle); max-width: 80ch; }
  .filters { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  .filters select, .filters input[type=text], .explorer-search, input.ctl { height: 36px; padding: 0 12px; border: 0; border-radius: 10px; background: var(--lab-surface); color: var(--lab-text); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 400 14px/1 var(--lab-font); }
  .filters select { min-width: 140px; }
  .filters select:focus, .filters input:focus, .explorer-search:focus, input.ctl:focus { outline: none; box-shadow: inset 0 0 0 2px var(--lab-brand); }
  .filters select option { color: #1f1c1b; background: #fff; }
  .rangebtns, .threshbtns { display: inline-flex; flex-wrap: wrap; gap: 4px; padding: 4px; border-radius: 9999px; background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); }
  .rangebtns button, .threshbtns button { height: 30px; padding: 0 14px; border: 0; border-radius: 9999px; background: transparent; color: var(--lab-text-muted); font: 500 13px/1 var(--lab-font); cursor: pointer; }
  .rangebtns button:hover, .threshbtns button:hover { background: var(--lab-surface-soft); color: var(--lab-text-strong); }
  .rangebtns button.active, .threshbtns button.active { background: var(--lab-ink-950); color: #fff; }
  .clearbtn { margin-left: auto; height: 36px; padding: 0 16px; border: 0; border-radius: 9999px; background: var(--lab-surface); color: var(--lab-text-strong); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 500 13.5px/1 var(--lab-font); cursor: pointer; }
  .clearbtn:hover { background: var(--lab-brand); color: #fff; box-shadow: inset 0 0 0 1px var(--lab-brand); }
  #filterContext { margin-top: 12px; min-height: 1em; font-size: 13px; color: var(--lab-brand-ink); }
  .summary-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
  @media (max-width: 900px) { .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  .stat { display: flex; flex-direction: column; gap: 6px; padding: 16px 18px; border-radius: var(--lab-radius-card); background: var(--lab-surface-soft); min-width: 0; }
  .stat .n { font-size: 1.5rem; line-height: 1.15; letter-spacing: -0.02em; font-weight: 600; color: var(--lab-text-strong); overflow-wrap: anywhere; font-variant-numeric: tabular-nums; }
  .stat .l { font-size: 13px; font-weight: 500; color: var(--lab-text-subtle); }
  .bar-row { display: grid; grid-template-columns: 200px 1fr 60px; gap: 12px; align-items: center; padding: 6px 0; cursor: pointer; }
  .bar-row:hover .bar-label { color: var(--lab-brand-ink); }
  .bar-label { font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--lab-text); }
  .bar-track { height: 8px; border-radius: 9999px; background: var(--lab-line); overflow: hidden; }
  .bar-fill { height: 100%; border-radius: 9999px; background: var(--lab-brand); }
  .bar-n { text-align: right; font-size: 13px; font-weight: 500; color: var(--lab-text-subtle); font-variant-numeric: tabular-nums; }
  .grp-label { margin: 0 0 6px; font-size: 13px; font-weight: 600; color: var(--lab-text-strong); }
  .grp-label + div + .grp-label, #humanBars + .grp-label { margin-top: 16px; }
  .timeline { display: flex; align-items: flex-end; gap: 3px; height: 128px; overflow-x: auto; padding: 8px 0 4px; }
  .tl-bar { position: relative; min-width: 8px; flex: 1 0 8px; border-radius: 6px 6px 2px 2px; background: var(--lab-brand); }
  .tl-bar.edge { background: var(--lab-primary-300); }
  .tl-bar:hover { background: var(--lab-primary-700); }
  .tl-bar .tip { display: none; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%); margin-bottom: 6px; padding: 4px 8px; border-radius: 8px; background: var(--lab-ink-950); color: #fff; font-size: 12px; white-space: nowrap; }
  .tl-bar:hover .tip { display: block; }
  .acc-block { padding: 8px 0; border-bottom: 1px solid var(--lab-line); }
  .acc-block:last-child { border-bottom: 0; }
  .acc-head { display: flex; align-items: center; gap: 10px; cursor: pointer; font-weight: 600; }
  .acc-head .chev { font-size: 11px; color: var(--lab-text-subtle); transition: transform var(--lab-fast) var(--lab-ease); }
  .acc-head.open .chev { transform: rotate(90deg); }
  .camp-list { display: none; margin-top: 6px; padding-left: 22px; }
  .camp-list.open { display: block; }
  table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
  th, td { max-width: 220px; padding: 10px 12px; border-bottom: 1px solid var(--lab-line); text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  th { position: sticky; top: 0; background: var(--lab-surface); color: var(--lab-text-subtle); font-size: 12.5px; font-weight: 500; cursor: pointer; user-select: none; }
  th.sorted { color: var(--lab-text-strong); }
  th.sorted::after { content: " ▾"; color: var(--lab-brand); }
  tr.datarow { cursor: pointer; }
  tr.datarow:hover { background: var(--lab-surface-soft); }
  .pill { display: inline-block; padding: 1px 9px; border-radius: 9999px; border: 1px solid transparent; font-size: 12px; font-weight: 600; line-height: 1.6; background: var(--lab-surface-soft); color: var(--lab-text-muted); }
  .pill-auto { color: var(--lab-amber-800); background: var(--lab-amber-50); border-color: var(--lab-amber-200); }
  .pill-rule { color: var(--lab-violet-800); background: var(--lab-violet-50); border-color: var(--lab-violet-200); }
  .pager { display: flex; gap: 8px; align-items: center; margin-top: 12px; font-size: 13px; color: var(--lab-text-subtle); }
  .pager button { height: 32px; padding: 0 14px; border: 0; border-radius: 9999px; background: var(--lab-surface); color: var(--lab-text-strong); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 500 13px/1 var(--lab-font); cursor: pointer; }
  .pager button:disabled { opacity: 0.4; cursor: default; }
  .explorer-search { width: 100%; margin-bottom: 12px; }
  .cfg-label { margin-bottom: 6px; font-size: 13px; font-weight: 600; color: var(--lab-text-strong); }
  .cfg-field { display: flex; flex-direction: column; gap: 6px; font-size: 13px; color: var(--lab-text-muted); }
  .cfg-check { display: flex; align-items: center; gap: 7px; cursor: pointer; font-size: 13.5px; }
  input.ctl { width: 80px; height: 32px; }
  .detail-overlay { display: none; position: fixed; inset: 0; z-index: 50; align-items: center; justify-content: center; padding: 16px; background: rgb(22 19 18 / 0.45); backdrop-filter: blur(2px); }
  .detail-overlay.open { display: flex; }
  .detail-card { width: min(540px, 100%); max-height: 85vh; overflow: auto; padding: 24px; border-radius: var(--lab-radius-tile); background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring), var(--lab-shadow-lift); }
  .detail-card h3 { margin: 0 0 16px; font-size: 1.125rem; font-weight: 600; color: var(--lab-text-strong); }
  .detail-card dl { display: grid; grid-template-columns: 130px 1fr; gap: 8px 12px; margin: 0; font-size: 14px; }
  .detail-card dt { color: var(--lab-text-subtle); font-size: 13px; }
  .detail-card dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
  .detail-card .close { float: right; cursor: pointer; color: var(--lab-text-subtle); }
  .note { margin-top: 8px; font-size: 13px; color: var(--lab-text-subtle); }
  .warn-box { margin: 10px 0 4px; padding: 10px 14px; border-radius: 12px; background: var(--lab-watch-soft); color: var(--lab-watch); font-size: 13.5px; }
  .empty { padding: 20px 0; text-align: center; color: var(--lab-text-subtle); }
  @media (max-width: 640px) {
    header { padding: 24px 16px 6px; }
    main { padding: 8px 16px 40px; gap: 12px; }
    section { padding: 16px; border-radius: 20px; }
    .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .stat { padding: 12px 14px; }
    .stat .n { font-size: 1.25rem; }
    .bar-row { grid-template-columns: 110px 1fr 44px; gap: 10px; }
    .filters select, .filters input[type=text] { flex: 1 1 140px; min-width: 0; }
    .clearbtn { margin-left: 0; }
    th, td { max-width: 150px; }
    .detail-card dl { grid-template-columns: 1fr; gap: 2px; }
    .detail-card dd { margin-bottom: 8px; }
  }
  /* Print / "Save as PDF". The screen layout clips: the Explorer scrolls
     sideways and its cells are single-line with an ellipsis, so on paper the
     right-hand columns (old/new value, matches, source) were simply cut off.
     On paper the table gets the full landscape width and its cells wrap. */
  @media print {
    @page { size: A4 landscape; margin: 10mm; }
    header { padding: 0 0 6px; }
    /* block, not the screen's flex column: Chrome won't break a page between
       flex items cleanly and pushed whole sections to the next page */
    main { display: block; padding: 0; }
    /* a real border, not the screen's 1px ring shadow: a shadow isn't part
       of the box, so its top hairline printed on the PREVIOUS page */
    section { margin: 0 0 10px; padding: 12px 14px; border: 1px solid #dfe3ec; box-shadow: none; break-inside: avoid; }
    .summary-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
    .stat { padding: 10px 12px; }
    .stat .n { font-size: 1.1rem; overflow-wrap: anywhere; }
    section:last-of-type { break-inside: auto; }
    .filters, .explorer-search, .pager button, #detailOverlay { display: none !important; }
    #filterContext { margin: 0; }
    .timeline { overflow: visible; }
    section > div[style*="overflow-x"] { overflow: visible !important; }
    table { font-size: 9.5px; table-layout: auto; }
    th, td { max-width: none; padding: 4px 6px; white-space: normal; overflow: visible; text-overflow: clip; overflow-wrap: anywhere; vertical-align: top; }
    th { position: static; font-size: 9px; }
    thead { display: table-header-group; }
    tr { break-inside: avoid; }
    .pill { padding: 0 6px; font-size: 9px; white-space: nowrap; }
    td .note { font-size: inherit; }
  }
</style>
</head>
<body class="lab">
<header>
  <h1>Ads Change History</h1>
  <div class="meta" id="metaLine"></div>
  <div class="meta" id="dataQuality"></div>
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
      <li>A campaign that doesn't appear in the input isn't reported as "unchanged": no row means no evidence either way, including whether it exists.</li>
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
      <select id="fRuleMatch" size="1">
        <option value="">All changes</option>
        <option value="__matched__">Matched a rule</option>
      </select>
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
    <h2>Rule Matches</h2>
    <div class="section-caption">Deterministic, user-adjustable rules — a match means the change crossed a threshold you set, not that it was good or bad. Off by default; every match always shows the exact number and rule next to it.</div>
    <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:10px;">
      <label style="display:flex;align-items:center;gap:7px;cursor:pointer;font-size:13px;">
        <input type="checkbox" id="ruleMatchesToggle"> Rule-based change highlighting
      </label>
      <details id="ruleMatchesConfig">
        <summary style="cursor:pointer;">Configure rules</summary>
        <div style="margin-top:10px;">
          <div class="cfg-label">Magnitude (±% change)</div>
          <div id="ruleMatchesMagnitudeInputs" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-bottom:12px;"></div>
          <div class="cfg-label">Structural</div>
          <div id="ruleMatchesStateInputs" style="display:flex;flex-wrap:wrap;gap:14px;"></div>
        </div>
      </details>
    </div>
    <div class="note" id="ruleMatchesCount"></div>
    <div id="ruleMatches"></div>
  </section>
  <section>
    <h2>Campaign Last Changes</h2>
    <div class="section-caption">Lists only campaigns with at least one change in this export. A campaign that doesn't appear in the export is not listed: it may have had no change in the export's date range, or may not exist — this report can't tell which. "Days since" is counted to the current time in your browser and under the current filters, not to the moment this file was generated.</div>
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
        <th data-key="field_name">Change</th><th data-key="old_value">Old Value</th><th data-key="new_value">New Value</th><th>Matches</th><th data-key="source">Source</th>
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
// The rows ship column-packed (see pack_changes() in the generator): rebuild
// the plain row objects once, here, so everything below reads DASH_DATA.changes
// exactly as before.
function unpackChanges(packed){
  const rows = Array.from({length: packed.n}, ()=>({}));
  for(const k of packed.keys){
    const col = packed.cols[k];
    if(col.v){ for(let i=0;i<packed.n;i++) rows[i][k] = col.v[i]; }
    else { for(let i=0;i<packed.n;i++) rows[i][k] = col.d[col.i[i]]; }
  }
  return rows;
}
if(DASH_DATA && DASH_DATA.changes_packed){
  DASH_DATA.changes = unpackChanges(DASH_DATA.changes_packed);
  delete DASH_DATA.changes_packed;
}
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
// Lab UI hues (the site's ramps), taken at the 700 step so a pill's text
// stays readable on white paper.
const CATEGORY_COLORS = {
  Budget:'#1c4dc9', Bidding:'#7008e7', Keyword:'#00776e', Ad:'#c53c00', Asset:'#b75000',
  Targeting:'#c20039', Audience:'#0069a4', Status:'#007956', Campaign:'#432dd7',
  AdGroup:'#007492', Conversion:'#953d00', Feed:'#005f46', Account:'#5d0ec0',
};
function categoryPillStyle(cat){
  const c = CATEGORY_COLORS[cat];
  return c ? ` style="color:${c};border-color:${c}33;background:${c}12;"` : '';
}
// General-purpose accent cycle for things with no fixed category identity —
// KPI cards, per-account bars. Same visual-only rationale as CATEGORY_COLORS.
// Lab UI series order; bars that carry no category draw in the first.
const HUES = ['#2f6bff','#8d54ff','#00baa7','#f99c00','#ff2357','#00a5ef'];
function barFillStyle(color, pct){
  return `width:${Math.max(2,pct)}%;background:${color};`;
}
// =====================================================================
// RULE MATCHES (V1: Magnitude + State) — deterministic, user-adjustable
// thresholds. A match means "this row crossed a value you set," never a
// verdict on whether the change was good, bad, or risky — no severity
// labels, no danger-red color, and every match always renders with the
// exact number and rule next to it so the reader judges for themselves.
// Computed entirely client-side from fields already in every row (like
// Category Distribution / User Activity / etc. already are) — no CLI flag,
// no re-run needed to change a threshold. Off by default.
// =====================================================================
// Matched on category/subcategory, not field_name/resource_type — those two
// are only populated for structured (field-column) sources. A text-summary
// source (the Turkish legacy format, and the real native Google Ads UI
// "Change history" CSV export — see docs/DOGFOODING-NOTES.md) leaves both
// null, but categorize_changes() always resolves category/subcategory via
// one path or the other, so it's the one signal reliable across every shape.
const MAGNITUDE_RULES = [
  { id:'budget_magnitude', label:'Budget change', defaultPct:50, match:c=>c.subcategory==='Budget changed' },
  { id:'target_cpa_magnitude', label:'Target CPA change', defaultPct:30, match:c=>c.subcategory==='Target CPA' },
  { id:'target_roas_magnitude', label:'Target ROAS change', defaultPct:30, match:c=>c.subcategory==='Target ROAS' },
  { id:'bid_magnitude', label:'Bid/CPC change', defaultPct:50, match:c=>c.subcategory==='Bid changed' },
];
// PAUSED->ENABLED deliberately not included: pausing stops something that
// was running (a structural event worth surfacing), re-enabling resumes
// normal operation (less surprising) — an intentional asymmetry, not a gap.
const STATE_RULES = [
  // 'Paused' is shared by campaign/ad-group/keyword pause events in a
  // text-summary source (no resource_type to key off), so when resource_type
  // is genuinely absent this falls back to the same raw_summary phrasing
  // categorize_changes() itself uses to tell them apart server-side.
  { id:'campaign_paused', label:'Campaign paused', defaultOn:true, match:c=>{
      if(c.subcategory!=='Paused') return false;
      if(c.resource_type) return c.resource_type==='CAMPAIGN';
      const s = c.raw_summary||'';
      // Turkish native UI export alternative — mirrors the "^\d+ kampanya
      // duraklat" summary_text_rule (Python) that assigns this subcategory
      // in the first place; keep both in sync if either wording changes.
      return (/campaigns?\s+paused/i.test(s) && !/ad\s*groups?|match\s+keywords?/i.test(s))
        || /^\d+\s*kampanya\s+duraklat/i.test(s);
    },
    // Text-summary rows (no explicit before/after column) don't carry
    // old_value/new_value for this phrasing — fall back to a still-factual
    // statement rather than a fabricated ENABLED->PAUSED that wasn't stated.
    fact:c=> (c.old_value && c.new_value) ? `${c.old_value} → ${c.new_value}` : 'status → PAUSED' },
  { id:'campaign_removed', label:'Campaign removed', defaultOn:true, match:c=> c.subcategory==='Campaign removed',
    fact:c=> `Operation: ${c.operation||'REMOVE'}` },
  { id:'ad_group_removed', label:'Ad group removed', defaultOn:true, match:c=> c.subcategory==='Ad group removed',
    fact:c=> `Operation: ${c.operation||'REMOVE'}` },
  // Default OFF, unlike the other three — "never judge" means pausing isn't
  // inherently more watch-worthy than re-enabling (a dormant campaign coming
  // back can matter just as much); this one's opt-in rather than opt-out so
  // the feature itself doesn't make that call for the user (2026-08-18,
  // 5th audit round — noted, not applied at the time; applied now).
  { id:'campaign_enabled', label:'Campaign enabled', defaultOn:false, match:c=>{
      if(c.subcategory!=='Enabled') return false;
      if(c.resource_type) return c.resource_type==='CAMPAIGN';
      const s = c.raw_summary||'';
      // Turkish native UI export alternative — mirrors the "^\d+ kampanya
      // etkin" summary_text_rule (Python) that assigns this subcategory in
      // the first place; keep both in sync if either wording changes.
      return (/campaigns?\s+active/i.test(s) && !/ad\s*groups?/i.test(s))
        || /^\d+\s*kampanya\s+etkin/i.test(s);
    },
    fact:c=> (c.old_value && c.new_value) ? `${c.old_value} → ${c.new_value}` : 'status → ENABLED' },
];
const ruleState = {
  enabled: false,
  pct: Object.fromEntries(MAGNITUDE_RULES.map(r=>[r.id, r.defaultPct])),
  stateOn: Object.fromEntries(STATE_RULES.map(r=>[r.id, r.defaultOn])),
};
function computeRuleMatches(c){
  const out = [];
  for(const r of MAGNITUDE_RULES){
    if(!r.match(c)) continue;
    const ov = c.old_value_num, nv = c.new_value_num;
    if(ov==null || nv==null || ov===0) continue; // CREATE/REMOVE has no "before" — % change undefined, not zero
    const pctChange = ((nv-ov)/Math.abs(ov))*100;
    const threshold = ruleState.pct[r.id];
    if(Math.abs(pctChange) >= threshold){
      const sign = pctChange>=0 ? '+' : '';
      out.push({ id:r.id, label:r.label, detail:`${sign}${pctChange.toFixed(0)}% · rule: ≥±${threshold}%` });
    }
  }
  for(const r of STATE_RULES){
    if(!ruleState.stateOn[r.id]) continue;
    if(r.match(c)) out.push({ id:r.id, label:r.label, detail:`${r.fact(c)} · rule: ${r.label}` });
  }
  return out;
}
function ruleMatchPillsHtml(matches){
  return matches.map(m=>`<span class="pill pill-rule" title="${escapeHtml(m.detail)}">${escapeHtml(m.label)}</span>`).join(' ');
}
const state = { accounts:new Set(), campaigns:new Set(), users:new Set(), actor:'', categories:new Set(), operations:new Set(), sources:new Set(), ruleMatch:'', range:'all', search:'', page:1, pageSize:200, sortKey:'timestamp_iso', sortDir:-1 };
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
  const fRuleMatch = document.getElementById('fRuleMatch');
  [...MAGNITUDE_RULES, ...STATE_RULES].forEach(r=>{
    const opt = document.createElement('option'); opt.value = r.id; opt.textContent = r.label; fRuleMatch.appendChild(opt);
  });
  fRuleMatch.addEventListener('change', e=>{
    state.ruleMatch = e.target.value; state.page = 1;
    // Selecting an actual rule (not "All changes") is an explicit ask to see
    // rule reasoning — turn the feature on so Explorer's Matches column and
    // the Rule Matches section don't show a filtered-but-unexplained '—'.
    if(state.ruleMatch && !ruleState.enabled){
      ruleState.enabled = true;
      document.getElementById('ruleMatchesToggle').checked = true;
    }
    render();
  });
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
    state.range='all'; state.search=''; state.page=1; state.ruleMatch='';
    document.getElementById('fSearch').value=''; document.getElementById('explorerSearch').value=''; document.getElementById('fActor').value=''; document.getElementById('fRuleMatch').value='';
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
    if(state.ruleMatch){
      const matches = computeRuleMatches(c);
      if(state.ruleMatch==='__matched__'){ if(!matches.length) return false; }
      else if(!matches.some(m=>m.id===state.ruleMatch)) return false;
    }
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
  // Plain tiles: the number is the signal, and no card's colour should read
  // as meaning something (none is "the good one").
  document.getElementById('summaryGrid').innerHTML = stats.map(([l,n])=>
    `<div class="stat"><div class="n">${escapeHtml(n)}</div><div class="l">${l}</div></div>`
  ).join('');
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
    // Magnitude is the bar's height alone, in one colour: a busy day isn't
    // "bad," it's just busier. The first and last day are lighter because
    // they may be partial.
    return `<div class="tl-bar${edge?' edge':''}" style="height:${h}%;"><div class="tip">${d}: ${counts[d]}</div></div>`;
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
    const col = i.color || color || HUES[0];
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
  el.innerHTML = '<div class="grp-label">Human</div><div id="humanBars"></div><div class="grp-label">Automation</div><div id="autoBars"></div>';
  barList(document.getElementById('humanBars'), humanList, label=>{ state.actor='human'; document.getElementById('fActor').value='human'; state.users = state.users.has(label) && state.users.size===1 ? new Set() : new Set([label]); render(); }, HUES[0]);
  barList(document.getElementById('autoBars'), autoList, label=>{
    state.actor = state.actor==='automation' && state.search===label.toLowerCase() ? '' : 'automation';
    document.getElementById('fActor').value = state.actor;
    state.search = state.actor ? label.toLowerCase() : '';
    document.getElementById('fSearch').value = state.actor ? label : '';
    document.getElementById('explorerSearch').value = state.actor ? label : '';
    render();
  }, '#f99c00');
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
    const accCol = HUES[0];
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
  const list = Object.entries(counts).sort((a,b)=>b[1]-a[1]).map(([name,count])=>({name,count,color:CATEGORY_COLORS[name]||'#98918f'}));
  barList(document.getElementById('categoryDist'), list, label=>{ state.categories = state.categories.has(label) && state.categories.size===1 ? new Set() : new Set([label]); render(); });
}
function renderRuleMatches(filtered){
  const countEl = document.getElementById('ruleMatchesCount');
  const el = document.getElementById('ruleMatches');
  if(!ruleState.enabled){
    countEl.textContent = '';
    el.innerHTML = '<div class="empty">Rule-based change highlighting is off.</div>';
    return;
  }
  const matched = filtered.map(c=>({c, matches:computeRuleMatches(c)})).filter(x=>x.matches.length);
  countEl.textContent = `${matched.length.toLocaleString()} of ${filtered.length.toLocaleString()} changes in view match a rule`;
  if(!matched.length){ el.innerHTML = '<div class="empty">No changes match the current rules.</div>'; return; }
  const SHOWN = 500;
  el.innerHTML = `<table><thead><tr><th>Date</th><th>Campaign</th><th>Field</th><th>Matched rule(s)</th></tr></thead><tbody>${
    matched.slice(0, SHOWN).map(({c,matches})=>`<tr class="datarow" data-id="${escapeHtml(c.change_id)}">
      <td>${c.timestamp_iso ? new Date(c.timestamp_iso).toLocaleString() : escapeHtml(c.timestamp)}</td>
      <td>${escapeHtml(c.campaign_name || (c.campaign_id ? `Campaign ${c.campaign_id}` : '—'))}</td>
      <td>${escapeHtml(c.field_name || c.subcategory || '—')}</td>
      <td>${ruleMatchPillsHtml(matches)}</td>
    </tr>`).join('')
  }</tbody></table>${matched.length > SHOWN ? `<div class="note">Showing first ${SHOWN} of ${matched.length} matches (still counted correctly above) — narrow with the top filters to see the rest.</div>` : ''}`;
  el.querySelectorAll('tr.datarow').forEach(row=> row.addEventListener('click', ()=> openDetail(row.dataset.id)));
}
function initRuleMatchesConfig(){
  const magEl = document.getElementById('ruleMatchesMagnitudeInputs');
  magEl.innerHTML = MAGNITUDE_RULES.map(r=>`
    <label class="cfg-field">
      ${escapeHtml(r.label)}
      <span style="display:flex;align-items:center;gap:6px;">±<input type="number" class="ctl" min="1" max="500" step="1" value="${r.defaultPct}" data-rule-id="${r.id}" style="width:70px;">%</span>
    </label>`).join('');
  magEl.querySelectorAll('input[type=number]').forEach(inp=>{
    inp.addEventListener('change', e=>{
      const v = parseFloat(e.target.value);
      ruleState.pct[e.target.dataset.ruleId] = (isFinite(v) && v > 0) ? v : MAGNITUDE_RULES.find(r=>r.id===e.target.dataset.ruleId).defaultPct;
      e.target.value = ruleState.pct[e.target.dataset.ruleId];
      render();
    });
  });
  const stateEl = document.getElementById('ruleMatchesStateInputs');
  stateEl.innerHTML = STATE_RULES.map(r=>`
    <label class="cfg-check">
      <input type="checkbox" ${r.defaultOn ? 'checked' : ''} data-rule-id="${r.id}"> ${escapeHtml(r.label)}
    </label>`).join('');
  stateEl.querySelectorAll('input[type=checkbox]').forEach(cb=>{
    cb.addEventListener('change', e=>{ ruleState.stateOn[e.target.dataset.ruleId] = e.target.checked; render(); });
  });
  document.getElementById('ruleMatchesToggle').addEventListener('change', e=>{ ruleState.enabled = e.target.checked; render(); });
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
  }</tbody></table>`;
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
  function displayValue(raw, display, unit, currency){
    // Fixed 2026-08-18 (3rd audit round, confirmed): `display` reached the
    // DOM unescaped here. Not currently exploitable — the only caller that
    // sets `unit` (the micros-conversion branch) always passes a computed
    // number/null, never raw source text — but escaping it defensively costs
    // nothing and removes the gap for whatever sets old_value_display next.
    // `currency` is only ever the run's own --currency code (validated
    // ^[A-Z]{3}$ before it reaches a row) — escaped anyway, same reasoning.
    const prefix = unit ? `${escapeHtml(display ?? '—')}${currency ? ' ' + escapeHtml(currency) : ''} ` : '';
    const suffix = unit ? `(raw: ${escapeHtml(raw ?? '—')})` : escapeHtml(raw ?? '—');
    return unit ? `${prefix}<span class="note" style="margin:0;">${suffix}</span>` : suffix;
  }
  body.innerHTML = pageRows.length ? pageRows.map(c=>`
    <tr class="datarow" data-id="${escapeHtml(c.change_id)}">
      <td>${c.timestamp_iso ? new Date(c.timestamp_iso).toLocaleString() : escapeHtml(c.timestamp)}</td>
      <td>${c.actor_type==='human' ? escapeHtml(c.user_name||c.user_email||'—') : `<span class="pill pill-auto">${escapeHtml(c.client_type||'automation')}</span>`}</td>
      <td>${escapeHtml(c.account_name||'')}</td><td>${escapeHtml(c.campaign_name||'—')}</td><td>${escapeHtml(c.ad_group_name||'—')}</td>
      <td><span class="pill"${categoryPillStyle(c.category)}>${escapeHtml(c.category)}</span></td><td>${escapeHtml(c.field_name || c.subcategory || '—')}</td>
      <td style="color:var(--bad);">${c.value_confidence==='parsed_from_summary' ? '~' : ''}${displayValue(c.old_value, c.old_value_display, c.value_unit, c.currency)}</td>
      <td style="color:var(--good);">${c.value_confidence==='parsed_from_summary' ? '~' : ''}${displayValue(c.new_value, c.new_value_display, c.value_unit, c.currency)}</td>
      <td>${ruleState.enabled ? (ruleMatchPillsHtml(computeRuleMatches(c)) || '—') : '—'}</td>
      <td>${escapeHtml(c.source)}</td>
    </tr>`).join('') : `<tr><td colspan="11" class="empty">No changes match current filters.</td></tr>`;
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
    ['Category', `${c.category} — ${c.subcategory}${c.category_confidence==='fallback_other' ? ' (no rule matched)' : ''}`], ['Field', c.field_name || '—'],
    ['Previous', prevVal], ['New', newVal],
    ['Operation', c.operation_confidence==='inferred' ? `${c.operation} (inferred, not stated by source)` : c.operation],
    ['Source', c.source], ['Value read from', c.value_confidence==='parsed_from_summary' ? 'summary text (parsed, not a structured field)' : 'structured field'],
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
  renderAccounts(filtered); renderCategories(filtered); renderRuleMatches(filtered);
  renderUntouched(filtered); renderExplorer(filtered);
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
  // What the run read vs. what is shown here — counts only, no verdict.
  const dq = m.data_quality;
  if(dq){
    const n = x => Number(x).toLocaleString();
    const lim = dq.in_report_with_limits;
    const flags = [['category_other','categorized "Other"'],['timezone_unknown','no timezone'],['operation_inferred','operation inferred'],
      ['value_parsed_from_summary','value read from summary text'],['multi_field_unsplit','several fields in one row']]
      .filter(([k])=>lim[k]>0).map(([k,label])=>`${n(lim[k])} ${label}`);
    document.getElementById('dataQuality').textContent =
      `Input: ${n(dq.input_rows)} rows · in this report: ${n(dq.rows_in_report)} · left out: ${n(dq.left_out.unreadable_date)} unreadable date, ${n(dq.left_out.duplicate)} duplicate`
      + (flags.length ? ` · of the rows shown: ${flags.join(', ')}` : '')
      + (dq.ignored_headers.length ? ` · source columns not used: ${dq.ignored_headers.join(', ')}` : '');
  }
  const tzUnknown = m.rows_excluded_from_timeline_unknown_tz || 0;
  if(m.row_count && tzUnknown / m.row_count > 0.05){
    caveats.push(`${tzUnknown.toLocaleString()} row(s) (${(100*tzUnknown/m.row_count).toFixed(1)}%) carry a timestamp with no timezone and are left out of the Activity Timeline — every other view still includes them. Re-run with --timezone <IANA name> if those timestamps share one known zone.`);
  }
  if(caveats.length){
    document.getElementById('apiCaveat').innerHTML = caveats.map(c=>`<div class="warn-box">${escapeHtml(c)}</div>`).join('');
  }
  initFilters();
  initRuleMatchesConfig();
  document.getElementById('threshBtns').addEventListener('click', e=>{
    if(e.target.tagName!=='BUTTON') return;
    [...e.target.parentElement.children].forEach(b=>b.classList.remove('active'));
    e.target.classList.add('active'); renderUntouched(getFiltered());
  });
  document.getElementById('pagerPrev').addEventListener('click', ()=>{ state.page--; renderExplorer(getFiltered()); });
  document.getElementById('pagerNext').addEventListener('click', ()=>{ state.page++; renderExplorer(getFiltered()); });
  document.querySelectorAll('#explorerTable th').forEach(th=> th.addEventListener('click', ()=>{
    if(!th.dataset.key) return; // e.g. the "Matches" column — a derived list, not a sortable field
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


def pack_changes(rows):
    """Column-oriented form of the canonical rows, for embedding in the
    dashboard. Added 2026-10-10: the rows were embedded as a plain JSON
    array of objects — 42 key names repeated on every row, and the same few
    account/campaign/category/source strings repeated thousands of times.
    25,000 rows made a 33 MB HTML file. Here each key appears once, and a
    column whose values repeat is stored as a table of its distinct values
    plus one small index per row; a column of mostly-unique values (ids,
    timestamps) is stored as-is. Same data, about a quarter of the bytes;
    unpackChanges() in the template rebuilds the identical row objects, so
    nothing else in the dashboard knows the difference. changes.jsonl and
    change_history.json keep the plain row form."""
    keys = list(dict.fromkeys(k for r in rows for k in r))
    cols = {}
    for k in keys:
        table, index, per_row = [], {}, []
        for r in rows:
            v = r.get(k)
            # type name in the key: 1, 1.0 and True are equal as dict keys
            ident = ("json", json.dumps(v, sort_keys=True)) if isinstance(v, (list, dict)) else (type(v).__name__, v)
            i = index.get(ident)
            if i is None:
                i = index[ident] = len(table)
                table.append(v)
            per_row.append(i)
        cols[k] = {"d": table, "i": per_row} if len(table) * 2 <= len(rows) else {"v": [r.get(k) for r in rows]}
    return {"n": len(rows), "keys": keys, "cols": cols}


def build_dashboard(rows, out_path, mask_users=False, generated_at=None, json_out=None, rows_skipped_unparseable_date=0, data_quality=None):
    sources = {r.get("source", "unknown") for r in rows}
    other_pct = round(100 * sum(1 for r in rows if r["category"] == "Other") / len(rows), 1) if rows else 0.0
    generated_at = generated_at or datetime.now().isoformat()
    if len(rows) > SCALE_WARN_THRESHOLD:
        print(f"WARNING: {len(rows)} rows exceeds the {SCALE_WARN_THRESHOLD} single-file guideline. Consider a date-range split.")
    data = build_aggregation(rows, sources, other_pct, mask_users, generated_at, date_parse_stats={"rows_skipped_unparseable_date": rows_skipped_unparseable_date})
    if data_quality is not None:
        data["meta"]["data_quality"] = data_quality
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
    embedded = {k: v for k, v in data.items() if k != "changes"}
    embedded["changes_packed"] = pack_changes(data["changes"])
    payload = json.dumps(embedded, ensure_ascii=False, separators=(",", ":"))
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
        # Fixed 2026-08-18 (4th audit round, confirmed): int(since.rstrip("dwm"))
        # crashed with a raw ValueError on garbage ("abc", "7x") and silently
        # accepted a negative count ("-7d") — rstrip only strips the trailing
        # unit letter, it doesn't validate what's in front of it, so "-7"
        # parsed as int(-7) without complaint and produced a cutoff in the
        # future instead of the past, silently returning wrong (usually
        # empty) results rather than erroring.
        if not SINCE_RE.match(since):
            raise ValueError(f"Invalid --since value: {since!r}. Expected a positive number followed by d/w/m, e.g. '7d', '2w', '1m'.")
        n = int(since[:-1])
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
                  extra_rules=None, open_browser=False, generated_at=None, account_name=None, currency=None, column_map=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if currency is not None and not CURRENCY_CODE_RE.match(currency):
        return {"status": "error", "message": f"Invalid --currency value: {currency!r}. Expected a 3-letter ISO 4217 code in capitals, e.g. 'TRY', 'USD', 'EUR'."}

    if extra_rules is not None:
        err = validate_extra_rules(extra_rules)
        if err:
            return {"status": "error", "message": err}

    v = validate_source(input_path, mapping_file=mapping_file, force=force_review, default_account_name=account_name, column_map=column_map)
    if v["status"] != "ok":
        return v
    (out_dir / "resolved_mapping.json").write_text(json.dumps(v, ensure_ascii=False), encoding="utf-8")

    n = normalize_changes(input_path, v["mapping"], v["source_label"], date_format=date_format, decimal_style=decimal_style, tz=tz, default_account_name=account_name)
    if n["status"] != "ok":
        return n

    # Fixed 2026-08-18 (4th audit round, confirmed): any unparseable-date
    # rate, even 90%+, previously still returned status "ok" — just a
    # WARNING line buried in coverage.txt that nothing forces a reader to
    # see. Past a threshold this high, the date column itself is probably
    # mismatched (wrong --date-format, or genuinely the wrong column), not a
    # handful of bad rows — the same "stop and confirm" gate every other
    # genuine ambiguity in this pipeline already gets, not silent output
    # built from a fraction of the real dataset presented as the whole
    # thing. 20% is a judgment call, not a measured constant; --force-review
    # (the same flag the >30%-empty-required-field gate already uses)
    # bypasses it once a human has actually looked and confirmed the rate is
    # real.
    input_total = len(n["rows"]) + n["rows_skipped_unparseable_date"]
    date_skip_pct = round(100 * n["rows_skipped_unparseable_date"] / input_total, 1) if input_total else 0.0
    if date_skip_pct > 20 and not force_review:
        return {
            "status": "needs_date_review",
            "message": f"{n['rows_skipped_unparseable_date']} of {input_total} rows ({date_skip_pct}%) could not be parsed as dates under the resolved format ({n['date_format_used']}). This is high enough that the date column or --date-format is probably wrong, not just a few bad rows.",
            "rows_skipped_unparseable_date": n["rows_skipped_unparseable_date"],
            "rows_skipped_unparseable_date_pct": date_skip_pct,
            "date_format_used": n["date_format_used"],
            "instructions": "Confirm with the user whether this rate is genuinely expected (a source with a lot of malformed dates) or the wrong --date-format was resolved/passed. If it's real, re-run with --force-review.",
        }

    deduped_rows, duplicate_count = dedupe_rows(n["rows"])
    n["rows"] = deduped_rows

    c = categorize_changes(n["rows"], allow_unknown=allow_unknown_categories, extra_rules=extra_rules)
    if c["status"] != "ok":
        (out_dir / "unknown-fields.json").write_text(json.dumps(c, indent=2, ensure_ascii=False), encoding="utf-8")
        return c

    # Fixed 2026-08-18 (audit F-04): masked BEFORE any output file is written,
    # so changes.jsonl and the dashboard are never inconsistent and neither
    # can leak an identity --mask-users was asked to hide.
    if currency:
        apply_currency(c["rows"], currency)
    c["rows"] = mask_rows(c["rows"], mask_users)

    # Fixed 2026-10-10 (6th audit round): a naive timestamp with no
    # --timezone can't be placed on the UTC timeline, so the row is left out
    # of it (correctly — no zone is guessed). But the only trace of that was
    # a footnote under the timeline chart: on a 25k-row export 17.7% of rows
    # were missing from it while this function's own summary said nothing.
    tz_unknown = sum(1 for row in c["rows"] if not row.get("timestamp_utc"))

    with open(out_dir / "changes.jsonl", "w", encoding="utf-8") as f:
        for row in c["rows"]:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    # Added 2026-10-10. status "ok" means nothing was ambiguous — not that
    # every input row is in the report, or that every row in it is fully
    # usable. The individual counts existed (some only in coverage.txt, some
    # only in the result, one only as a dashboard footnote); this is the one
    # place that states all of them, and they add up:
    #   input_rows + rows_added_by_field_split - left_out = rows_in_report
    # "in_report_with_limits" rows ARE in the report; each count names one
    # way a row is less than complete. Nothing here is a verdict on the data
    # — no threshold, no pass/fail. The existing stop-and-ask gates (>20%
    # unreadable dates, >30% empty required column) are unchanged.
    data_quality = {
        "input_rows": n["input_rows"],
        "rows_added_by_field_split": n["rows_added_by_field_split"],
        "left_out": {"unreadable_date": n["rows_skipped_unparseable_date"], "duplicate": duplicate_count},
        "rows_in_report": len(c["rows"]),
        "in_report_with_limits": {
            "category_other": sum(1 for row in c["rows"] if row.get("category") == "Other"),
            "timezone_unknown": tz_unknown,
            "operation_inferred": sum(1 for row in c["rows"] if row.get("operation_confidence") == "inferred"),
            "value_parsed_from_summary": sum(1 for row in c["rows"] if row.get("value_confidence") == "parsed_from_summary"),
            "multi_field_unsplit": sum(1 for row in c["rows"] if row.get("multi_field")),
            "name_without_reference_dropped": n["orphan_names_dropped"],
        },
        "ignored_headers": v["ignored_headers"],
    }
    with open(out_dir / "coverage.txt", "w", encoding="utf-8") as f:
        dq_left = data_quality["left_out"]
        f.write(f"Input rows: {data_quality['input_rows']}\n")
        if data_quality["rows_added_by_field_split"]:
            f.write(f"Rows added by splitting multi-field events: {data_quality['rows_added_by_field_split']}\n")
        f.write(f"Left out: {sum(dq_left.values())} (unreadable date: {dq_left['unreadable_date']}, duplicate: {dq_left['duplicate']})\n")
        f.write(f"In the report: {data_quality['rows_in_report']}\n")
        f.write("In the report, with a limit: " + ", ".join(f"{k.replace('_', ' ')}: {val}" for k, val in data_quality["in_report_with_limits"].items()) + "\n")
        f.write(f"Source columns not used: {', '.join(data_quality['ignored_headers']) or 'none'}\n\n")
        f.write(f"Total rows: {len(c['rows'])}\n")
        if duplicate_count:
            f.write(f"Duplicates removed (identical change_id): {duplicate_count}\n")
        if n["rows_skipped_unparseable_date"]:
            input_total = len(c["rows"]) + n["rows_skipped_unparseable_date"]
            skip_pct = round(100 * n["rows_skipped_unparseable_date"] / input_total, 1) if input_total else 0.0
            f.write(f"Rows skipped (unparseable date): {n['rows_skipped_unparseable_date']} ({skip_pct}%)\n")
            if skip_pct > 5:
                f.write("WARNING: unparseable-date skip rate exceeds 5% — check the source's date column for a format this run didn't expect.\n")
        if n["orphan_names_dropped"]:
            f.write(f"Campaign/ad group names ignored on rows without a matching reference: {n['orphan_names_dropped']} — the source's own campaign/ad_group column is empty on those rows, so the name can't say which entity the row belongs to.\n")
        if tz_unknown:
            f.write(f"Rows with unknown timezone: {tz_unknown} ({round(100 * tz_unknown / len(c['rows']), 1)}%) — excluded from the timeline; pass --timezone <IANA name> if the source's naive timestamps share one known zone.\n")
        f.write(f"Other: {c['other_pct']}%\n")
        if c["other_pct"] > 10:
            f.write("WARNING: Other exceeds 10% — check for a structured_rules gap.\n")
        f.write("\nCategory breakdown:\n")
        for cat, cnt in Counter({k: v2 for k, v2 in c["category_breakdown"].items()}).most_common():
            f.write(f"  {cat}: {cnt}\n")

    data = build_dashboard(c["rows"], out_dir / "dashboard.html", mask_users=mask_users, generated_at=generated_at,
                            json_out=out_dir / "change_history.json", rows_skipped_unparseable_date=n["rows_skipped_unparseable_date"],
                            data_quality=data_quality)

    result = {
        "status": "ok", "out": str(out_dir / "dashboard.html"), "row_count": len(c["rows"]),
        "other_pct": c["other_pct"], "accounts": len(data["accounts"]),
        "human_users": len(data["users"]["human"]), "automation_actors": len(data["users"]["automation"]),
        "rows_skipped_unparseable_date": n["rows_skipped_unparseable_date"],
        "rows_timezone_unknown": tz_unknown,
        "orphan_names_dropped": n["orphan_names_dropped"],
        "duplicates_removed": duplicate_count,
        "date_format_used": n["date_format_used"], "decimal_style_used": n["decimal_style_used"],
        "ignored_headers": v["ignored_headers"],
        "data_quality": data_quality,
    }
    if open_browser:
        webbrowser.open(f"file://{(out_dir / 'dashboard.html').resolve()}")
    return result


# =====================================================================
# SELF-TEST
# This is the test suite — no external framework. Each case is a
# `with _test_case("what is being proven"):` block: a small synthetic
# fixture, a real run_pipeline()/validate_source() call, assertions whose
# messages name what would break, and a [PASS] line.
# =====================================================================
_SELF_TEST_FAILURES = []


class _test_case:
    """Wraps one test case: an exception inside the `with` block is recorded
    as a failure of that case instead of ending the run."""

    def __init__(self, label):
        self.label = label

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None or not issubclass(exc_type, Exception):
            return False
        import traceback
        line = traceback.extract_tb(tb)[-1].lineno
        detail = (str(exc).strip().splitlines() or [""])[0] or exc_type.__name__
        _SELF_TEST_FAILURES.append(self.label)
        print(f"[FAIL] {self.label}\n       line {line}: {exc_type.__name__}: {detail[:300]}")
        return True


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

# Rule Matches (MAGNITUDE_RULES/STATE_RULES/computeRuleMatches/getFiltered's
# rule-match filter) is dashboard-template client JS, not Python — self_test()
# can't reach it via run_pipeline()/validate_source() like everything else in
# this file. Extracted straight from DASHBOARD_TEMPLATE and run under Node so
# it can never silently drift from what actually ships. Node-only, and
# skipped (not failed) when Node isn't on PATH — the tool itself still needs
# nothing beyond the Python standard library to run; this one regression
# check is the sole exception, and it's opt-in by environment, not required.
_JS_RULE_MATCHES_TEST_SUITE = r"""
const assert = require('assert');
let passed = 0, failures = 0;
function check(desc, fn){
  try { fn(); passed++; }
  catch(e){ failures++; console.error('FAIL: ' + desc + '\n  ' + e.message); }
}
function mkRow(over){
  return Object.assign({
    subcategory: null, resource_type: null, operation: null,
    old_value_num: null, new_value_num: null, old_value: null, new_value: null,
    raw_summary: '',
  }, over);
}

check('budget +49% under the 50% threshold does not match', () => {
  ruleState.pct.budget_magnitude = 50;
  const r = mkRow({ subcategory: 'Budget changed', old_value_num: 100, new_value_num: 149 });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'budget_magnitude'), false);
});
check('budget +50% at a 50% threshold matches, detail shows the exact number', () => {
  ruleState.pct.budget_magnitude = 50;
  const r = mkRow({ subcategory: 'Budget changed', old_value_num: 100, new_value_num: 150 });
  const m = computeRuleMatches(r).find(m => m.id === 'budget_magnitude');
  assert.ok(m, 'expected a match');
  assert.ok(m.detail.includes('+50%'), 'detail was: ' + m.detail);
});
check('budget -50% (a decrease) at a 50% threshold matches — symmetric on sign', () => {
  ruleState.pct.budget_magnitude = 50;
  const r = mkRow({ subcategory: 'Budget changed', old_value_num: 100, new_value_num: 50 });
  assert.ok(computeRuleMatches(r).some(m => m.id === 'budget_magnitude'));
});
check('old_value_num 0 never matches (percent-of-zero is undefined, not treated as +Infinity%)', () => {
  ruleState.pct.budget_magnitude = 50;
  const r = mkRow({ subcategory: 'Budget changed', old_value_num: 0, new_value_num: 100 });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'budget_magnitude'), false);
});
check('a CREATE (subcategory "Budget created", not "Budget changed") never hits the magnitude rule', () => {
  const r = mkRow({ subcategory: 'Budget created', new_value_num: 100000 });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'budget_magnitude'), false);
});
check('campaign ENABLED->PAUSED (structured, resource_type present) matches campaign_paused', () => {
  const r = mkRow({ subcategory: 'Paused', resource_type: 'CAMPAIGN', old_value: 'ENABLED', new_value: 'PAUSED' });
  const m = computeRuleMatches(r).find(m => m.id === 'campaign_paused');
  assert.ok(m, 'expected a match');
  assert.ok(m.detail.includes('ENABLED → PAUSED'), 'detail was: ' + m.detail);
});
check('campaign PAUSED->ENABLED (subcategory "Enabled") never matches campaign_paused — intentional asymmetry', () => {
  const r = mkRow({ subcategory: 'Enabled', resource_type: 'CAMPAIGN', old_value: 'PAUSED', new_value: 'ENABLED' });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'campaign_paused'), false);
});
check('campaign_enabled defaults to OFF, unlike the other three state rules', () => {
  assert.strictEqual(ruleState.stateOn.campaign_enabled, false);
  assert.strictEqual(ruleState.stateOn.campaign_paused, true);
  assert.strictEqual(ruleState.stateOn.campaign_removed, true);
  assert.strictEqual(ruleState.stateOn.ad_group_removed, true);
});
check('campaign PAUSED->ENABLED matches campaign_enabled once turned on, with a factual detail', () => {
  ruleState.stateOn.campaign_enabled = true;
  const r = mkRow({ subcategory: 'Enabled', resource_type: 'CAMPAIGN', old_value: 'PAUSED', new_value: 'ENABLED' });
  const m = computeRuleMatches(r).find(m => m.id === 'campaign_enabled');
  assert.ok(m, 'expected a match');
  assert.ok(m.detail.includes('PAUSED → ENABLED'), 'detail was: ' + m.detail);
});
check('campaign_enabled stays off by default even when a row would otherwise match', () => {
  ruleState.stateOn.campaign_enabled = false;
  const r = mkRow({ subcategory: 'Enabled', resource_type: 'CAMPAIGN', old_value: 'PAUSED', new_value: 'ENABLED' });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'campaign_enabled'), false);
  ruleState.stateOn.campaign_enabled = true; // leave on for the remaining checks below
});
check('text-summary "N campaigns active" matches campaign_enabled', () => {
  const r = mkRow({ subcategory: 'Enabled', raw_summary: '2 campaigns active' });
  assert.ok(computeRuleMatches(r).some(m => m.id === 'campaign_enabled'));
});
check('text-summary "N ad groups active" does NOT match campaign_enabled', () => {
  const r = mkRow({ subcategory: 'Enabled', raw_summary: '2 ad groups active' });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'campaign_enabled'), false);
});
check('campaign REMOVE matches campaign_removed with an "Operation: REMOVE" detail', () => {
  const r = mkRow({ subcategory: 'Campaign removed', operation: 'REMOVE' });
  const m = computeRuleMatches(r).find(m => m.id === 'campaign_removed');
  assert.ok(m, 'expected a match');
  assert.ok(m.detail.includes('Operation: REMOVE'), 'detail was: ' + m.detail);
});
check('ad group REMOVE matches ad_group_removed', () => {
  const r = mkRow({ subcategory: 'Ad group removed', operation: 'REMOVE' });
  assert.ok(computeRuleMatches(r).some(m => m.id === 'ad_group_removed'));
});
check('text-summary "N campaigns paused" (no resource_type column) matches campaign_paused', () => {
  const r = mkRow({ subcategory: 'Paused', raw_summary: '2 campaigns paused' });
  assert.ok(computeRuleMatches(r).some(m => m.id === 'campaign_paused'));
});
check('text-summary "N ad groups paused" does NOT match campaign_paused', () => {
  const r = mkRow({ subcategory: 'Paused', raw_summary: '2 ad groups paused' });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'campaign_paused'), false);
});
check('text-summary "N broad match keywords paused" does NOT match campaign_paused', () => {
  const r = mkRow({ subcategory: 'Paused', raw_summary: '5 broad match keywords paused' });
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'campaign_paused'), false);
});
check('lowering the threshold live picks up a match that was excluded a moment ago', () => {
  const r = mkRow({ subcategory: 'Budget changed', old_value_num: 100, new_value_num: 120 });
  ruleState.pct.budget_magnitude = 50;
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'budget_magnitude'), false);
  ruleState.pct.budget_magnitude = 10;
  assert.ok(computeRuleMatches(r).some(m => m.id === 'budget_magnitude'));
  ruleState.pct.budget_magnitude = 50;
});
check('unchecking a state rule\'s own box excludes it even though the row still matches structurally', () => {
  const r = mkRow({ subcategory: 'Campaign removed', operation: 'REMOVE' });
  ruleState.stateOn.campaign_removed = false;
  assert.strictEqual(computeRuleMatches(r).some(m => m.id === 'campaign_removed'), false);
  ruleState.stateOn.campaign_removed = true;
});
check('getFiltered(): "Matched a rule" keeps only rows with >=1 match at current thresholds', () => {
  DASH_DATA = { changes: [
    mkRow({ change_id: 'a', subcategory: 'Budget changed', old_value_num: 100, new_value_num: 200 }),
    mkRow({ change_id: 'b', subcategory: 'Budget changed', old_value_num: 100, new_value_num: 110 }),
  ], meta: {} };
  state.ruleMatch = '__matched__';
  assert.deepStrictEqual(getFiltered().map(c => c.change_id), ['a']);
  state.ruleMatch = '';
});
check('getFiltered(): filtering by one specific rule id excludes rows matching a different rule', () => {
  DASH_DATA = { changes: [
    mkRow({ change_id: 'budget-hit', subcategory: 'Budget changed', old_value_num: 100, new_value_num: 200 }),
    mkRow({ change_id: 'removed-hit', subcategory: 'Campaign removed', operation: 'REMOVE' }),
  ], meta: {} };
  state.ruleMatch = 'campaign_removed';
  assert.deepStrictEqual(getFiltered().map(c => c.change_id), ['removed-hit']);
  state.ruleMatch = '';
});

if (failures > 0) {
  console.error(failures + ' of ' + (passed + failures) + ' Rule Matches JS assertions failed');
  process.exit(1);
}
console.log(passed + ' Rule Matches JS assertions passed');
"""


def _run_js_rule_matches_regression():
    import shutil
    import subprocess
    if not shutil.which("node"):
        print("[SKIP] Rule Matches JS regression suite (Node not found on PATH — install Node to run this check; not required to use the tool)")
        return
    match = re.search(r"<script>(.*?)</script>", DASHBOARD_TEMPLATE, re.S)
    assert match, "DASHBOARD_TEMPLATE has no <script> block to extract"
    # DASH_DATA ships as `const` (never reassigned in the real dashboard —
    # it's baked in at generation time); the test suite needs to reassign it
    # per-check, so relax it to `let` only inside this harness.
    body = match.group(1).replace(
        "const DASH_DATA = /*__DASH_DATA__*/null;", "let DASH_DATA = null;", 1
    )
    assert "let DASH_DATA = null;" in body, "DASH_DATA declaration line not found — DASHBOARD_TEMPLATE's opening const may have changed shape"
    harness = "global.document = { addEventListener: function(){} };\n" + body + "\n" + _JS_RULE_MATCHES_TEST_SUITE
    proc = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=15)
    assert proc.returncode == 0, f"Rule Matches JS regression suite failed:\n{proc.stdout}\n{proc.stderr}"
    print(f"[PASS] Rule Matches JS regression suite (Node): {proc.stdout.strip()}")


# Fixture for the 6th audit round (2026-10-10): the ChangeEvent header set
# plus the three display-name columns a joined export carries next to it
# (account_name/campaign_name/ad_group_name) and one column no alias covers
# ("feed"). Row 1 is deliberately the awkward one — naive timestamp, empty
# user_email, empty ad_group, and a free-text value with a digit in it.
def _r6_row(n, ts, rtype, rpath, op, field, old, new, email="user.a@example.test", client="GOOGLE_ADS_WEB_CLIENT", ad_group="customers/9990001/adGroups/3001"):
    return {
        "change_date_time": ts, "customer_id": "9990001",
        "resource_name": f"customers/9990001/changeEvents/178565760000000{n}~{n}~0",
        "user_email": email, "client_type": client, "change_resource_type": rtype,
        "change_resource_name": f"customers/9990001/{rpath}", "resource_change_operation": op,
        "changed_fields": field, "old_resource": old, "new_resource": new,
        "campaign": "customers/9990001/campaigns/2001", "ad_group": ad_group, "feed": "",
        "account_name": "Account A", "campaign_name": "Campaign Alpha", "ad_group_name": "Ad Group 1" if ad_group else "",
    }


SAMPLE_API_WITH_NAMES_ROWS = [
    _r6_row(0, "2026-08-01T10:00:00+03:00", "AD_GROUP_AD", "adGroupAds/3001~4001", "UPDATE", "status", "ENABLED", "PAUSED"),
    _r6_row(1, "2026-08-02 09:12:00", "CAMPAIGN", "campaigns/2001", "UPDATE", "name", "Search | Generic | v1", "Search | Generic | Core", email="", client="GOOGLE_ADS_SCRIPTS", ad_group=""),
    _r6_row(2, "2026-08-03T11:00:00+03:00", "CAMPAIGN_BUDGET", "campaignBudgets/1001", "UPDATE", "amount_micros", "50000000", "75000000", ad_group=""),
    _r6_row(3, "2026-08-04T12:00:00+03:00", "AD_GROUP_AD", "adGroupAds/3001~4002", "CREATE", "status", "", "ENABLED"),
]


def _structured_rule_shadows(earlier, later):
    """True when every row `later` can match is already matched by `earlier`
    — i.e. `later` is unreachable under first-match-wins. Conservative: only
    reports a shadow it can prove from the rule dicts alone."""
    def types(rule):
        if "resource_type" in rule:
            return {rule["resource_type"]}
        return set(rule["resource_type_in"]) if "resource_type_in" in rule else None
    te, tl = types(earlier), types(later)
    if te is not None and (tl is None or not tl <= te):
        return False
    if "resource_type_contains" in earlier:
        needle = earlier["resource_type_contains"].upper()
        if tl is not None:
            if not all(needle in t for t in tl):
                return False
        elif needle not in later.get("resource_type_contains", "").upper():
            return False
    def needles(rule):
        n = rule.get("field_name_contains", [])
        return [n.lower()] if isinstance(n, str) else [x.lower() for x in n]
    if not all(any(e in l for l in needles(later)) for e in needles(earlier)):
        return False
    if "field_name_not_contains" in earlier and earlier["field_name_not_contains"] != later.get("field_name_not_contains"):
        return False
    return all(earlier[k] == later.get(k) for k in ("operation", "new_value_equals") if k in earlier)


def _run_round6_regressions(td):
    """6th audit round (2026-10-10) — found by cross-checking changes.jsonl
    against a 25k-row synthetic export row by row. Every case below produced
    status "ok" with wrong or silently-missing output before its fix."""
    with _test_case('literal account_name/campaign_name/ad_group_name columns are mapped and reach the output (previ…'):
        f = td / "api_with_names.json"
        f.write_text(json.dumps(SAMPLE_API_WITH_NAMES_ROWS), encoding="utf-8")
        headers, _ = read_rows(f)
        profile_path = PROFILES_DIR / f"{fingerprint(headers)}.json"

        v = validate_source(f)
        assert v["status"] == "ok", f"round6 fixture failed validation: {v}"
        for canonical in ("account_name", "campaign_name", "ad_group_name"):
            assert v["mapping"].get(canonical) == canonical, f"literal '{canonical}' column not mapped: {v['mapping']}"
        r = run_pipeline(f, td / "out_r6", generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok", f"round6 fixture failed: {r}"
        with open(td / "out_r6" / "changes.jsonl", encoding="utf-8") as fh:
            rows = {(x["resource_type"], x["operation"]): x for x in (json.loads(line) for line in fh)}
        ad_pause, camp_rename, budget, ad_create = rows[("AD_GROUP_AD", "UPDATE")], rows[("CAMPAIGN", "UPDATE")], rows[("CAMPAIGN_BUDGET", "UPDATE")], rows[("AD_GROUP_AD", "CREATE")]
        assert (ad_pause["account_name"], ad_pause["campaign_name"], ad_pause["ad_group_name"]) == ("Account A", "Campaign Alpha", "Ad Group 1"), f"display names lost: {ad_pause}"
        print("[PASS] literal account_name/campaign_name/ad_group_name columns are mapped and reach the output (previously dropped: every row showed 'Campaign <id>')")

    with _test_case('validate_source: source columns no alias matched are listed in ignored_headers instead of vanis…'):
        assert v.get("ignored_headers") == ["feed"], f"unused source column not reported: {v.get('ignored_headers')}"
        print("[PASS] validate_source: source columns no alias matched are listed in ignored_headers instead of vanishing without a trace")

    with _test_case('mapping profiles: only human-confirmed mappings are cached; a stale auto-derived profile no lon…'):
        # A profile auto-derived from the alias table froze that table's state
        # at the time: a later alias fix never reached a file seen once before.
        assert not profile_path.exists(), "an alias-matched source must not be cached as a mapping profile"
        PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        stale = {k: h for k, h in v["mapping"].items() if not k.endswith("_name")}
        profile_path.write_text(json.dumps({"source_label": "google_ads_api_change_event_json", "mapping": stale, "headers_seen": headers}), encoding="utf-8")
        v_stale = validate_source(f)
        assert v_stale["mapping"].get("campaign_name") == "campaign_name", f"stale auto-derived profile shadowed the alias table: {v_stale['mapping']}"
        profile_path.unlink()
        print("[PASS] mapping profiles: only human-confirmed mappings are cached; a stale auto-derived profile no longer shadows the alias table")

    with _test_case("soft_warnings: 'No campaign reference column' no longer fires when campaign_resource is mapped"):
        base = {"timestamp": "t", "user_email": "u", "operation": "o"}
        assert not any("No campaign reference" in w for w in soft_warnings({**base, "campaign_resource": "campaign"})), "campaign_resource alone is a campaign reference"
        assert any("No campaign reference" in w for w in soft_warnings(base)), "warning must still fire with no campaign column at all"
        print("[PASS] soft_warnings: 'No campaign reference column' no longer fires when campaign_resource is mapped")

    with _test_case("categorize: pausing/enabling an ad is Status/Paused|Enabled, not 'Ad changed' (the generic AD U…"):
        assert (ad_pause["category"], ad_pause["subcategory"]) == ("Status", "Paused"), f"AD_GROUP_AD status change miscategorized: {ad_pause['category']}/{ad_pause['subcategory']}"
        assert (ad_create["category"], ad_create["subcategory"]) == ("Ad", "Ad created"), f"AD_GROUP_AD CREATE must stay 'Ad created': {ad_create['category']}/{ad_create['subcategory']}"
        print("[PASS] categorize: pausing/enabling an ad is Status/Paused|Enabled, not 'Ad changed' (the generic AD UPDATE rule used to win first)")

    with _test_case('structured_rules: FEED/FEED_ITEM removals reach their own category, and no rule is fully shadow…'):
        assert match_structured({"resource_type": "FEED", "field_name": "name", "operation": "REMOVE"}) == ("Feed", "Feed removed")
        assert match_structured({"resource_type": "FEED_ITEM", "field_name": "name", "operation": "REMOVE"}) == ("Feed", "Feed item removed")
        rules = CATEGORY_RULES["structured_rules"]
        shadowed = [(i, j) for j in range(len(rules)) for i in range(j) if _structured_rule_shadows(rules[i], rules[j])]
        assert not shadowed, f"unreachable structured_rules (earlier index fully shadows later): {shadowed}"
        print("[PASS] structured_rules: FEED/FEED_ITEM removals reach their own category, and no rule is fully shadowed by an earlier one")

    with _test_case("normalize_number: only a whole-value number (optional currency/unit) parses — 'Search | Generic…"):
        for text in ("Search | Generic | v1", "AG | Generic | 01", "feed/100", "v1", "customers/1/campaigns/456", "10:30", "2026-08-01", "Campaign 7"):
            assert normalize_number(text, "TR") is None, f"free text parsed as a number: {text!r} -> {normalize_number(text, 'TR')}"
        for raw, style, expected in (("50000000", "TR", 50000000.0), ("3.2", "TR", 3.2), ("150.000", "TR", 150000.0), ("3,50", "TR", 3.5),
                                     ("₺150,00", "TR", 150.0), ("150.000 TL", "TR", 150000.0), ("%20", "TR", 20.0), ("20%", "TR", 20.0),
                                     ("$1,234.56", "US", 1234.56), ("TRY100.00/day", "US", 100.0), ("5km", "TR", 5.0), ("-3,50", "TR", -3.5)):
            assert normalize_number(raw, style) == expected, f"genuine number no longer parsed: {raw!r} -> {normalize_number(raw, style)}"
        assert camp_rename["old_value_num"] is None and camp_rename["old_value_display"] is None, f"campaign name got a numeric value: {camp_rename}"
        assert budget["old_value_display"] == 50.0, f"micros amount no longer converted: {budget}"
        print("[PASS] normalize_number: only a whole-value number (optional currency/unit) parses — 'Search | Generic | v1' is no longer 1.0")

    with _test_case('needs_date_format/needs_decimal_style: sample_values are the ambiguous values themselves, not t…'):
        mixed = [dict(row) for row in SAMPLE_API_WITH_NAMES_ROWS]
        mixed[3]["change_date_time"] = "03/04/2026 10:00:00"
        fm = td / "api_mixed_dates.json"
        fm.write_text(json.dumps(mixed), encoding="utf-8")
        n_ask = normalize_changes(fm, v["mapping"], v["source_label"])
        assert n_ask["status"] == "needs_date_format", f"an ambiguous slash date must still stop the pipeline: {n_ask['status']}"
        assert n_ask["sample_values"] == ["03/04/2026 10:00:00"], f"needs_date_format must show the ambiguous values, got: {n_ask['sample_values']}"
        mixed_dec = [dict(row) for row in SAMPLE_API_WITH_NAMES_ROWS]
        mixed_dec[3]["new_resource"] = "150.000"
        fd = td / "api_ambiguous_decimal.json"
        fd.write_text(json.dumps(mixed_dec), encoding="utf-8")
        n_dec = normalize_changes(fd, v["mapping"], v["source_label"])
        assert n_dec["status"] == "needs_decimal_style" and n_dec["sample_values"] == ["150.000"], f"needs_decimal_style must show the ambiguous values, got: {n_dec}"
        print("[PASS] needs_date_format/needs_decimal_style: sample_values are the ambiguous values themselves, not the first 5 rows")

    with _test_case('parse_timestamp: year-first ISO rows still parse under --date-format DMY/MDY (a mixed file used…'):
        n_dmy = normalize_changes(fm, v["mapping"], v["source_label"], date_format="DMY")
        assert n_dmy["status"] == "ok" and n_dmy["rows_skipped_unparseable_date"] == 0 and len(n_dmy["rows"]) == 4, f"mixed ISO + DD/MM file lost rows under DMY: {n_dmy.get('rows_skipped_unparseable_date')} skipped"
        assert sorted(x["timestamp_iso"] for x in n_dmy["rows"])[-1] == "2026-08-03T11:00:00" and any(x["timestamp_iso"] == "2026-04-03T10:00:00" for x in n_dmy["rows"])
        print("[PASS] parse_timestamp: year-first ISO rows still parse under --date-format DMY/MDY (a mixed file used to lose one format or the other)")

    with _test_case('run summary + coverage.txt report rows excluded from the timeline for having no timezone, and n…'):
        assert r.get("rows_timezone_unknown") == 1, f"run summary must count rows with no resolvable timezone: {r.get('rows_timezone_unknown')}"
        coverage = (td / "out_r6" / "coverage.txt").read_text(encoding="utf-8")
        assert "Rows with unknown timezone: 1 (25.0%)" in coverage and "--timezone" in coverage, f"coverage.txt doesn't report unknown-timezone rows:\n{coverage}"
        print("[PASS] run summary + coverage.txt report rows excluded from the timeline for having no timezone, and name the flag that resolves them")

    with _test_case('normalize_changes: an empty identity/reference cell is None like its derived *_id sibling, not…'):
        assert camp_rename["user_email"] is None and camp_rename["ad_group_resource"] is None and camp_rename["ad_group_name"] is None, f"empty identity/reference cells must be None, not '': {camp_rename}"
        assert camp_rename["actor_type"] == "automation" and camp_rename["old_value"] == "Search | Generic | v1"
        print("[PASS] normalize_changes: an empty identity/reference cell is None like its derived *_id sibling, not '' in one field and None in the next")


def _run_round7_cases(td):
    """Follow-ups to the 6th audit round (2026-10-10): the remaining
    correctness gaps it listed, plus --currency, per-field rows for
    multi-field events, and the column-packed dashboard payload."""
    iso = ["2026-08-01 10:00:00"] * 200
    with _test_case("detect_date_format: a slash date past row 200 is still seen (only the first 200 rows used to be sampled)"):
        assert detect_date_format(iso + ["03/04/2026 10:00:00"]) is None, "an ambiguous slash date after row 200 must still stop and ask"
        assert detect_date_format(iso + ["25/04/2026 10:00:00"]) == "DMY"
        assert detect_date_format(iso + ["04/25/2026 10:00:00"]) == "MDY"
        print("[PASS] detect_date_format: a slash date past row 200 is still seen (only the first 200 rows used to be sampled)")

    with _test_case("detect_date_format: one unreadable value in an ISO column no longer asks 'DD/MM or MM/DD?'"):
        assert detect_date_format(["2026-08-01 10:00:00"] * 50 + ["N/A"]) == "ISO"
        assert detect_date_format(["Aug 16, 2026, 10:42:26 PM"] * 5 + ["n/a"]) == "UI_EN"
        assert detect_date_format(["garbage", "more garbage"]) is None, "a column with no recognizable date at all must still stop"
        junk = [dict(row) for row in SAMPLE_API_WITH_NAMES_ROWS]
        junk[1]["change_date_time"] = "N/A"
        fj = td / "api_one_bad_date.json"
        fj.write_text(json.dumps(junk), encoding="utf-8")
        n_junk = normalize_changes(fj, validate_source(fj)["mapping"], "t")
        assert n_junk["status"] == "ok" and n_junk["rows_skipped_unparseable_date"] == 1, f"one bad date must be counted as skipped, not stop the run: {n_junk.get('status')}"
        print("[PASS] detect_date_format: one unreadable value in an ISO column no longer asks 'DD/MM or MM/DD?'")

    with _test_case("structured_rules: creating a campaign/ad group whose changed field is status is 'created', not Status/Enabled"):
        assert match_structured({"resource_type": "CAMPAIGN", "field_name": "status", "operation": "CREATE", "new_value": "ENABLED"}) == ("Campaign", "Campaign created")
        assert match_structured({"resource_type": "AD_GROUP", "field_name": "status", "operation": "CREATE", "new_value": "PAUSED"}) == ("AdGroup", "Ad group created")
        assert match_structured({"resource_type": "CAMPAIGN", "field_name": "status", "operation": "UPDATE", "new_value": "ENABLED"}) == ("Status", "Enabled")
        print("[PASS] structured_rules: creating a campaign/ad group whose changed field is status is 'created', not Status/Enabled")

    with _test_case("structured_rules: 'Negative keyword' needs both 'keyword' and 'negative' in the field; no dict literal repeats a key"):
        neg = {"resource_type": "AD_GROUP_CRITERION", "operation": "CREATE"}
        assert match_structured({**neg, "field_name": "negative_keyword.text"}) == ("Keyword", "Negative keyword")
        assert match_structured({**neg, "field_name": "keyword.text, negative"}) == ("Keyword", "Negative keyword")
        assert match_structured({**neg, "field_name": "negative, placement.url"}) is None, "a negative non-keyword criterion is not a negative keyword"
        import ast
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        repeated = sorted({(d.lineno, k.value) for d in ast.walk(tree) if isinstance(d, ast.Dict)
                           for k in d.keys if isinstance(k, ast.Constant) and sum(1 for o in d.keys if isinstance(o, ast.Constant) and o.value == k.value) > 1})
        assert not repeated, f"dict literal repeats a key (the earlier value is silently discarded): {repeated}"
        print("[PASS] structured_rules: 'Negative keyword' needs both 'keyword' and 'negative' in the field; no dict literal repeats a key")

    orphan = [dict(row) for row in SAMPLE_API_WITH_NAMES_ROWS]
    orphan[2]["ad_group_name"] = "Ad Group 1"   # a budget row: its ad_group reference is empty
    fo = td / "api_orphan_name.json"
    fo.write_text(json.dumps(orphan), encoding="utf-8")
    with _test_case("normalize_changes: a display name can't attach a row to an ad group its own reference column says it lacks"):
        r_o = run_pipeline(fo, td / "out_r7", generated_at="2026-08-17T00:00:00")
        assert r_o["status"] == "ok", f"orphan-name fixture failed: {r_o}"
        with open(td / "out_r7" / "changes.jsonl", encoding="utf-8") as fh:
            o_rows = [json.loads(line) for line in fh]
        o_budget = next(x for x in o_rows if x["resource_type"] == "CAMPAIGN_BUDGET")
        assert o_budget["ad_group_name"] is None and o_budget["campaign_name"] == "Campaign Alpha", f"orphan ad_group_name kept: {o_budget['ad_group_name']!r}"
        assert next(x for x in o_rows if x["operation"] == "CREATE")["ad_group_name"] == "Ad Group 1", "a row WITH an ad_group reference keeps its name"
        assert r_o.get("orphan_names_dropped") == 1, f"dropped names must be counted in the run summary: {r_o.get('orphan_names_dropped')}"
        assert "without a matching reference: 1" in (td / "out_r7" / "coverage.txt").read_text(encoding="utf-8")
        fl = td / "legacy_names_only.csv"
        fl.write_text(SAMPLE_LEGACY_SUMMARY_TR_CSV, encoding="utf-8")
        n_l = normalize_changes(fl, validate_source(fl)["mapping"], "legacy_summary_tr", decimal_style="TR")
        assert sum(1 for x in n_l["rows"] if x["ad_group_name"]) == 5 and n_l["orphan_names_dropped"] == 0, "a name-only source (no reference column at all) keeps every name"
        print("[PASS] normalize_changes: a display name can't attach a row to an ad group its own reference column says it lacks")

    with _test_case("--currency: monetary rows carry the code and show it as their unit; other rows don't; a bad code is rejected"):
        r_c = run_pipeline(fo, td / "out_r7_cur", currency="TRY", generated_at="2026-08-17T00:00:00")
        assert r_c["status"] == "ok", f"--currency run failed: {r_c}"
        with open(td / "out_r7_cur" / "changes.jsonl", encoding="utf-8") as fh:
            c_rows = {x["field_name"]: x for x in (json.loads(line) for line in fh)}
        assert (c_rows["amount_micros"]["currency"], c_rows["amount_micros"]["value_unit"], c_rows["amount_micros"]["old_value_display"]) == ("TRY", "TRY", 50.0), f"budget row: {c_rows['amount_micros']}"
        assert c_rows["name"]["currency"] is None and c_rows["name"]["value_unit"] is None
        assert o_budget["currency"] is None and o_budget["value_unit"] == "units (converted from micros)", "without --currency nothing changes"
        assert run_pipeline(fo, td / "out_r7_badcur", currency="lira")["status"] == "error"
        print("[PASS] --currency: monetary rows carry the code and show it as their unit; other rows don't; a bad code is rejected")

    with _test_case("multi-field events: split into one row per field when old/new carry the per-field values (nested resource JSON)"):
        camp_old = {"campaign": {"resourceName": "customers/9990001/campaigns/2001", "status": "ENABLED", "name": "Old name"}}
        camp_new = {"campaign": {"resourceName": "customers/9990001/campaigns/2001", "status": "PAUSED", "name": "New name"}}
        events = [
            _r6_row(7, "2026-08-05T10:00:00+03:00", "CAMPAIGN", "campaigns/2001", "UPDATE", ["status", "name"], json.dumps(camp_old), json.dumps(camp_new), ad_group=""),
            # camelCase keys + a nested object passed as-is, not as a JSON string
            _r6_row(8, "2026-08-05T11:00:00+03:00", "CAMPAIGN_BUDGET", "campaignBudgets/1001", "UPDATE", ["amount_micros", "delivery_method"],
                    {"campaignBudget": {"amountMicros": "50000000", "deliveryMethod": "STANDARD"}}, {"campaignBudget": {"amountMicros": 75000000, "deliveryMethod": "ACCELERATED"}}, ad_group=""),
        ]
        fmf = td / "api_multi_field_nested.json"
        fmf.write_text(json.dumps(events), encoding="utf-8")
        r_mf = run_pipeline(fmf, td / "out_r7_mf", generated_at="2026-08-17T00:00:00")
        assert r_mf["status"] == "ok" and r_mf["row_count"] == 4, f"2 events x 2 fields must give 4 rows: {r_mf}"
        with open(td / "out_r7_mf" / "changes.jsonl", encoding="utf-8") as fh:
            mf = {x["field_name"]: x for x in (json.loads(line) for line in fh)}
        assert (mf["status"]["old_value"], mf["status"]["new_value"], mf["status"]["subcategory"]) == ("ENABLED", "PAUSED", "Paused"), f"status row: {mf.get('status')}"
        assert (mf["name"]["old_value"], mf["name"]["new_value"], mf["name"]["subcategory"]) == ("Old name", "New name", "Campaign name changed")
        assert (mf["amount_micros"]["old_value"], mf["amount_micros"]["new_value"], mf["amount_micros"]["new_value_display"]) == ("50000000", "75000000", 75.0), f"budget row: {mf['amount_micros']}"
        assert mf["delivery_method"]["new_value"] == "ACCELERATED"
        assert not any(x["multi_field"] for x in mf.values()) and mf["status"]["event_id"] == mf["name"]["event_id"] and mf["status"]["change_id"] != mf["name"]["change_id"]
        assert json.loads((td / "out_r7_mf" / "change_history.json").read_text(encoding="utf-8"))["meta"]["event_count"] == 2
        print("[PASS] multi-field events: split into one row per field when old/new carry the per-field values (nested resource JSON)")

    with _test_case("dashboard payload: changes are embedded column-packed and unpack in the browser's own JS to the identical rows (Node)"):
        import subprocess
        many = [{**row, "change_id": f"{row['change_id']}{i}", "timestamp_utc": None if i % 7 == 0 else row["timestamp_utc"]} for i in range(150) for row in o_rows]
        packed = pack_changes(many)
        m = re.search(r"function unpackChanges\(packed\)\{.*?\n\}\n", DASHBOARD_TEMPLATE, re.S)
        assert m, "unpackChanges() not found in the dashboard template"
        (td / "packed.json").write_text(json.dumps(packed), encoding="utf-8")
        js = m.group(0) + f"\nprocess.stdout.write(JSON.stringify(unpackChanges(JSON.parse(require('fs').readFileSync({json.dumps(str(td / 'packed.json'))}, 'utf8')))));"
        proc = subprocess.run(["node", "-e", js], capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, f"unpackChanges failed under Node:\n{proc.stderr}"
        assert json.loads(proc.stdout) == many, "unpacked rows differ from the rows that were packed"
        plain, small = len(json.dumps(many)), len(json.dumps(packed))
        assert small < plain * 0.4, f"packing saved too little: {small} vs {plain} bytes"
        html = (td / "out_r7" / "dashboard.html").read_text(encoding="utf-8")
        assert '"changes_packed"' in html and not re.search(r'"changes"\s*:\s*\[', html), "the shipped dashboard must embed the packed form only"
        assert isinstance(json.loads((td / "out_r7" / "change_history.json").read_text(encoding="utf-8"))["changes"], list), "change_history.json stays plain rows"
        print(f"[PASS] dashboard payload: changes are embedded column-packed and unpack in the browser's own JS to the identical rows (Node) — {round(100 * small / plain)}% of the plain size on this fixture")


class _FakeEnum:
    def __init__(self, name):
        self.name = name


class _FakeResource:
    """Stands in for a proto-plus ChangedResource: only to_dict() is used."""
    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data


def _fake_change_event(n, rtype, rname, op, paths, old, new, campaign=""):
    import types
    return types.SimpleNamespace(change_event=types.SimpleNamespace(
        resource_name=f"customers/9990001/changeEvents/178565760000000{n}~{n}~0", change_date_time=f"2026-08-0{n} 10:00:00.000000",
        change_resource_type=_FakeEnum(rtype), change_resource_name=rname, client_type=_FakeEnum("GOOGLE_ADS_WEB_CLIENT"),
        user_email="user.a@example.test", resource_change_operation=_FakeEnum(op), changed_fields=types.SimpleNamespace(paths=paths),
        old_resource=_FakeResource(old), new_resource=_FakeResource(new), campaign=campaign, ad_group="", feed="", feed_item="", asset=""))


def _run_round8_cases(td):
    """2026-10-10, third pass: the live-fetch tool's API version, the scope
    wording around "campaigns with no change", one data-quality summary for
    an ok run, and answering needs_mapping without hand-writing JSON."""
    tool_path = SCRIPT_DIR.parent.parent / "tools" / "fetch_live_data.py"
    if not tool_path.exists():
        print("[SKIP] tools/fetch_live_data.py is not shipped next to this copy of the skill — its cases need the repo checkout")
    else:
        import importlib.util
        import types
        spec = importlib.util.spec_from_file_location("fetch_live_data_under_test", tool_path)
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        with _test_case("fetch tool: requests one named, still-served Google Ads API version, and requirements.txt can supply it"):
            src = tool_path.read_text(encoding="utf-8")
            assert re.fullmatch(r"v\d+", getattr(tool, "API_VERSION", "")), "the API version must be one named constant, API_VERSION"
            literals = set(re.findall(r'version\s*=\s*["\'](v\d+)["\']', src))
            assert not literals, f"hard-coded API version literal(s) {literals} — use API_VERSION so the sunset check below covers every call"
            assert date.today() < tool.API_VERSION_SUNSET, (
                f"Google Ads API {tool.API_VERSION} was scheduled to sunset in {tool.API_VERSION_SUNSET:%B %Y} — requests to a sunset version fail. "
                "Check https://developers.google.com/google-ads/api/docs/sunset-dates, then update API_VERSION, API_VERSION_SUNSET and tools/requirements.txt together.")
            req = (tool_path.parent / "requirements.txt").read_text(encoding="utf-8")
            assert re.search(r"^google-ads>=\d+\.\d+", req, re.M), f"requirements.txt must set the minimum google-ads release that serves {tool.API_VERSION}: {req!r}"
            print(f"[PASS] fetch tool: requests one named, still-served Google Ads API version ({tool.API_VERSION}), and requirements.txt can supply it")

        with _test_case("fetch tool (mocked client, no credentials): asks for API_VERSION and writes rows the skill reads end to end"):
            seen = {}
            events = [
                _fake_change_event(1, "CAMPAIGN_BUDGET", "customers/9990001/campaignBudgets/1001", "UPDATE", ["amount_micros"],
                                   {"campaign_budget": {"amount_micros": 50000000}}, {"campaign_budget": {"amount_micros": 75000000}}),
                _fake_change_event(2, "CAMPAIGN", "customers/9990001/campaigns/2001", "UPDATE", ["status", "name"],
                                   {"campaign": {"status": "ENABLED", "name": "Campaign Alpha"}}, {"campaign": {"status": "PAUSED", "name": "Campaign Beta"}},
                                   campaign="customers/9990001/campaigns/2001"),
            ]

            class FakeClient:
                @classmethod
                def load_from_env(cls, version=None):
                    seen["version"] = version
                    return cls()

                def get_service(self, name):
                    return types.SimpleNamespace(search=lambda customer_id, query: seen.update(query=query) or events)

            fakes = {"google": types.ModuleType("google"), "google.ads": types.ModuleType("google.ads"), "google.ads.googleads": types.ModuleType("google.ads.googleads"),
                     "google.ads.googleads.client": types.ModuleType("google.ads.googleads.client"), "google.ads.googleads.errors": types.ModuleType("google.ads.googleads.errors")}
            fakes["google.ads.googleads.client"].GoogleAdsClient = FakeClient
            fakes["google.ads.googleads.errors"].GoogleAdsException = type("GoogleAdsException", (Exception,), {})
            saved = {k: sys.modules.get(k) for k in fakes}
            sys.modules.update(fakes)
            try:
                fetched = tool.fetch("9990001", days=7, limit=50)
            finally:
                for k, v in saved.items():
                    sys.modules.pop(k, None) if v is None else sys.modules.__setitem__(k, v)
            assert seen["version"] == tool.API_VERSION, f"client was created for {seen['version']!r}, not API_VERSION"
            assert "LAST_7_DAYS" in seen["query"] and "LIMIT 50" in seen["query"]
            assert (fetched[0]["changed_fields"], fetched[0]["old_resource"], fetched[0]["new_resource"]) == ("amount_micros", "50000000", "75000000"), f"single-field flatten: {fetched[0]}"
            ff = td / "fetched.json"
            ff.write_text(json.dumps(fetched), encoding="utf-8")
            r_f = run_pipeline(ff, td / "out_r8_fetch", generated_at="2026-08-17T00:00:00")
            assert r_f["status"] == "ok" and r_f["row_count"] == 3, f"1 single-field + 1 two-field event must give 3 rows: {r_f}"
            print("[PASS] fetch tool (mocked client, no credentials): asks for API_VERSION and writes rows the skill reads end to end — synthetic shapes only, NOT a live-API verification")

    dq_rows = [dict(row) for _ in range(3) for row in SAMPLE_API_WITH_NAMES_ROWS]
    for i, row in enumerate(dq_rows):
        row["resource_name"] = f"customers/9990001/changeEvents/17856576000000{i:02d}~{i}~0"
    dq_rows[5]["change_date_time"] = "N/A"                                   # unreadable date -> left out
    dq_rows[6]["resource_name"] = dq_rows[2]["resource_name"]                # exact re-import of row 2 -> duplicate
    dq_rows[7] = {**dq_rows[7], "change_resource_type": "CAMPAIGN_CRITERION", "changed_fields": "proximity_target"}   # no rule -> Other
    fq = td / "api_data_quality.json"
    fq.write_text(json.dumps(dq_rows), encoding="utf-8")
    with _test_case("data_quality: one summary of what an ok run read, left out and could not classify — in the result, coverage.txt and the JSON"):
        r_q = run_pipeline(fq, td / "out_r8_dq", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r_q["status"] == "ok", f"data-quality fixture failed: {r_q}"
        dq = r_q.get("data_quality")
        assert dq, "run result has no data_quality block"
        assert (dq["input_rows"], dq["rows_in_report"], dq["left_out"]) == (12, 10, {"unreadable_date": 1, "duplicate": 1}), f"counts: {dq}"
        assert dq["input_rows"] + dq["rows_added_by_field_split"] - sum(dq["left_out"].values()) == dq["rows_in_report"], "the counts must add up"
        assert dq["in_report_with_limits"]["category_other"] == 1 and dq["in_report_with_limits"]["timezone_unknown"] == 2, f"flags: {dq['in_report_with_limits']}"
        assert dq["ignored_headers"] == ["feed"]
        cov = (td / "out_r8_dq" / "coverage.txt").read_text(encoding="utf-8")
        assert "Input rows: 12" in cov and "In the report: 10" in cov and "Left out: 2" in cov, f"coverage.txt lacks the summary:\n{cov}"
        meta = json.loads((td / "out_r8_dq" / "change_history.json").read_text(encoding="utf-8"))["meta"]
        assert meta["data_quality"] == dq, "change_history.json must carry the same block"
        html = (td / "out_r8_dq" / "dashboard.html").read_text(encoding="utf-8")
        assert 'id="dataQuality"' in html and '"data_quality"' in html, "dashboard must show the data-quality line"
        print("[PASS] data_quality: one summary of what an ok run read, left out and could not classify — in the result, coverage.txt and the JSON")

    with _test_case("dashboard: 'Campaign Last Changes' says up front that it only covers campaigns with a change in this export"):
        section = html[html.index("<h2>Campaign Last Changes</h2>"):html.index('<div id="untouched">')]
        assert "at least one change in this export" in section and "can't tell" in section, "scope sentence missing from the section itself"
        assert "at least one change in this export" in meta["untouched_scope_note"]
        assert "What this report can't do" in html and "A campaign that doesn't appear" in html, "limitations list must name the absent-campaign case"
        assert "category_confidence" in html[html.index("function openDetail"):], "detail panel must show how the category was assigned"
        print("[PASS] dashboard: 'Campaign Last Changes' says up front that it only covers campaigns with a change in this export")

    with _test_case("--mask-users: no e-mail address survives in any output file — including one sitting on an automation row"):
        m_rows = [dict(row) for row in SAMPLE_API_WITH_NAMES_ROWS]
        m_rows[1]["user_email"] = "script.owner@example.test"      # client_type GOOGLE_ADS_SCRIPTS -> actor_type automation
        fmk = td / "api_mask_automation.json"
        fmk.write_text(json.dumps(m_rows), encoding="utf-8")
        r_mk = run_pipeline(fmk, td / "out_r8_mask", mask_users=True, generated_at="2026-08-17T00:00:00")
        assert r_mk["status"] == "ok", f"mask fixture failed: {r_mk}"
        leaks = {f.name: sorted(set(re.findall(r"[\w.]+@example\.test", f.read_text(encoding="utf-8")))) for f in (td / "out_r8_mask").iterdir()}
        assert not any(leaks.values()), f"e-mail address left in masked output: { {k: v for k, v in leaks.items() if v} }"
        with open(td / "out_r8_mask" / "changes.jsonl", encoding="utf-8") as fh:
            mk = {(x["resource_type"], x["operation"]): x for x in (json.loads(line) for line in fh)}
        assert mk[("CAMPAIGN", "UPDATE")]["actor_type"] == "automation" and mk[("CAMPAIGN", "UPDATE")]["client_type"] == "GOOGLE_ADS_SCRIPTS", "the row stays an automation row"
        assert re.fullmatch(r"User [A-Z]+", mk[("AD_GROUP_AD", "UPDATE")]["user_name"]) and mk[("AD_GROUP_AD", "UPDATE")]["account_name"] == "Account A", "humans get a label; account names are not masked"
        print("[PASS] --mask-users: no e-mail address survives in any output file — including one sitting on an automation row")

    fu = td / "unknown_cols.csv"
    fu.write_text(SAMPLE_UNKNOWN_FORMAT_CSV, encoding="utf-8")
    answers = {"timestamp": "Modified On", "user_email": "Modified By Email", "account_name": "Ads Account", "campaign_name": "Camp Name",
               "ad_group_name": "AdGroup Name", "raw_summary": "What Changed", "old_value": "Was", "new_value": "Now", "resource_type": "What Changed"}
    with _test_case("--map: needs_mapping can be answered column by column on the command line, no JSON file to write"):
        saved_dir = globals()["PROFILES_DIR"]
        globals()["PROFILES_DIR"] = td / "profiles-map"
        try:
            assert run_pipeline(fu, td / "out_r8_map0", generated_at="2026-08-17T00:00:00")["status"] == "needs_mapping"
            r_m = run_pipeline(fu, td / "out_r8_map", column_map=answers, allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
            assert r_m["status"] == "ok" and r_m["row_count"] == 3, f"--map run failed: {r_m}"
            again = run_pipeline(fu, td / "out_r8_map2", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
            assert again["status"] == "ok", "an answer given with --map is a human-confirmed mapping: it must be remembered like --mapping-file"
            bad_field = run_pipeline(fu, td / "out_r8_map3", column_map={"when": "Modified On"})
            assert bad_field["status"] == "error" and "when" in bad_field["message"], f"unknown canonical field must be rejected: {bad_field}"
            bad_header = run_pipeline(fu, td / "out_r8_map4", column_map={"timestamp": "Changed On"})
            assert bad_header["status"] == "error" and "Changed On" in bad_header["message"], f"a header the file doesn't have must be rejected: {bad_header}"
            assert parse_column_map(["timestamp=Modified On", "old_value=Was"]) == {"timestamp": "Modified On", "old_value": "Was"}
            try:
                parse_column_map(["timestamp"])
                raise AssertionError("an entry without '=' must be rejected")
            except ValueError:
                pass
        finally:
            globals()["PROFILES_DIR"] = saved_dir
        print("[PASS] --map: needs_mapping can be answered column by column on the command line, no JSON file to write")


def self_test():
    """Runs every case; returns True only if none failed. A failing case is
    reported and the run continues, so one run shows every failure — a case
    that depends on an earlier, failed one fails too (usually with a
    NameError): fix the first failure listed and re-run."""
    import tempfile
    global PROFILES_DIR
    _SELF_TEST_FAILURES.clear()
    real_profiles_dir = PROFILES_DIR
    try:
        with tempfile.TemporaryDirectory() as td:
            _run_pipeline_cases(Path(td))
    finally:
        PROFILES_DIR = real_profiles_dir
    with _test_case("Rule Matches JS regression suite (Node)"):
        _run_js_rule_matches_regression()
    if _SELF_TEST_FAILURES:
        print(f"\n{len(_SELF_TEST_FAILURES)} self-test case(s) FAILED:")
        for label in _SELF_TEST_FAILURES:
            print(f"  - {label}")
    else:
        print("\nAll self-tests passed.")
    return not _SELF_TEST_FAILURES


def _run_pipeline_cases(td):
    global PROFILES_DIR
    # Isolate mapping-profiles/ for the duration of the test — otherwise
    # a saved profile from one run leaks into the next and self-test
    # results depend on prior runs instead of being reproducible.
    PROFILES_DIR = td / "mapping-profiles"
    with _test_case('legacy_summary_tr: … rows, …% Other, … human users, … automation actor(s)'):
        # --- fixture 1: known legacy_summary_tr, full run in one call ---
        f1 = td / "legacy.csv"
        f1.write_text(SAMPLE_LEGACY_SUMMARY_TR_CSV, encoding="utf-8")
        r1 = run_pipeline(f1, td / "out1", decimal_style="TR", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r1["status"] == "ok", f"fixture1 failed: {r1}"
        assert r1["row_count"] == 10, f"fixture1 row_count={r1['row_count']}"
        assert (td / "out1" / "dashboard.html").exists()
        print(f"[PASS] legacy_summary_tr: {r1['row_count']} rows, {r1['other_pct']}% Other, "
              f"{r1['human_users']} human users, {r1['automation_actors']} automation actor(s)")

    with _test_case('api_change_event: correctly stopped for unknown category (CAMPAIGN_CRITERION/proximity_target)…'):
        # --- fixture 2: API JSON, includes one unmapped category (CAMPAIGN_CRITERION/proximity_target) ---
        f2 = td / "api.json"
        f2.write_text(json.dumps(SAMPLE_API_CHANGE_EVENT_JSON), encoding="utf-8")
        r2 = run_pipeline(f2, td / "out2", generated_at="2026-08-17T00:00:00")
        assert r2["status"] == "needs_category_review", f"fixture2 should need category review, got: {r2['status']}"
        assert any(u["field_name"] == "proximity_target" for u in r2["unknown_combinations"]), "fixture2: expected CAMPAIGN_CRITERION/proximity_target to be flagged unknown"
        print(f"[PASS] api_change_event: correctly stopped for unknown category (CAMPAIGN_CRITERION/proximity_target) instead of guessing")

    with _test_case('api_change_event with --allow-unknown-categories: … rows built'):
        r2b = run_pipeline(f2, td / "out2b", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r2b["status"] == "ok", f"fixture2b failed: {r2b}"
        assert r2b["row_count"] == 5
        print(f"[PASS] api_change_event with --allow-unknown-categories: {r2b['row_count']} rows built")

    with _test_case("ISO offset ('+03:00') resolves to UTC with no --timezone flag needed"):
        # regression guard: ISO offset ('+03:00') must resolve to UTC WITHOUT
        # a --timezone flag — this silently broke before the 2026-08-18 fix.
        with open(td / "out2b" / "changes.jsonl", encoding="utf-8") as fh:
            api_rows = [json.loads(line) for line in fh]
        offset_row = next(r for r in api_rows if r["timestamp"].endswith("+03:00"))
        assert offset_row["timestamp_utc"] == "2026-08-01T07:00:00+00:00", f"offset not preserved: {offset_row['timestamp_utc']}"
        assert offset_row["timezone"] == "+03:00"
        print(f"[PASS] ISO offset ('+03:00') resolves to UTC with no --timezone flag needed")

    with _test_case('amount_micros converted to human-readable display value, raw value preserved'):
        # regression guard: micros amounts must be human-readable, raw preserved
        micros_row = next(r for r in api_rows if r["field_name"] == "amount_micros" and r["old_value"] == "150000000")
        assert micros_row["old_value_display"] == 150.0, f"micros not converted: {micros_row}"
        assert micros_row["old_value"] == "150000000", "raw value must never be overwritten"
        print(f"[PASS] amount_micros converted to human-readable display value, raw value preserved")

    with _test_case("source_event_id captured from API's own resource_name, used as event_id"):
        # regression guard: ChangeEvent's own event identity (resource_name)
        # must be captured as source_event_id and used as event_id, not
        # silently discarded in favor of the content-hash change_id.
        assert offset_row["source_event_id"] == "customers/9990001/changeEvents/1785657600000000~1~0"
        assert offset_row["event_id"] == offset_row["source_event_id"]
        assert offset_row["event_id"] != offset_row["change_id"]
        print(f"[PASS] source_event_id captured from API's own resource_name, used as event_id")

    with _test_case('identical duplicate rows collapse to 1 (duplicates_removed=…)'):
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

    with _test_case('same-named campaign in 2 different accounts produces 2 separate Campaign Last Changes entries'):
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

    with _test_case("GOOGLE_ADS_API client_type classified as 'unknown' actor, not assumed human"):
        # regression guard: GOOGLE_ADS_API must not be auto-classified human —
        # it's genuinely ambiguous (could be a human's script or a service account).
        assert derive_actor_type(None, "GOOGLE_ADS_API") == "unknown", "GOOGLE_ADS_API must not default to human"
        print(f"[PASS] GOOGLE_ADS_API client_type classified as 'unknown' actor, not assumed human")

    with _test_case('escapeHtml present and used broadly in the shipped dashboard'):
        # regression guard: escapeHtml must exist in the shipped dashboard —
        # weak but real guard against a future edit silently removing it and
        # reopening the XSS hole verified manually in the browser this session.
        dashboard_html = (td / "out2b" / "dashboard.html").read_text(encoding="utf-8")
        assert "function escapeHtml" in dashboard_html, "escapeHtml helper missing from shipped dashboard"
        assert dashboard_html.count("escapeHtml(") > 15, "escapeHtml used too rarely — likely missing from a render path"
        print(f"[PASS] escapeHtml present and used broadly in the shipped dashboard")

    with _test_case("Explorer table's numeric-aware sort comparator present in shipped dashboard (verified separatel…"):
        # regression guard: Explorer table's numeric-aware sort comparator
        # (3rd audit round fix) must ship in the dashboard. The actual sort
        # LOGIC was verified directly under Node this session — [100, 20]
        # sorted by old_value_num now correctly returns [20, 100], not the
        # previous lexicographic ['100', '20'] — this just guards the
        # function against being silently removed later.
        assert "function explorerSortValue" in dashboard_html, "numeric-aware Explorer sort helper missing from shipped dashboard"
        assert "compareExplorerSortValues" in dashboard_html, "numeric-aware Explorer sort comparator missing from shipped dashboard"
        print(f"[PASS] Explorer table's numeric-aware sort comparator present in shipped dashboard (verified separately under Node: [100,20] sorts to [20,100], not lexicographic)")

    with _test_case("'user_name' now self-maps like user_email/client_type already did — a source using that literal…"):
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

    with _test_case("read_rows(): JSON headers now come from the union of ALL rows' keys, not just row 0 — a key onl…"):
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

    with _test_case('check_hard_required() now runs for --mapping-file and cached-fingerprint mappings too, not just…'):
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

    with _test_case('an invalid --timezone (e.g. a typo) now stops the pipeline with a clear error instead of being…'):
        # =================================================================
        # 4th audit round (2026-08-18) — every claim independently
        # reproduced against this file's actual code before being fixed,
        # same standing practice as the first three rounds.
        # =================================================================

        # An invalid --timezone (a typo) used to be silently swallowed per
        # row, leaving timestamp_utc=None and the row's own "timezone" field
        # holding the invalid string, with status "ok" throughout.
        f_badtz = td / "p4_badtz.json"
        f_badtz.write_text(json.dumps([
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "tz1", "user_email": "a@example.test",
             "change_resource_type": "CAMPAIGN", "resource_change_operation": "UPDATE", "changed_fields": "status",
             "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ]), encoding="utf-8")
        r_badtz = run_pipeline(f_badtz, td / "out_badtz", tz="Europe/Istanbull", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_badtz["status"] == "error", f"an invalid --timezone must stop the pipeline, got: {r_badtz}"
        assert "Istanbull" in r_badtz["message"], f"error message should name the bad value: {r_badtz['message']}"
        r_goodtz = run_pipeline(f_badtz, td / "out_goodtz", tz="Europe/Istanbul", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_goodtz["status"] == "ok", f"a valid --timezone must still work: {r_goodtz}"
        print(f"[PASS] an invalid --timezone (e.g. a typo) now stops the pipeline with a clear error instead of being silently swallowed per-row")

    with _test_case('a missing or invalid-JSON --mapping-file now returns a structured error instead of a raw traceb…'):
        # A missing/invalid --mapping-file used to crash with a raw
        # FileNotFoundError/JSONDecodeError instead of a structured status.
        v_missing_map = validate_source(f_badtz, mapping_file=td / "does_not_exist.json", save_profile=False)
        assert v_missing_map["status"] == "error", f"a missing --mapping-file must return a structured error, got: {v_missing_map}"
        f_badjson_map = td / "p4_bad_mapping.json"
        f_badjson_map.write_text("{not valid json", encoding="utf-8")
        v_badjson_map = validate_source(f_badtz, mapping_file=f_badjson_map, save_profile=False)
        assert v_badjson_map["status"] == "error", f"an invalid-JSON --mapping-file must return a structured error, got: {v_badjson_map}"
        print(f"[PASS] a missing or invalid-JSON --mapping-file now returns a structured error instead of a raw traceback")

    with _test_case('validate_extra_rules(): a malformed --extra-rules entry (missing category/subcategory, no match…'):
        # extra_rules entries missing category/subcategory used to crash
        # match_structured() with a raw KeyError the moment a row matched —
        # --extra-rules is this tool's own documented fix for
        # needs_category_review, so a malformed rule here must be validated
        # up front, not crash mid-categorization.
        assert validate_extra_rules([{"resource_type": "CAMPAIGN"}]) is not None, "a rule missing category/subcategory must fail validation"
        assert validate_extra_rules([{"category": "X", "subcategory": "Y"}]) is not None, "a rule with no matcher key must fail validation"
        assert validate_extra_rules("not a list") is not None, "a non-list extra_rules must fail validation"
        assert validate_extra_rules([{"resource_type": "CAMPAIGN", "category": "X", "subcategory": "Y"}]) is None, "a well-formed rule must pass validation"
        r_badrule = run_pipeline(f_badtz, td / "out_badrule", extra_rules=[{"resource_type": "CAMPAIGN"}], generated_at="2026-08-17T00:00:00")
        assert r_badrule["status"] == "error", f"run_pipeline must reject a malformed extra_rules entry before touching the data, got: {r_badrule}"
        print(f"[PASS] validate_extra_rules(): a malformed --extra-rules entry (missing category/subcategory, no matcher, wrong shape) now fails validation up front instead of crashing match_structured() with a raw KeyError")

    with _test_case('query_changes(): --since now validates against ^\\d+[dwm]$ — garbage and negative values raise V…'):
        # query's --since parser: int(since.rstrip("dwm")) crashed on garbage
        # ("abc", "7x") and silently accepted a negative count ("-7d"),
        # producing a future cutoff and silently-wrong (usually empty)
        # results instead of an error.
        for bad_since in ("abc", "7x", "-7d", "d", ""):
            if not bad_since:
                continue  # empty string is falsy, since=None path, not exercised here
            try:
                query_changes([{"timestamp_iso": "2026-08-01T00:00:00"}], since=bad_since)
                raise AssertionError(f"--since {bad_since!r} should have raised ValueError")
            except ValueError:
                pass
        assert query_changes([{"timestamp_iso": "2026-08-01T00:00:00"}], since="0d") == [{"timestamp_iso": "2026-08-01T00:00:00"}]
        print(f"[PASS] query_changes(): --since now validates against ^\\d+[dwm]$ — garbage and negative values raise ValueError instead of crashing or silently returning wrong results")

    with _test_case("a >20% unparseable-date rate now stops with needs_date_review instead of silently returning 'ok…"):
        # Any unparseable-date rate, even 90%+, previously still returned
        # status "ok" with just a WARNING line in coverage.txt. Past 20% the
        # pipeline now stops (mirrors the >30%-empty-required-field gate's
        # force_review escape hatch).
        f_highskip = td / "p4_high_date_skip.json"
        f_highskip.write_text(json.dumps([
            {"change_date_time": "2026-08-01 09:00:00", "customer_id": "1", "resource_name": "hs1", "user_email": "a@example.test",
             "change_resource_type": "CAMPAIGN", "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"},
        ] + [
            {"change_date_time": "2026-13-45 09:00:00", "customer_id": "1", "resource_name": f"hs{i}", "user_email": "a@example.test",
             "change_resource_type": "CAMPAIGN", "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "ENABLED", "new_resource": "PAUSED"}
            for i in range(2, 6)
        ]), encoding="utf-8")
        r_highskip = run_pipeline(f_highskip, td / "out_highskip", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r_highskip["status"] == "needs_date_review", f"a >20% unparseable-date rate must stop the pipeline, got: {r_highskip}"
        r_highskip_forced = run_pipeline(f_highskip, td / "out_highskip_forced", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_highskip_forced["status"] == "ok", f"--force-review must let a high unparseable-date rate proceed, got: {r_highskip_forced}"
        print(f"[PASS] a >20% unparseable-date rate now stops with needs_date_review instead of silently returning 'ok' with most of the dataset dropped (--force-review bypasses it once confirmed)")

    with _test_case("a CAMPAIGN/AD_GROUP-type change with no separate attributed 'campaign'/'ad_group' field now fal…"):
        # Found 2026-08-18 via a real API-connected user's export (not a
        # pasted audit): a CAMPAIGN/AD_GROUP-type change whose row has no
        # separate "campaign"/"ad_group" attributed field (Google's own
        # field only makes sense for a resource BELOW the campaign — a
        # campaign doesn't point to itself) left campaign_id/ad_group_id
        # empty even though the resource's own identity (already resolved
        # from change_resource_name) IS that identity. Symptom: real rows
        # correctly categorized "Campaign"/"AdGroup" still didn't count
        # toward changed_campaigns/changed_ad_groups.
        self_ref_rows = [
            {"change_date_time": "2026-08-17 09:28:28", "customer_id": "1", "resource_name": "sr1", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "CAMPAIGN", "change_resource_name": "customers/1/campaigns/555",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "PAUSED", "new_resource": "ENABLED"},
            {"change_date_time": "2026-08-17 09:29:00", "customer_id": "1", "resource_name": "sr2", "user_email": "a@example.test",
             "client_type": "GOOGLE_ADS_WEB_CLIENT", "change_resource_type": "AD_GROUP", "change_resource_name": "customers/1/adGroups/777",
             "resource_change_operation": "UPDATE", "changed_fields": "status", "old_resource": "PAUSED", "new_resource": "ENABLED"},
        ]
        f_selfref = td / "p4_self_referential.json"
        f_selfref.write_text(json.dumps(self_ref_rows), encoding="utf-8")
        r_selfref = run_pipeline(f_selfref, td / "out_selfref", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_selfref["status"] == "ok", f"self-referential campaign/ad_group fixture failed: {r_selfref}"
        with open(td / "out_selfref" / "changes.jsonl", encoding="utf-8") as fh:
            selfref_rows = [json.loads(l) for l in fh]
        camp_row = next(r for r in selfref_rows if r["resource_type"] == "CAMPAIGN")
        ag_row = next(r for r in selfref_rows if r["resource_type"] == "AD_GROUP")
        assert camp_row["campaign_id"] == "555", f"a CAMPAIGN-type row with no attributed 'campaign' field must fall back to its own resource_id, got {camp_row['campaign_id']!r}"
        assert ag_row["ad_group_id"] == "777", f"an AD_GROUP-type row with no attributed 'ad_group' field must fall back to its own resource_id, got {ag_row['ad_group_id']!r}"
        with open(td / "out_selfref" / "change_history.json", encoding="utf-8") as fh:
            ch_selfref = json.load(fh)
        assert ch_selfref["summary"]["changed_campaigns"] == 1, f"expected 1 changed campaign, got {ch_selfref['summary']}"
        assert ch_selfref["summary"]["changed_ad_groups"] == 1, f"expected 1 changed ad group, got {ch_selfref['summary']}"
        print(f"[PASS] a CAMPAIGN/AD_GROUP-type change with no separate attributed 'campaign'/'ad_group' field now falls back to the resource's own identity, instead of leaving campaign_id/ad_group_id empty and undercounting changed_campaigns/changed_ad_groups")

    with _test_case("a real native Google Ads UI 'Change history' CSV export (no account/field_name/resource_type co…"):
        # Found 2026-08-18 via a real native Google Ads UI "Change history
        # report" CSV export (Campaigns > Change history > Download) — not a
        # pasted audit, not the ChangeEvent API. Genuinely different shape
        # from every fixture above: Date & time / User / Campaign / Ad group
        # / Changes, no account column, no field_name/resource_type column
        # at all, a narrow-no-break-space (U+202F) before AM/PM, and a
        # spelled-out-month timestamp format none of DMY/MDY/ISO covered.
        ui_export_csv = (
            "Date & time,User,Campaign,Ad group,Changes\n"
            "\"Aug 16, 2026, 10:42:26 PM\",a@example.test,Campaign Alpha,,\"Campaign changed\n"
            "  Status changed from paused to active\"\n"
            "\"Aug 16, 2026, 5:01:31 PM\",a@example.test,Campaign Alpha,,\"1 campaign active\n"
            "  Status changed from paused to active\"\n"
            "\"Aug 16, 2026, 5:01:30 PM\",a@example.test,Campaign Alpha,Ad Group 1,\"Ad group created\n"
            "  Status is enabled\"\n"
            "\"Aug 16, 2026, 5:01:00 PM\",Google Ads System,Campaign Alpha,,\"12 budget amount decreased\n"
            "  Budget is $5.00\"\n"
        )
        f_ui_export = td / "p4_ui_export.csv"
        f_ui_export.write_text(ui_export_csv, encoding="utf-8")
        r_ui_export = run_pipeline(f_ui_export, td / "out_ui_export", account_name="Test Account", generated_at="2026-08-17T00:00:00")
        assert r_ui_export["status"] == "ok", f"native UI export fixture failed: {r_ui_export}"
        assert r_ui_export["date_format_used"] == "UI_EN", f"expected UI_EN date format auto-detected, got {r_ui_export['date_format_used']}"
        assert r_ui_export["other_pct"] == 0.0, f"expected all 4 rows to categorize (0% Other), got {r_ui_export['other_pct']}"
        with open(td / "out_ui_export" / "changes.jsonl", encoding="utf-8") as fh:
            ui_rows = [json.loads(l) for l in fh]
        assert all(r["account_name"] == "Test Account" for r in ui_rows), "‑‑account-name must apply to every row when the source has no account column"
        cats = {r["category"] for r in ui_rows}
        assert cats == {"Campaign", "Status", "AdGroup", "Budget"}, f"unexpected category set: {cats}"
        print(f"[PASS] a real native Google Ads UI 'Change history' CSV export (no account/field_name/resource_type column, narrow-no-break-space before AM/PM, spelled-out-month timestamps) now runs end to end — --account-name fills the missing account, UI_EN auto-detects the timestamp format, and the new English summary_text_rules categorize it instead of 100% falling to Other")

    with _test_case("a second real native UI export's gaps now covered: a new 'Account' category for customer/accoun…"):
        # Found 2026-08-18 via a SECOND real native UI export (a different
        # account than the one above) — heavier on account-administration
        # events than the first, which surfaced two more gaps: no category
        # existed at all for customer/account-level events (identity,
        # security, access, manager, notifications), and the keyword
        # patterns above only covered "phrase match," missing "broad match"
        # entirely — the more common match type in practice. Other went
        # from 47.8% to 0.0% on the real file this was found against.
        account_events_csv = (
            "Date & time,User,Campaign,Ad group,Changes\n"
            "\"Aug 17, 2026, 6:47:08 PM\",a@example.test,,,\"Customer identity created\n  \"\n"
            "\"Jul 30, 2026, 5:23:50 PM\",a@example.test,,,\"Customer security settings changed\n  Allowed domains changed\"\n"
            "\"Jul 29, 2026, 11:47:16 PM\",a@example.test,,,\"Customer manager created\n  Status is Pending\"\n"
            "\"Jul 29, 2026, 10:37:26 PM\",a@example.test,,,\"Sent invitation to access account with \"\"Standard access\"\" to \"\"b@example.test\"\"\n  \"\n"
            "\"Jul 29, 2026, 10:00:00 PM\",a@example.test,,,\"Updated a@example.test's notification setting to not receive non-critical newsletter messages\n  \"\n"
            "\"Jul 8, 2026, 3:05:41 PM\",a@example.test,Campaign Alpha,,\"Campaign asset created\n  Status is Enabled\"\n"
            "\"Jul 16, 2026, 7:25:33 PM\",a@example.test,Campaign Alpha,Ad Group 1,\"1 broad match keyword added\n  running shoes\"\n"
            "\"Jul 20, 2026, 9:00:00 PM\",a@example.test,Campaign Alpha,Ad Group 1,\"1 broad match keyword paused\n  discount shoes: Status changed from enabled to paused\"\n"
        )
        f_account_events = td / "p4_account_events.csv"
        f_account_events.write_text(account_events_csv, encoding="utf-8")
        r_account_events = run_pipeline(f_account_events, td / "out_account_events", account_name="Test Account", generated_at="2026-08-17T00:00:00")
        assert r_account_events["status"] == "ok", f"account-events fixture failed: {r_account_events}"
        assert r_account_events["other_pct"] == 0.0, f"expected all 8 rows to categorize, got other_pct={r_account_events['other_pct']}"
        with open(td / "out_account_events" / "changes.jsonl", encoding="utf-8") as fh:
            ae_rows = [json.loads(l) for l in fh]
        cats = {r["category"] for r in ae_rows}
        assert "Account" in cats, f"customer/account-administration events must categorize as Account, got categories: {cats}"
        assert "Asset" in cats, f"'Campaign asset created' must categorize as Asset, got categories: {cats}"
        broad_kw_added = next(r for r in ae_rows if "broad match keyword added" in (r["raw_summary"] or ""))
        broad_kw_paused = next(r for r in ae_rows if "broad match keyword paused" in (r["raw_summary"] or ""))
        assert broad_kw_added["category"] == "Keyword", f"broad match keyword added must categorize as Keyword, got {broad_kw_added['category']}"
        assert broad_kw_paused["category"] == "Status", f"broad match keyword paused must categorize as Status, got {broad_kw_paused['category']}"
        print(f"[PASS] a second real native UI export's gaps now covered: a new 'Account' category for customer/account-administration events (identity/security/access/manager/notifications), broad-match keywords (previously only phrase-match was covered), and bare/Campaign/Customer asset-creation lines")

    with _test_case('F-01: two distinct real events (different source_event_id, identical other fields) both survive…'):
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

    with _test_case("F-02: changed_fields as a JSON list no longer crashes (multi_field=True, field_name='…')"):
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

    with _test_case("F-03: ChangeEvent's real campaign/ad_group fields captured correctly (campaign_id=…, ad_group_i…"):
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

    with _test_case('F-04: --mask-users masks changes.jsonl (previously leaked the raw email there)'):
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

    with _test_case('F-05: Summary.changed_campaigns/most_active_campaign correctly account-scoped (… campaigns, top…'):
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

    with _test_case('F-07: operation_confidence distinguishes inferred UPDATE from explicit UNKNOWN'):
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

    with _test_case('F-08/F-09: SEARCH_ADS_360_SYNC and SEARCH_ADS_360_POST classify consistently (both automation)'):
        # F-08/F-09: SEARCH_ADS_360_POST must classify the same as its sibling
        # SEARCH_ADS_360_SYNC (both automation) — was inconsistent before.
        assert derive_actor_type(None, "SEARCH_ADS_360_SYNC") == "automation"
        assert derive_actor_type(None, "SEARCH_ADS_360_POST") == "automation", "F-08/F-09: SEARCH_ADS_360_POST must match its SA360 sibling"
        print(f"[PASS] F-08/F-09: SEARCH_ADS_360_SYNC and SEARCH_ADS_360_POST classify consistently (both automation)")

    with _test_case('F-11: all 6 previously-uncovered resource types (FEED, FEED_ITEM, CAMPAIGN_FEED, AD_GROUP_FEED,…'):
        # F-11: the 6 previously-zero-coverage real ChangeEventResourceType
        # values must now resolve to a real category, not Other/unmatched.
        for rt, expected_category in [("FEED", "Feed"), ("FEED_ITEM", "Feed"), ("CAMPAIGN_FEED", "Feed"),
                                       ("AD_GROUP_FEED", "Feed"), ("AD_GROUP_BID_MODIFIER", "Bidding"),
                                       ("CAMPAIGN_ASSET_SET", "Asset")]:
            hit = match_structured({"resource_type": rt, "field_name": "x", "operation": "UPDATE", "new_value": ""})
            assert hit is not None and hit[0] == expected_category, f"F-11: {rt} should categorize as {expected_category}, got {hit}"
        print(f"[PASS] F-11: all 6 previously-uncovered resource types (FEED, FEED_ITEM, CAMPAIGN_FEED, AD_GROUP_FEED, AD_GROUP_BID_MODIFIER, CAMPAIGN_ASSET_SET) now categorize")

    with _test_case('F-16/F-17/F-18: empty CSV, invalid JSON, missing file all return structured errors (no uncaught…'):
        # F-16/F-17/F-18: malformed input must return a structured error, not
        # crash with a raw Python exception.
        f_empty = td / "f16_empty.csv"; f_empty.write_text("", encoding="utf-8")
        assert run_pipeline(f_empty, td / "out_f16", generated_at="2026-08-17T00:00:00")["status"] == "error"
        f_badjson = td / "f17_bad.json"; f_badjson.write_text("{not valid json", encoding="utf-8")
        assert run_pipeline(f_badjson, td / "out_f17", generated_at="2026-08-17T00:00:00")["status"] == "error"
        assert run_pipeline(td / "f18_missing.csv", td / "out_f18", generated_at="2026-08-17T00:00:00")["status"] == "error"
        print(f"[PASS] F-16/F-17/F-18: empty CSV, invalid JSON, missing file all return structured errors (no uncaught exceptions)")

    with _test_case('F-19: duplicate column headers are detected and warned about'):
        # F-19: duplicate column headers must be detected and warned about.
        f_dup = td / "f19_dup.csv"
        f_dup.write_text("Date,Date,User,Account,Campaign,Change,Was,Now\n2026-08-01,2026-08-01,U1,A,C1,status,ENABLED,PAUSED\n", encoding="utf-8")
        dup_mapping = {"source_label": "t", "mapping": {"timestamp": "Date", "user_name": "User", "account_name": "Account",
                       "campaign_name": "Campaign", "raw_summary": "Change", "old_value": "Was", "new_value": "Now", "resource_type": "Change"}}
        f_dup_map = td / "f19_dup_mapping.json"; f_dup_map.write_text(json.dumps(dup_mapping), encoding="utf-8")
        v_dup = validate_source(f_dup, mapping_file=f_dup_map, save_profile=False)
        assert any("Duplicate column header" in w for w in v_dup.get("warnings", [])), f"F-19: duplicate header warning missing: {v_dup.get('warnings')}"
        print(f"[PASS] F-19: duplicate column headers are detected and warned about")

    with _test_case("P0: script-context XSS — payload appears only as escaped unicode, never as raw '</script>'"):
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

    with _test_case('P1: two different campaign_ids in the same account (no display name) count as 2 campaigns, not 1'):
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

    with _test_case("P1: change_history.json's untouched field renamed to days_since_last_change_at_generation (no l…"):
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

    with _test_case('P1: unparseable-date rows reported in coverage.txt and change_history.json meta, not just a sil…'):
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
        # force_review=True: this 2-row fixture is 50% unparseable by design
        # (that's the point of the test) — legitimately trips the 4th-audit-
        # round's >20% needs_date_review gate on a sample this small, same
        # known-correct gate behavior as the other force_review fixtures.
        r_mixdate = run_pipeline(f_mixdate, td / "out_p1mixdate", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r_mixdate["rows_skipped_unparseable_date"] == 1
        coverage_text = (td / "out_p1mixdate" / "coverage.txt").read_text(encoding="utf-8")
        assert "Rows skipped (unparseable date): 1" in coverage_text, f"P1: coverage.txt must report the skip: {coverage_text}"
        with open(td / "out_p1mixdate" / "change_history.json", encoding="utf-8") as fh:
            ch_mixdate = json.load(fh)
        assert ch_mixdate["meta"]["rows_skipped_unparseable_date"] == 1
        print(f"[PASS] P1: unparseable-date rows reported in coverage.txt and change_history.json meta, not just a silently-dropped count")

    with _test_case('P2: ISO timestamp with trailing junk after a valid offset is rejected (previously silently trun…'):
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

    with _test_case('P2: DMY-format timestamp with an explicit offset now preserves it (previously always dropped)'):
        # P2: DMY format now preserves a trailing offset, matching the ISO
        # branch's existing behavior (previously DMY never even tried).
        dmy_dt = parse_timestamp("01/08/2026 10:00:00+03:00", "DMY")
        assert dmy_dt.tzinfo is not None, "P2: DMY timestamp with an explicit offset must preserve it"
        assert dmy_dt.utcoffset().total_seconds() == 3 * 3600
        print(f"[PASS] P2: DMY-format timestamp with an explicit offset now preserves it (previously always dropped)")

    with _test_case('P2: CAMPAIGN/AD_GROUP REMOVE categorize as Campaign removed/Ad group removed, not swallowed by…'):
        # P2: CAMPAIGN/AD_GROUP REMOVE must categorize as Campaign
        # removed/Ad group removed, not be shadowed by the generic
        # operation=REMOVE -> Status/Removed catch-all.
        assert match_structured({"resource_type": "CAMPAIGN", "field_name": "status", "operation": "REMOVE", "new_value": ""}) == ("Campaign", "Campaign removed")
        assert match_structured({"resource_type": "AD_GROUP", "field_name": "status", "operation": "REMOVE", "new_value": ""}) == ("AdGroup", "Ad group removed")
        print(f"[PASS] P2: CAMPAIGN/AD_GROUP REMOVE categorize as Campaign removed/Ad group removed, not swallowed by the generic Status/Removed rule")

    with _test_case('P2: two different account_ids count as 2 active accounts (account_id-based, not account_name-ba…'):
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

    with _test_case('P2: two different user emails count as 2 active users (email-based identity, not display-name-b…'):
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

    with _test_case('P3: known_sources now actually consulted — source_label names the real format instead of generi…'):
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

    with _test_case("SUMMARY_CHANGED_FROM_TO_RE: 'X changed/increased/decreased from A to B' summaries now parsed as…"):
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

    with _test_case("Permanent 'what this report can't do' disclosure present in every dashboard, independent of thi…"):
        # Permanent "what this report can't do" disclosure: unlike apiCaveat's
        # run-specific warn-boxes, this must appear in every dashboard, even
        # one with clean data and no API rows — it's about the tool's own
        # structural limits, not this run's data quality.
        dash_html = (td / "out_cft" / "dashboard.html").read_text(encoding="utf-8")
        assert "What this report can't do" in dash_html, "permanent limitations disclosure missing from dashboard"
        assert "never whether a change was good, risky" in dash_html, "limitations disclosure must stay factual, not judgmental"
        print(f"[PASS] Permanent 'what this report can't do' disclosure present in every dashboard, independent of this run's data")

    with _test_case('normalize_number: negative values with a single separator no longer have their magnitude corrup…'):
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

    with _test_case("try_alias_match: a plain 'campaign' display-name column (values with no '/') resolves to campai…"):
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

    with _test_case('make_change_id: same event with a space-separated vs T-separated raw timestamp now dedupes corr…'):
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

    with _test_case("parse_timestamp: slash-separated ISO dates ('2026/08/01') now parse under --date-format ISO (pr…"):
        # parse_timestamp: a slash-separated ISO-shaped date must parse under
        # explicit --date-format ISO, matching DMY/MDY (which already accept
        # both '/' and '.').
        slash_iso_dt = parse_timestamp("2026/08/01 10:00:00", "ISO")
        assert slash_iso_dt.year == 2026 and slash_iso_dt.month == 8 and slash_iso_dt.day == 1, f"slash-separated ISO date not parsed correctly: {slash_iso_dt}"
        print(f"[PASS] parse_timestamp: slash-separated ISO dates ('2026/08/01') now parse under --date-format ISO (previously always raised unparseable)")

    with _test_case('read_rows: a JSON array of non-object values ([1, 2, 3]) now returns a structured error instead…'):
        # read_rows: a JSON array of non-object values must raise a clear
        # ValueError (surfaced as a structured error status), not an
        # unhandled AttributeError from rows[0].keys().
        f_prim = td / "p3_primitives.json"
        f_prim.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
        r_prim = run_pipeline(f_prim, td / "out_prim", generated_at="2026-08-17T00:00:00")
        assert r_prim["status"] == "error", f"primitive-array JSON must return a structured error, got: {r_prim}"
        print(f"[PASS] read_rows: a JSON array of non-object values ([1, 2, 3]) now returns a structured error instead of crashing with AttributeError")

    with _test_case("read_rows: a JSON object without 'results'/'changeEvents' now names the actual problem, not jus…"):
        # read_rows: a JSON object without a recognized wrapper key names the
        # actual problem instead of a generic "no headers found".
        f_unwrapped = td / "p3_unwrapped.json"
        f_unwrapped.write_text(json.dumps({"changes": [{"a": 1}]}), encoding="utf-8")
        r_unwrapped = run_pipeline(f_unwrapped, td / "out_unwrapped", generated_at="2026-08-17T00:00:00")
        assert r_unwrapped["status"] == "error", f"unrecognized dict shape must return a structured error, got: {r_unwrapped}"
        assert "results" in r_unwrapped["message"] or "changeEvents" in r_unwrapped["message"], f"error message should name the missing wrapper key: {r_unwrapped['message']}"
        print(f"[PASS] read_rows: a JSON object without 'results'/'changeEvents' now names the actual problem, not just 'no headers found'")

    with _test_case("mask_rows: the same real identity keeps the same mask label ('…') across separate runs, even wh…"):
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

    with _test_case('TSV input: … rows built (previously untested code path)'):
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

    with _test_case("dict-wrapped JSON ({'results': [...]}) input: … rows built (previously untested code path)"):
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

    with _test_case('unknown_format: correctly stopped for column mapping (unmapped: …)'):
        # --- fixture 3: genuinely unknown header format -> Layer 3 ---
        f3 = td / "unknown.csv"
        f3.write_text(SAMPLE_UNKNOWN_FORMAT_CSV, encoding="utf-8")
        r3 = run_pipeline(f3, td / "out3", generated_at="2026-08-17T00:00:00")
        assert r3["status"] == "needs_mapping", f"fixture3 should need mapping, got: {r3['status']}"
        print(f"[PASS] unknown_format: correctly stopped for column mapping (unmapped: {r3['unmapped_headers']})")

    with _test_case('unknown_format with confirmed mapping: … rows built, profile saved for next time'):
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

    with _test_case('unknown_format second run, no --mapping-file needed: fingerprint match reused the saved profile'):
        # prove the fingerprint (Layer 1) now hits without needing --mapping-file again
        r3c = run_pipeline(f3, td / "out3c", allow_unknown_categories=True, force_review=True, generated_at="2026-08-17T00:00:00")
        assert r3c["status"] == "ok", f"fixture3c (fingerprint reuse) failed: {r3c}"
        print(f"[PASS] unknown_format second run, no --mapping-file needed: fingerprint match reused the saved profile")

    _run_round6_regressions(td)
    _run_round7_cases(td)
    _run_round8_cases(td)


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
    p_run.add_argument("--map", action="append", metavar="FIELD=HEADER", help="Answer a needs_mapping question on the command line: canonical_field=Column header, repeatable (--map timestamp=\"Modified On\" --map old_value=Was). Same effect as --mapping-file, without writing a JSON file.")
    p_run.add_argument("--date-format", choices=["DMY", "MDY", "ISO", "UI_EN"])
    p_run.add_argument("--decimal-style", choices=["TR", "US"])
    p_run.add_argument("--timezone", default="unknown")
    p_run.add_argument("--mask-users", action="store_true")
    p_run.add_argument("--allow-unknown-categories", action="store_true")
    p_run.add_argument("--force-review", action="store_true")
    p_run.add_argument("--extra-rules", help="JSON file: list of extra structured_rules entries to apply on top of CATEGORY_RULES.")
    p_run.add_argument("--account-name", help="Constant account name/id for every row, for a source with no account column at all (e.g. the Google Ads UI's own single-account Change history CSV export).")
    p_run.add_argument("--currency", help="3-letter ISO code (e.g. TRY) of the account's currency. Shown as the unit on budget/bid/target-CPA amounts. Never guessed: without it, amounts stay unit-less. One code for the whole file — don't pass it for a multi-currency MCC export.")
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
            # Fixed 2026-08-18 (4th audit round, confirmed): same structured-
            # error contract as every other malformed-input case in this CLI
            # — a missing/invalid --extra-rules file used to crash with a
            # raw traceback instead of the JSON status envelope every other
            # failure here already prints.
            try:
                with open(args.extra_rules, encoding="utf-8") as f:
                    extra_rules = json.load(f)
            except FileNotFoundError:
                print(json.dumps({"status": "error", "message": f"--extra-rules file not found: {args.extra_rules}"}, indent=2, ensure_ascii=False))
                sys.exit(2)
            except json.JSONDecodeError as e:
                print(json.dumps({"status": "error", "message": f"--extra-rules file is not valid JSON: {args.extra_rules} ({e})"}, indent=2, ensure_ascii=False))
                sys.exit(2)
        try:
            column_map = parse_column_map(args.map)
        except ValueError as e:
            print(json.dumps({"status": "error", "message": str(e)}, indent=2, ensure_ascii=False))
            sys.exit(2)
        result = run_pipeline(
            args.input, args.out_dir, mapping_file=args.mapping_file, column_map=column_map or None, date_format=args.date_format,
            decimal_style=args.decimal_style, tz=args.timezone, mask_users=args.mask_users,
            allow_unknown_categories=args.allow_unknown_categories, force_review=args.force_review,
            extra_rules=extra_rules, open_browser=args.open, account_name=args.account_name, currency=args.currency,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        sys.exit(0 if result.get("status") == "ok" else 2)

    if args.cmd == "query":
        rows = []
        with open(args.input, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        try:
            matched = query_changes(rows, user=args.user, account=args.account, campaign=args.campaign,
                                     category=args.category, operation=args.operation, source=args.source, since=args.since)
        except ValueError as e:
            print(json.dumps({"status": "error", "message": str(e)}, indent=2, ensure_ascii=False))
            sys.exit(2)
        if args.format == "json":
            print(json.dumps({"count": len(matched), "rows": matched}, indent=2, ensure_ascii=False))
        else:
            print(f"{len(matched)} matching change(s)\n")
            for r in matched:
                actor = r.get("user_name") or r.get("user_email") or (f"[{r.get('client_type')}]" if r.get("actor_type") == "automation" else "unknown")
                print(f"{r.get('timestamp')}  {actor:<20}  {r.get('account_name') or '':<20}  {r.get('campaign_name') or '—':<25}  {r.get('category')}/{r.get('subcategory')}  {r.get('old_value')} -> {r.get('new_value')}")


if __name__ == "__main__":
    main()
