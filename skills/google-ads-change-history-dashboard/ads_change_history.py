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
  mapping-profiles/<fingerprint>.json in a per-user data folder
  ($XDG_DATA_HOME or ~/.local/share/google-ads-change-history-dashboard/)
  after the run reaches ok, so the same source never has to be re-confirmed.

  Exit codes: 0 ok, 2 a needs_* question or a structured input error, 3
  internal_error (an unexpected exception, wrapped as JSON — a bug).

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

  change_id is a content hash of the row's identity and content (see
  make_change_id()) — used for DEDUPING (dedupe_rows(), called by
  run_pipeline before categorization): only genuinely identical rows from
  overlapping files collapse; rows within one file are never dropped. The
  count is reported in coverage.txt. event_id is source_event_id when
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
    A spelled-out month (Turkish, English and other UI languages) is never
    ambiguous and is read without asking.
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
    populated). tools/fetch_live_data.py (optional, needs API access)
    writes this shape; a multi-field event whose old/new are resource
    snapshots is split into one row per field here.
  - google_ads_ui_export_en_guess: UNVERIFIED, low confidence — not confirmed
    against a real UI export in this session. Deliberately weak so a real
    export safely falls through to Layer 3 (ask+learn) instead of silently
    misapplying a guess.

REPO / PRIVACY NOTES
  --mask-users defaults OFF (raw user identity shown). With it, a human
  actor becomes "User A"/"User B" (stable across runs — the identity->label
  table is mask-labels.json in the per-user data folder, never to be
  shared), and e-mail-shaped text and known identities are scrubbed from
  every string field of every output file (summary and values included).
  NOT masked, with or without the flag: account, campaign and ad group
  names, account/campaign/ad-group IDs, resource paths and event IDs. Showing those is the point of the tool; a
  report that must hide the client's identity needs more than this flag.

USAGE
  python3 ads_change_history.py run <input.csv|.json> --out-dir ./out \\
      [--account-name "Account A"] [--timezone Europe/Istanbul] \\
      [--date-format DMY] [--decimal-style TR] [--currency TRY] [--encoding cp1254] \\
      [--mapping-file confirmed_mapping.json | --map FIELD="Header" ...] \\
      [--mask-users] [--open]

  python3 ads_change_history.py query <out/changes.jsonl> --user "User A" --since 7d [--since-anchor newest]

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
import io
import json
import os
import re
import string
import sys
import unicodedata
import webbrowser
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta, timezone as dt_timezone
from difflib import get_close_matches
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent


def _default_state_dir():
    """2026-11-10 (B-8): mapping profiles + mask labels live in a per-user data
    dir, so a read-only skill install still works; skill dir only as fallback."""
    import os
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    user_dir = Path(base) / "google-ads-change-history-dashboard" / "mapping-profiles"
    for cand in (user_dir, SCRIPT_DIR / "mapping-profiles"):
        probe = cand
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        if os.access(str(probe), os.W_OK):
            return cand
    return user_dir


PROFILES_DIR = _default_state_dir()
PROFILE_SCHEMA_VERSION = 2  # profiles without this stamp are ignored (2026-11-10)
INPUT_ENCODING = None  # set by --encoding; None = auto-detect (2026-11-10, L-1)
CSV_FIELD_SIZE_LIMIT = 64 * 1024 * 1024
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
        # Native-export headers in de/es/fr/pt/it/nl added 2026-11-10 (L-7).
        "timestamp": ["Tarih", "Change Date", "Change Date/Time", "Date", "change_date_time", "Değişiklik Tarihi", "Date & time", "Change date", "Zaman",
                      "Tarih ve saat", "Date and time", "Datum und Uhrzeit", "Datum", "Fecha y hora", "Fecha", "Date et heure",
                      "Data e hora", "Data", "Data e ora", "Datum en tijd"],
        "account_id": ["Hesap ID", "Account ID", "Customer ID", "customer_id", "external_customer_id"],
        # Fixed 2026-10-10 (6th audit round): account_name/campaign_name/
        # ad_group_name were each missing their own literal snake_case name —
        # the same gap "user_name" had below. A ChangeEvent export joined
        # with display names (customer_id + account_name, campaign +
        # campaign_name, ...) mapped the ID/path columns and silently dropped
        # all three name columns, so every row rendered as "Campaign <id>".
        "account_name": ["account_name", "Hesap Adı", "Account", "Account name", "Client", "Hesap", "Konto", "Cuenta", "Compte", "Conta"],
        # Fixed 2026-08-18 (found via a synthetic-dataset dry run, not a
        # pasted audit): "user_name" itself was missing from its own alias
        # list — user_email/client_type both already self-reference (their
        # alias lists include their own literal snake_case name), user_name
        # didn't. A source using that literal column name (e.g. a JSON export
        # naming an automation script via "user_name") mapped nothing —
        # actor_type silently fell back to "unknown" for every such row
        # instead of correctly resolving to "automation".
        "user_name": ["user_name", "Kullanıcı", "Changed by", "User", "Değiştiren", "Değiştiren Kullanıcı",
                      "Nutzer", "Benutzer", "Geändert von", "Usuario", "Modificado por", "Utilisateur", "Modifié par",
                      "Usuário", "Utilizador", "Alterado por", "Utente", "Modificato da", "Gebruiker", "Gewijzigd door"],
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
        "campaign_name": ["campaign_name", "Campaign", "Kampanya", "campaign.name", "Kampagne", "Campaña", "Campagne", "Campanha", "Campagna"],
        "ad_group_id": ["Ad group ID", "Reklam Grubu ID", "ad_group.id"],
        "ad_group_name": ["ad_group_name", "Ad group", "Reklam Grubu", "ad_group.name", "Anzeigengruppe", "Grupo de anuncios",
                          "Groupe d'annonces", "Grupo de anúncios", "Gruppo di annunci", "Advertentiegroep"],
        # Singular "Change"-style headers only map here when the column really
        # holds operation words; free text goes to raw_summary (2026-11-10, D-4).
        "operation": ["resource_change_operation", "Change", "Değişiklik", "Operation", "Änderung", "Cambio", "Modification", "Alteração", "Modifica", "Wijziging"],
        "field_name": ["changed_fields", "Field", "Alan", "Değişen Alan"],
        "old_value": ["old_resource", "Eski Teknik Veri", "Before", "Old value", "Önceki", "Old"],
        "new_value": ["new_resource", "Yeni Teknik Veri", "After", "New value", "Yeni", "New"],
        # "Changes" added 2026-08-18 — the real Google Ads UI's own
        # "Change history report" CSV export (Campaigns > Change history >
        # Download) uses this exact column name, verified against a real
        # export this session. It's a multi-line free-text cell that can
        # bundle several distinct changes into one row (see the module
        # docstring's note on this format's limits).
        "raw_summary": ["Değişiklik Özeti (Nereden -> Nereye)", "Değişiklik Özeti", "Change summary", "Summary", "Description", "Changes",
                        "Değişiklikler", "Änderungen", "Cambios", "Modifications", "Alterações", "Modifiche", "Wijzigingen"],
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
    # Added 2026-11-10 (D-3/A-1..A-6): checked first, per statement. A rule
    # matches when its headline pattern matches line 1 AND its detail pattern
    # matches one of the indented lines below it.
    "summary_detail_rules": [
        {"headline": r"^Kampanya öğesi değiştirildi", "detail": r"olan Durum, Kaldırıldı olarak", "category": "Asset", "subcategory": "Asset removed"},
        {"headline": r"(değiştirildi|changed)$", "detail": r"(olan Durum, Duraklatıldı olarak|^Status changed from \S+ to Paused)", "category": "Status", "subcategory": "Paused"},
        {"headline": r"(değiştirildi|changed)$", "detail": r"(olan Durum, Etkin olarak|^Status changed from \S+ to (Enabled|Active))", "category": "Status", "subcategory": "Enabled"},
        {"headline": r"(değiştirildi|changed)$", "detail": r"(olan Durum, Kaldırıldı olarak|^Status changed from \S+ to Removed)", "category": "Status", "subcategory": "Removed"},
        {"headline": r"^(Kampanya değiştirildi|Campaign changed)", "detail": r"(Hedef EBM|Target CPA)", "category": "Bidding", "subcategory": "Target CPA"},
        {"headline": r"^(Kampanya değiştirildi|Campaign changed)", "detail": r"(Hedef ROAS|Target ROAS)", "category": "Bidding", "subcategory": "Target ROAS"},
        {"headline": r"^(Kampanya değiştirildi|Campaign changed)", "detail": r"(teklif stratejisi|Teklif sınırı|Tıklama sayısını|Dönüşüm sayısını|Dönüşüm değerini|bid strategy|bidding strategy|Bid limit|Maximi[sz]e (clicks|conversions))", "category": "Bidding", "subcategory": "Strategy changed"},
    ],
    "summary_text_rules": [
        {"pattern": r"^Yeni Kelime Eklendi", "category": "Keyword", "subcategory": "Keyword added"},
        {"pattern": r"^Negatif Kelime Eklendi", "category": "Keyword", "subcategory": "Negative keyword"},
        {"pattern": r"^(TBM Değişti|AdGroup TBM Değişti|Teklif Değişti)", "category": "Bidding", "subcategory": "Bid changed"},
        {"pattern": r"^(Strateji Değişti|Teklif Stratejisi)", "category": "Bidding", "subcategory": "Strategy changed"},
        {"pattern": r"^Bütçe Değişti", "category": "Budget", "subcategory": "Budget changed"},
        # F-1 fixed 2026-11-10: "A -> B" is categorized by B (the new state), not A.
        {"pattern": r"^Durum Değişti.*->\s*\W*(ENABLED|Etkin)", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^Durum Değişti.*->\s*\W*(PAUSED|Duraklat)", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^Durum Değişti.*->\s*\W*(REMOVED|Kaldırıldı)", "category": "Status", "subcategory": "Removed"},
        {"pattern": r"^Durum Değişti(?!.*->).*(ENABLED|Etkin)", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^Durum Değişti(?!.*->).*(PAUSED|Duraklat)", "category": "Status", "subcategory": "Paused"},
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
        {"pattern": r"^\d+ (age ranges?|genders?|household incomes?|parental status(es)?) (added|removed|changed)", "category": "Audience", "subcategory": "Audience added"},
        {"pattern": r"^\d+ languages? (added|removed)", "category": "Targeting", "subcategory": "Language targeting changed"},
        {"pattern": r"^\d+ (countr(y|ies)|regions?|cit(y|ies)|locations?) (added|removed)", "category": "Targeting", "subcategory": "Location targeting changed"},
        {"pattern": r"^\d+ (devices?|platforms?) (added|removed|changed)", "category": "Targeting", "subcategory": "Device targeting changed"},
        {"pattern": r"^\d+ bid adjustments? changed", "category": "Bidding", "subcategory": "Bid modifier changed"},
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
        # 2026-11-10: platform and ad-schedule adjustments are bid modifiers, as structured AD_GROUP_BID_MODIFIER.
        {"pattern": r"^\d+ teklif ayarlama değişikliği", "category": "Bidding", "subcategory": "Bid modifier changed"},
        {"pattern": r"^\d+ reklam grubu etkin", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^\d+ reklam grubu duraklat", "category": "Status", "subcategory": "Paused"},
        {"pattern": r'^Bütçe ".*" olarak ayarlandı', "category": "Budget", "subcategory": "Budget changed"},
        {"pattern": r"^Reklam grubu oluşturuldu", "category": "AdGroup", "subcategory": "Ad group created"},
        {"pattern": r"^Reklam grubu değiştirildi", "category": "AdGroup", "subcategory": "Ad group settings changed"},
        {"pattern": r"^Reklam grubu kaldırıldı", "category": "AdGroup", "subcategory": "Ad group removed"},
        {"pattern": r"^\d+ duyarlı arama ağı reklamı oluşturuldu", "category": "Ad", "subcategory": "Ad created"},
        {"pattern": r"^\d+ duyarlı arama ağı reklamı değiştirildi", "category": "Ad", "subcategory": "Ad changed"},
        {"pattern": r"^\d+ duyarlı arama ağı reklamı kaldırıldı", "category": "Ad", "subcategory": "Ad removed"},
        {"pattern": r"^\d+ [Dd]uyarlı görüntülü reklam oluşturuldu", "category": "Ad", "subcategory": "Ad created"},
        {"pattern": r"^\d+ [Dd]uyarlı görüntülü reklam değiştirildi", "category": "Ad", "subcategory": "Ad changed"},
        {"pattern": r"^\d+ [Dd]uyarlı görüntülü reklam kaldırıldı", "category": "Ad", "subcategory": "Ad removed"},
        {"pattern": r"^Öğe (oluşturuldu|eklendi)", "category": "Asset", "subcategory": "Asset added"},
        {"pattern": r"^Öğe değiştirildi", "category": "Asset", "subcategory": "Asset changed"},
        # Matched against the statement headline only (2026-11-10), so a detail line can't trigger it.
        {"pattern": r"negatif (anahtar kelime listesi uygulandı|(geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? (eklendi|listeye eklendi|kaldırıldı))", "category": "Keyword", "subcategory": "Negative keyword"},
        {"pattern": r"^\d+ (geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? eklendi", "category": "Keyword", "subcategory": "Keyword added"},
        {"pattern": r"^\d+ (geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? kaldırıldı", "category": "Keyword", "subcategory": "Keyword removed"},
        {"pattern": r"^\d+ (geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? duraklatıldı", "category": "Status", "subcategory": "Paused"},
        {"pattern": r"^\d+ (geniş|sıralı|tam) eşlemeli anahtar kelime(ler)? etkin", "category": "Status", "subcategory": "Enabled"},
        {"pattern": r"^\d+ dil(ler)? (eklendi|kaldırıldı)", "category": "Targeting", "subcategory": "Language targeting changed"},
        {"pattern": r"^\d+ (ülke(ler)?|bölge(ler)?|şehir(ler)?|konum(lar)?) (eklendi|kaldırıldı)", "category": "Targeting", "subcategory": "Location targeting changed"},
        {"pattern": r"^\d+ (platform(lar)?|cihaz(lar)?) (eklendi|kaldırıldı|değiştirildi)", "category": "Targeting", "subcategory": "Device targeting changed"},
        {"pattern": r"^\d+ (yaş aralık(ları)?|cinsiyet(ler)?|hane geliri|ebeveynlik durumu) (eklendi|kaldırıldı|değiştirildi)", "category": "Audience", "subcategory": "Audience added"},
        {"pattern": r"(Hesap varsayılan hedefleri|kampanyaya özel hedef)", "category": "Conversion", "subcategory": "Conversion tracking changed"},
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
        # 2026-11-10: a campaign-specific custom goal is a conversion goal, not an audience (A-3).
        {"pattern": r"adlı kampanya özel hedefi (eklendi|değiştirildi|kaldırıldı)", "category": "Conversion", "subcategory": "Conversion tracking changed"},
    ],
    "fallback": {"category": "Other", "subcategory": "Other"},
}

# L-8 fixed 2026-11-10: whole words only ("Abbott", "Kapil", "Capital One" are people).
AUTOMATION_USER_PATTERNS = re.compile(
    r"(^ads-|\b(system|automated|automation|rules?|scripts?|bot|api|otomatik|otomasyon|kural|automático|automática|automatique|automatisch|automatico|Skript|скрипт|автоматическ\w*)\b)",
    re.IGNORECASE,
)
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
# B-9 2026-11-10: every summary regex runs on one line capped at SUMMARY_LINE_CAP chars.
SUMMARY_LINE_CAP = 2000
SUMMARY_OLD_NEW_RE = re.compile(r"(?<![\d.,])(-?\d[\d.,]{0,30})\s{0,5}->\s{0,5}(-?\d[\d.,]{0,30})")
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
# 2026-11-10: also düşürüldü/yükseltildi, currency before or after the number, quoted text values.
_TR_AMOUNT = r"[₺$€£]?\s?-?\d[\d.,\u00a0\u202f']{0,30}(?:\s?(?:₺|TL|€|\$|£))?"
SUMMARY_TR_OLAN_DEGERINE_RE = re.compile(
    r"(?<![\w.,])(" + _TR_AMOUNT + r")\s+olan\s+[^,\n]{1,200},\s*(" + _TR_AMOUNT + r")\s+(?:olarak değiştirildi|değerine (?:artırıldı|azaltıldı|düşürüldü|yükseltildi|değiştirildi))"
)
SUMMARY_TR_OLAN_TEXT_RE = re.compile(r'^\s*(?:[^:\n]{1,200}:\s*)?("[^"\n]{0,500}"|\S{1,60})\s+olan\s+[^,\n]{1,200},\s*("[^"\n]{0,500}"|\S{1,60})\s+olarak değiştirildi', re.IGNORECASE)
# old-only "X değeri ₺N idi" and new-only percent "X %+10 olarak ayarlandı" (A-4/D-8).
SUMMARY_TR_OLD_ONLY_RE = re.compile(r"değeri\s+(" + _TR_AMOUNT + r")\s+idi\b")
SUMMARY_PERCENT_SET_RE = re.compile(r"(?:%\s?([+-]?\d[\d.,]{0,10})|([+-]?\d[\d.,]{0,10})\s?%)\s+(?:olarak ayarlandı|set)", re.IGNORECASE)
SUMMARY_NEW_AMOUNT_RE = re.compile(r"(?:Bütçe tutarı(?: değeri)?|Budget amount|Amount)\s*:\s*(" + _TR_AMOUNT + r")", re.IGNORECASE)
CURRENCY_SYMBOLS = (("₺", "TRY"), ("TL", "TRY"), ("TRY", "TRY"), ("€", "EUR"), ("EUR", "EUR"), ("£", "GBP"), ("GBP", "GBP"), ("$", "USD"), ("USD", "USD"))
# A-5: "<Campaign>: ₺N olan Bütçe tutarı, ..." / "<Campaign>: Bütçe tutarı değeri: ₺N".
SUMMARY_BUDGET_OWNER_RE = re.compile(r"^\s+([^:\n]{1,200}?):\s*(?:\S{1,40}\s+olan\s+)?(?:Bütçe tutarı|Budget amount)", re.IGNORECASE)
# L-6: words that show the summary is in a UI language these rules know (TR/EN).
KNOWN_SUMMARY_VOCAB_RE = re.compile(r"(değiştirildi|oluşturuldu|eklendi|kaldırıldı|duraklatıldı|etkin|uygulandı|ayarlandı|değişikliği|Değişti|changed|created|added|removed|paused|enabled|active|applied|set to|updated)", re.IGNORECASE)
# -? prefix added 2026-08-18 (3rd audit round, confirmed): without it, a
# negative value like "-150.00" or "-3,50" matched neither pattern (both
# required the string to START with a digit), fell through to
# normalize_number()'s catch-all branch, and had its separator silently
# stripped instead of converted — "-150.00" became -15000.0, not -150.0. The
# sign character sits before the digits either way, so adding "-?" is
# sufficient; the replace() calls downstream don't touch it.
DIGIT_GROUP_3 = re.compile(r"^[+-]?\d{1,3}([.,]\d{3})+$")
DIGIT_GROUP_SHORT = re.compile(r"^[+-]?\d+[.,]\d{1,2}$")
SINCE_RE = re.compile(r"^\d+[dwm]$")
EMAIL_RE = re.compile(r"[\w.+-]{1,64}@[\w-]{1,63}(?:\.[\w-]{1,63})+")


# =====================================================================
# STAGE 1-2: SOURCE DETECTION + VALIDATION
# =====================================================================
def normalize_header(h):
    return " ".join(h.strip().lower().split())


def read_rows(path, info=None, encoding=None):
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
    if path.is_dir():
        raise ValueError(f"Input path is a directory, not a file: {path}")
    raw = path.read_bytes()
    for magic, what in _MAGIC_SIGNATURES:
        if raw.startswith(magic):
            raise ValueError(f"{path} is {what}, not CSV/JSON. Open it and export/save it as CSV (UTF-8), then re-run on the .csv file.")

    if path.suffix.lower() == ".json":
        try:
            data = json.loads(_decode_input(path, raw, encoding)[0].lstrip("\ufeff"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Input file is not valid JSON: {path} ({e})")
        data = _scrub_surrogates(data)
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

    text, enc_used = _decode_input(path, raw, encoding)
    text = text.replace("\x00", "").replace("\ufeff", "")
    if path.suffix.lower() == ".tsv":
        delimiter = "\t"
    else:
        delimiter = _sniff_delimiter(text)
    csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)
    try:
        all_rows = list(csv.reader(io.StringIO(text, newline=""), delimiter=delimiter))
    except csv.Error as e:
        if "field larger than field limit" in str(e):
            raise ValueError(f"A cell in {path} is larger than {CSV_FIELD_SIZE_LIMIT // (1024 * 1024)} MB — the file is probably not a real CSV export (or a quote is unbalanced).")
        raise ValueError(f"Could not parse {path} as CSV: {e}")
    hdr_idx = _find_header_row(all_rows)
    if info is not None:
        info.update({"encoding": enc_used, "delimiter": delimiter, "preamble_rows_skipped": hdr_idx})
    if hdr_idx is None:
        return [], []
    headers = [h.strip() for h in all_rows[hdr_idx]]
    rows = []
    for rec in all_rows[hdr_idx + 1:]:
        if not rec:
            continue  # same as csv.DictReader: blank lines are not rows
        d = dict(zip(headers, rec))
        if len(rec) > len(headers):
            d[None] = rec[len(headers):]
        elif len(rec) < len(headers):
            for h in headers[len(rec):]:
                d.setdefault(h, None)
        rows.append(d)
    return headers, rows


_MAGIC_SIGNATURES = (
    (b"PK\x03\x04", "an Excel .xlsx / zip archive"),
    (b"\x1f\x8b", "a gzip archive"),
    (b"\xd0\xcf\x11\xe0", "a legacy Excel .xls file"),
)
# Mojibake tells: C1 controls, or UTF-8 bytes read as a single-byte code page.
_MOJIBAKE_RE = re.compile("[\u0080-\u009f]|[\u00c3\u00c4\u00c5\u00c2][\u0080-\u00bf]|\u00c4\u00b1|\u00c5\u0178|\u00c3\u00bc")


def _decode_input(path, raw, encoding=None):
    """2026-11-10 (L-1/L-2/T-7): BOM-aware decode; cp1254/latin-1 fallbacks are
    rejected when the result looks like mojibake. Returns (text, encoding)."""
    encoding = encoding or INPUT_ENCODING
    if encoding:
        try:
            return raw.decode(encoding), encoding
        except (UnicodeDecodeError, LookupError) as e:
            raise ValueError(f"Could not decode {path} with --encoding {encoding}: {e}")
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return raw.decode("utf-16"), "utf-16"
        except UnicodeDecodeError as e:
            raise ValueError(f"{path} has a UTF-16 byte-order mark but is not valid UTF-16: {e}")
    if raw[:2000].count(b"\x00") > len(raw[:2000]) // 4:
        # UTF-16 without a BOM: NUL bytes sit on the odd (LE) or even (BE) side.
        sample = raw[:2000]
        le = sample[1::2].count(0) >= sample[0::2].count(0)
        try:
            return raw.decode("utf-16-le" if le else "utf-16-be"), ("utf-16-le" if le else "utf-16-be")
        except UnicodeDecodeError:
            pass
    try:
        return raw.decode("utf-8-sig"), "utf-8"
    except UnicodeDecodeError:
        pass
    for enc in ("cp1254", "latin-1"):
        try:
            text = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        letters = [c for c in text[:20000] if c.isalpha()]
        non_ascii = sum(1 for c in letters if ord(c) > 127)
        if not _MOJIBAKE_RE.search(text) and non_ascii <= 0.4 * max(len(letters), 1):
            return text, enc
    raise ValueError(f"Could not decode {path}: not UTF-8/UTF-16, and the cp1254/latin-1 fallbacks produce garbled text. Re-export it as CSV (UTF-8), or pass --encoding <codec> (e.g. --encoding cp1251).")


def _sniff_delimiter(text):
    """2026-11-10 (L-3): ',' ';' or tab — whichever gives the most rows with the
    same (largest) column count in the first lines; ',' wins ties."""
    head = text[:65536]
    best, best_score = ",", -1
    for d in (",", ";", "\t"):
        try:
            recs = list(csv.reader(io.StringIO(head, newline=""), delimiter=d))[:40]
        except csv.Error:
            continue
        counts = Counter(len(r) for r in recs if len(r) > 1)
        if not counts:
            continue
        width, n = max(counts.items(), key=lambda kv: (kv[1], kv[0]))
        score = n * 1000 + width
        if score > best_score:
            best, best_score = d, score
    return best


def _known_alias_set():
    out = set()
    for alias_list in HEADER_ALIASES["aliases"].values():
        out.update(normalize_header(a) for a in alias_list)
    return out


def _find_header_row(all_rows, scan=30):
    """2026-11-10 (T-1/B-6): native exports start with title/date-range lines.
    The header is the first row with >=2 known aliases; else the first row
    that has the file's most common width (>1 column); else row 0."""
    if not all_rows:
        return None
    aliases = _known_alias_set()
    head = all_rows[:scan]
    for i, rec in enumerate(head):
        if sum(1 for c in rec if normalize_header(c.replace("\ufeff", "")) in aliases) >= 2:
            return i
    counts = Counter(len(r) for r in all_rows if len(r) > 1)
    if counts:
        width = max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]
        for i, rec in enumerate(head):
            if len(rec) == width:
                return i
    return 0


def _scrub_surrogates(obj):
    """Lone surrogates (legal in JSON, illegal in UTF-8 output) -> U+FFFD (2026-11-10, S-7)."""
    if isinstance(obj, str):
        return obj.encode("utf-8", "replace").decode("utf-8") if any("\ud800" <= ch <= "\udfff" for ch in obj) else obj
    if isinstance(obj, list):
        return [_scrub_surrogates(x) for x in obj]
    if isinstance(obj, dict):
        return {_scrub_surrogates(k): _scrub_surrogates(v) for k, v in obj.items()}
    return obj


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
        try:
            with open(profile_path, encoding="utf-8") as f:
                profile = json.load(f)
        except (OSError, ValueError):
            return None  # unreadable cache is ignored, never fatal (2026-11-10)
        if not isinstance(profile, dict) or profile.get("schema_version") != PROFILE_SCHEMA_VERSION or not isinstance(profile.get("mapping"), dict):
            return None  # stale or foreign profile (2026-11-10, B-8)
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


_CLIENT_TYPE_VALUE_RE = re.compile(r"^[A-Z0-9]+(_[A-Z0-9]+)+$")


def _column_is_operation_words(rows, header, n=50):
    """2026-11-10 (D-4/F-3/T-6): a "Change"-titled column is an operation column
    only if its values are operation words; multi-line free text is a summary."""
    vals = sample_values(rows or [], header, n)
    if not vals:
        return True
    if any("\n" in v for v in vals):
        return False
    known = sum(1 for v in vals if apply_value_alias("operation", v) in ("CREATE", "UPDATE", "REMOVE") or re.fullmatch(r"[A-Z_]+", v.strip()))
    wordy = sum(1 for v in vals if len(v.split()) > 2)
    return known >= 0.5 * len(vals) or wordy < 0.5 * len(vals)


def try_alias_match(headers, rows=None, warnings=None):
    alias_lookup = defaultdict(list)
    for canonical, alias_list in HEADER_ALIASES["aliases"].items():
        for a in alias_list:
            if canonical not in alias_lookup[normalize_header(a)]:
                alias_lookup[normalize_header(a)].append(canonical)
    warnings = warnings if warnings is not None else []
    mapping, unmatched = {}, []
    for h in headers:
        if h in API_EXACT_CASE_RESOURCE_ALIASES and _column_looks_like_resource_path(rows, h):
            mapping[API_EXACT_CASE_RESOURCE_ALIASES[h]] = h
            continue
        cands = alias_lookup.get(normalize_header(h))
        if not cands:
            unmatched.append(h)
            continue
        canonical = cands[0]
        if len(cands) > 1:
            # 2026-11-10 (L-10): "Client" is an account in MCC exports, a client type in API data.
            vals = sample_values(rows or [], h, 20)
            canonical = "client_type" if vals and all(_CLIENT_TYPE_VALUE_RE.match(v.strip()) for v in vals) and "client_type" in cands else [c for c in cands if c != "client_type"][0]
            warnings.append(f"Column {h!r} could mean {cands}; read it as {canonical}. Pass --map to change that.")
        if canonical == "operation" and not _column_is_operation_words(rows, h):
            canonical = "raw_summary"
        if canonical in mapping:
            warnings.append(f"Columns {mapping[canonical]!r} and {h!r} both look like {canonical}; using {mapping[canonical]!r}. Pass --map {canonical}=\"...\" to choose.")
            unmatched.append(h)
            continue
        mapping[canonical] = h
    return mapping, unmatched


def save_mapping_profile(profile):
    """Writes a human-confirmed mapping profile; returns a list of warnings, never
    raises (2026-11-10, B-8: called only once a run reached ok)."""
    if not profile:
        return []
    try:
        PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        with open(PROFILES_DIR / f"{profile['fingerprint']}.json", "w", encoding="utf-8") as f:
            json.dump({k: v for k, v in profile.items() if k != "fingerprint"}, f, indent=2, ensure_ascii=False)
    except OSError as e:
        return [f"Could not save the confirmed mapping to {PROFILES_DIR} ({e.strerror or e}); it will be asked again next time."]
    return []


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


_SAMPLE_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")


def sample_values(rows, header, n=3):
    # 2026-11-10 (S-3): samples are shown in chat; e-mail addresses are redacted.
    vals = []
    for r in rows:
        v = r.get(header)
        if v is not None and str(v).strip():
            vals.append(_SAMPLE_EMAIL_RE.sub("<email>", str(v)))
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
    read_info = {}
    try:
        headers, rows = read_rows(input_path, info=read_info)
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
    map_warnings = []
    if read_info.get("preamble_rows_skipped"):
        map_warnings.append(f"Skipped {read_info['preamble_rows_skipped']} title/preamble line(s) before the header row.")
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
        except (OSError, UnicodeDecodeError) as e:
            return {"status": "error", "message": f"Mapping file could not be read: {mapping_file} ({e})"}
        # 2026-11-10 (S-7): a non-object mapping used to crash with AttributeError.
        confirmed = _scrub_surrogates(confirmed)
        mapping = confirmed.get("mapping", confirmed) if isinstance(confirmed, dict) else None
        if not isinstance(mapping, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()):
            return {"status": "error", "message": f"Mapping file must be a JSON object of canonical_field -> column header strings (optionally under a 'mapping' key): {mapping_file}"}
        source_label = str(confirmed.get("source_label", "user_confirmed"))
    else:
        profile = try_fingerprint_match(headers)
        if profile:
            mapping = profile["mapping"]
            source_label = profile.get("source_label", "fingerprint_match")
        else:
            mapping, unmatched = try_alias_match(headers, rows, map_warnings)
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
        # 2026-11-10 (D-4/F-3): an explicitly mapped header belongs to that field only.
        explicit = set(column_map.values())
        mapping = {k: h for k, h in mapping.items() if h not in explicit}
        mapping = {**mapping, **column_map}
        unmatched = [h for h in unmatched if h not in column_map.values()]
        if source_label in ({"alias_match", "unmapped_confirmed"} | set(HEADER_ALIASES["known_sources"])):
            source_label = "user_confirmed"

    if mapping.get("operation") and mapping.get("operation") == mapping.get("raw_summary") and "operation" not in (column_map or {}):
        mapping = {k: h for k, h in mapping.items() if k != "operation"}  # free text is not an operation (D-4)
    missing = check_hard_required(mapping, default_account_name=default_account_name)
    if missing:
        no_account = "account_name" not in mapping and "account_id" not in mapping
        instr = ("Show unmapped_headers + sample_values to the user in chat. Ask which canonical field each maps to (see module docstring: CANONICAL SCHEMA). "
                 "Re-run with one --map canonical_field=\"Column header\" per answer (or put them in a JSON file, {'source_label': ..., 'mapping': {canonical: header}}, and pass --mapping-file <file>).")
        if "resource_type" in missing:
            instr += " resource_type is only needed when there is no free-text change-summary column: mapping that column as raw_summary satisfies it."
        if no_account and not default_account_name:
            instr += " The file has no account column (normal for a single-account Google Ads UI export): pass --account-name \"<account>\"."
        return {
            "status": "needs_mapping",
            "input": str(input_path),
            "fingerprint": fingerprint(headers),
            "auto_mapped": mapping,
            "missing_required": missing,
            "unmapped_headers": unmatched,
            "sample_values": {h: sample_values(rows, h) for h in unmatched},
            "suggested_mapping": suggest_fuzzy_mapping(unmatched),
            "instructions": instr,
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

    # 2026-11-10 (B-8): the profile is only handed back here; run_pipeline saves
    # it once the whole run reached ok, so a failed run never caches a mapping.
    profile = None
    if save_profile and (mapping_file or column_map):
        profile = {"schema_version": PROFILE_SCHEMA_VERSION, "fingerprint": fingerprint(headers), "source_label": source_label,
                   "confirmed_by": "mapping_file" if mapping_file else "map_flag", "mapping": mapping, "headers_seen": headers}

    return {
        "status": "ok", "input": str(input_path), "row_count": len(rows),
        "fingerprint": fingerprint(headers), "source_label": source_label,
        "mapping": mapping,
        # Fixed 2026-10-10 (6th audit round): unmapped_headers was only ever
        # reported on the needs_mapping path. On an "ok" run a source column
        # no alias matched just vanished — which is how three fully-populated
        # name columns went missing with nothing anywhere saying so.
        "ignored_headers": [h for h in headers if h not in set(mapping.values())],
        "_profile": profile,
        "warnings": map_warnings + soft_warnings(mapping) + ([f"Duplicate column header(s) detected: {dup_headers} — only the last occurrence of each is used (stdlib csv.DictReader behavior), earlier columns of the same name are silently discarded."] if dup_headers else []),
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
# L-9 2026-11-10: trailing currency symbols accepted; B-9: digits capped at 40.
NUMERIC_VALUE_RE = re.compile(
    r"^([+-])?\s*(?:[₺$€£¥%]|TRY|TL|USD|EUR|GBP)?\s*([+-]?\d[\d.,]{0,40})\s*(?:%|[₺$€£¥]|[^\W\d_]{1,4})?(?:/[^\W\d_]{1,5})?$"
)
_DIGIT_GROUP_SPACE_RE = re.compile(r"(?<=\d)[\u00a0\u202f\u2009' ](?=\d{3}(?!\d))")


def _numeric_core(raw):
    """The bare digits/separators/sign of a numeric-looking value, or None
    when the value is text that merely contains digits."""
    if raw is None:
        return None
    text = str(raw).strip()
    if len(text) > 64:
        return None
    m = NUMERIC_VALUE_RE.match(_DIGIT_GROUP_SPACE_RE.sub("", text))
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
        # 2026-11-10: "0,001" is never a thousands group; a TR comma is the decimal mark.
        if DIGIT_GROUP_SHORT.match(s) or (s.count(sep) == 1 and DIGIT_GROUP_3.match(s) and (s.lstrip("+-").split(sep)[0] == "0" or (decimal_style == "TR" and sep == ","))):
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
    for ch in ("\u200e", "\u200f", "\u061c", "\ufeff"):  # bidi marks (2026-11-10, L-5)
        s = s.replace(ch, "")
    return s


YEAR_FIRST_DATE_RE = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}")
SLASH_DATE_RE = re.compile(r"^(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})")

# 2026-11-10 (T-1/L-4): spelled-out month names, keyed accent-free and lowercase.
_MONTH_NAMES = {}
for _num, _names in enumerate((
    "oca ocak jan january januar jän janner januari ene enero janv janvier janeiro gen gennaio",
    "şub şubat feb february februar febrero fév févr février fev fevereiro febbraio februari",
    "mar mart march mär märz mrz marzo mars março mrt maart",
    "nis nisan apr april abr abril avr avril aprile",
    "may mayıs mayis mai mayo maio mag maggio mei",
    "haz haziran jun june juni junio juin junho giu giugno",
    "tem temmuz jul july juli julio juil juillet julho lug luglio",
    "ağu ağustos aug august ago agosto août aout augustus",
    "eyl eylül sep sept september septiembre set septembre setembro settembre",
    "eki ekim oct october okt oktober octubre octobre out outubro ott ottobre",
    "kas kasım kasim nov november noviembre novembre novembro",
    "ara aralık aralik dec december dez dezember dic diciembre déc décembre dezembro dicembre",
), start=1):
    for _n in _names.split():
        _MONTH_NAMES[unicodedata.normalize("NFD", _n).encode("ascii", "ignore").decode().replace("ı", "i")] = _num
_DATE_FILLER_WORDS = {"de", "del", "a", "à", "las", "um", "às", "as", "at", "alle", "om", "saat", "le", "el", "o"}
_TEXT_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})(?::(\d{2}))?(?:[.,](\d+))?")
_AMPM_RE = re.compile(r"(?<![^\W\d_])([ap])\.?\s?m\.?(?![^\W\d_])|(?<![^\W\d_])(öö|ös)(?![^\W\d_])", re.IGNORECASE)
_TEXT_OFFSET_RE = re.compile(r"\s*(?:gmt|utc)?\s*([+-])(\d{1,2}):?(\d{2})?\s*$", re.IGNORECASE)


def _month_key(word):
    w = word.replace("İ", "i").replace("I", "ı").lower().replace("ı", "i")
    return unicodedata.normalize("NFD", w).encode("ascii", "ignore").decode().rstrip(".")


def parse_text_month_timestamp(raw):
    """'11 Ağu 2026 02:33:50', 'Aug 16, 2026, 10:42:26 PM', '16. Aug. 2026',
    '16 de ago. de 2026 22:42', '16 août 2026 à 22:42' -> datetime (aware if an
    offset is given). Raises ValueError; never guesses a leftover word."""
    s = _normalize_spaces(str(raw).strip())
    tz = None
    m = _TEXT_OFFSET_RE.search(s)
    if m and _TEXT_TIME_RE.search(s[:m.start()]):
        sign = 1 if m.group(1) == "+" else -1
        tz = dt_timezone(sign * timedelta(hours=int(m.group(2)), minutes=int(m.group(3) or 0)))
        s = s[:m.start()]
    ampm = None
    am = _AMPM_RE.search(s)
    if am:
        tag = (am.group(1) or am.group(2)).lower()
        ampm = "am" if tag in ("a", "öö") else "pm"
        s = s[:am.start()] + " " + s[am.end():]
    hh = mi = ss = us = 0
    tm = _TEXT_TIME_RE.search(s)
    if tm:
        hh, mi, ss = int(tm.group(1)), int(tm.group(2)), int(tm.group(3) or 0)
        if tm.group(4):
            us = int((tm.group(4) + "000000")[:6])
        s = s[:tm.start()] + " " + s[tm.end():]
    month = day = year = None
    numbers = []
    for tok in re.findall(r"\d+|[^\W\d_]+\.?", s):
        if tok[0].isdigit():
            numbers.append(tok)
            continue
        key = _month_key(tok)
        if key in _MONTH_NAMES and month is None:
            month = _MONTH_NAMES[key]
        elif key not in _DATE_FILLER_WORDS and tok.lower().rstrip(".") not in _DATE_FILLER_WORDS:
            raise ValueError(f"unparseable timestamp: {raw}")
    if month is None or len(numbers) != 2:
        raise ValueError(f"unparseable timestamp: {raw}")
    years = [n for n in numbers if len(n) == 4]
    if len(years) != 1:
        raise ValueError(f"unparseable timestamp: {raw}")
    year = int(years[0])
    day = int([n for n in numbers if n is not years[0]][0])
    if ampm:
        if not 1 <= hh <= 12:
            raise ValueError(f"unparseable timestamp: {raw}")
        hh = (hh % 12) + (12 if ampm == "pm" else 0)
    dt = datetime(year, month, day, hh, mi, ss, us)
    return dt.replace(tzinfo=tz) if tz else dt


def _looks_text_month(v):
    try:
        parse_text_month_timestamp(v)
        return True
    except ValueError:
        return False


def _iso_candidate_fix(c):
    """Python 3.9 fromisoformat gaps (B-14): 1-digit M/D, ',123' or non-3/6-digit
    fractions, '+0300' / '+03' offsets."""
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})(.*)$", c)
    if not m:
        return c
    rest = m.group(4)
    rest = re.sub(r"^([ T]\d{1,2}:\d{2}:\d{2})[.,](\d+)", lambda x: x.group(1) + "." + (x.group(2) + "000000")[:6], rest)
    rest = re.sub(r"^([ T])(\d):", lambda x: x.group(1) + "0" + x.group(2) + ":", rest)
    rest = re.sub(r"([+-])(\d{2})(\d{2})$", r"\1\2:\3", rest)
    rest = re.sub(r"(\d)\s*([+-])(\d{2})$", r"\1\2\3:00", rest)
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}{rest}"


def date_format_question(ts_values):
    """The needs_date_format answer: the DD/MM question only for real slash
    dates; otherwise 'unrecognised date shape' with samples (2026-11-10, L-5)."""
    vals = [v for v in ts_values if v and str(v).strip()]
    slash = [v for v in vals if SLASH_DATE_RE.match(_normalize_spaces(str(v).strip()))]
    if slash:
        return {"status": "needs_date_format", "message": "Timestamp column is ambiguous (DD/MM vs MM/DD). Ask the user, then re-run with --date-format DMY|MDY|ISO.", "sample_values": slash[:5]}
    return {"status": "needs_date_format", "message": f"unrecognised date shape: {str(vals[0])[:60] if vals else '(empty)'} — this is not a date layout the script knows. Check the timestamp column mapping, or re-export with an ISO (YYYY-MM-DD HH:MM:SS) date column.", "sample_values": vals[:5]}


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
        # A spelled-out month is never DD/MM-ambiguous (2026-11-10, T-1/L-4).
        text = 0 if (iso or ui_en) else sum(1 for s in sample[:500] if _looks_text_month(s))
        if not (iso or ui_en or text):
            return None
        if text:
            return "TEXT_MONTH"
        return "ISO" if iso >= ui_en else "UI_EN"
    for m in slash:
        a, b = int(m.group(1)), int(m.group(2))
        if a > 12:
            return "DMY"
        if b > 12:
            return "MDY"
    return None


DMY_OFFSET_RE = re.compile(
    r"^(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})\.?(?:,?\s*|T)(\d{1,2}:\d{2}(:\d{2})?)?(?:[.,]\d+)?\s*(Z|[+-]\d{2}:?\d{2})?\s*(?:([AaPp])\.?\s?[Mm]\.?)?$"
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
        if re.match(r"^\d{4}/\d{1,2}/\d{1,2}", iso_candidate):
            iso_candidate = iso_candidate.replace("/", "-", 2)
        iso_candidate = _iso_candidate_fix(iso_candidate)
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
    if fmt in ("UI_EN", "TEXT_MONTH"):
        try:
            return datetime.strptime(s, "%b %d, %Y, %I:%M:%S %p")
        except ValueError:
            try:
                return parse_text_month_timestamp(s)
            except ValueError:
                raise ValueError(f"unparseable {fmt} timestamp: {raw}")
    m = DMY_OFFSET_RE.match(s)
    if not m:
        return parse_text_month_timestamp(s)  # a spelled-out month in a DMY/MDY file
    a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    day, month = (a, b) if fmt == "DMY" else (b, a)
    hh = mm = ss = 0
    if m.group(4):
        parts = m.group(4).split(":")
        hh, mm = int(parts[0]), int(parts[1])
        ss = int(parts[2]) if len(parts) > 2 else 0
    if m.group(7):  # 12h clock (2026-11-10, L-4)
        if not 1 <= hh <= 12:
            raise ValueError(f"unparseable timestamp: {raw}")
        hh = (hh % 12) + (12 if m.group(7).lower() == "p" else 0)
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


_OP_REMOVE_RE = re.compile(r"\b(silindi|kaldırıldı|removed|deleted|eliminad[oa]s?|borrad[oa]s?|supprimée?s?|entfernt|gelöscht|rimoss[oaie]|eliminat[oaie]|removid[oa]s?|excluíd[oa]s?|удален\w*|удалён\w*)\b", re.IGNORECASE)
_OP_CREATE_RE = re.compile(r"\b(eklendi|oluşturuldu|uygulandı|added|created|applied|cread[oa]s?|añadid[oa]s?|agregad[oa]s?|créée?s?|ajoutée?s?|erstellt|hinzugefügt|creat[oaie]|aggiunt[oaie]|criad[oa]s?|adicionad[oa]s?|создан\w*|добавлен\w*)\b", re.IGNORECASE)
_STATUS_DETAIL_RE = re.compile(r"(?:olan Durum, (\w+) olarak|^\s*Status changed from \S+ to (\w+))", re.IGNORECASE | re.MULTILINE)


def split_statement(text):
    """(headline, detail_lines) of one statement: line 1 and the indented lines under it."""
    lines = str(text or "").split("\n")
    return lines[0].strip()[:SUMMARY_LINE_CAP], [l[:SUMMARY_LINE_CAP] for l in lines[1:] if l.strip()]


def split_statements(cell):
    """D-3 2026-11-10: a bundled free-text cell -> one text per statement
    (a non-indented line plus the indented detail lines under it)."""
    out = []
    for line in str(cell or "").replace("\r\n", "\n").split("\n"):
        if line.strip() and not line[0].isspace() or not out:
            out.append([line])
        else:
            out[-1].append(line)
    return ["\n".join(x).rstrip() for x in out if "".join(x).strip()] or [str(cell or "")]


def infer_operation(raw_summary):
    """A-7 2026-11-10: per statement, from its headline; a "changed" headline
    whose status detail ends in Removed is a REMOVE. Pause/enable stay UPDATE
    (the Status category says which)."""
    if not raw_summary:
        return "UPDATE"
    headline, details = split_statement(raw_summary)
    if _OP_REMOVE_RE.search(headline):
        return "REMOVE"
    if _OP_CREATE_RE.search(headline):
        return "CREATE"
    m = _STATUS_DETAIL_RE.search("\n".join(details))
    if m and (m.group(1) or m.group(2) or "").lower() in ("kaldırıldı", "removed"):
        return "REMOVE"
    return "UPDATE"


def _currency_of(text):
    for sym, code in CURRENCY_SYMBOLS:
        if sym in text:
            return code
    return None


def parse_statement_values(text, operation=None):
    """Old/new value strings parsed from one summary statement, plus its unit:
    (old, new, unit) or None. Each pattern runs per line (B-9 length cap)."""
    lines = [l[:SUMMARY_LINE_CAP] for l in str(text or "").split("\n")]
    for line in lines:
        m = SUMMARY_OLD_NEW_RE.search(line) or SUMMARY_TR_OLAN_DEGERINE_RE.search(line)
        if m:
            return m.group(1).strip(), m.group(2).strip(), _currency_of(m.group(0))
        m = SUMMARY_CHANGED_FROM_TO_RE.search(line)
        if m:
            return m.group(1).strip().rstrip("."), m.group(2).strip().rstrip("."), _currency_of(m.group(0))
    for line in lines:
        m = SUMMARY_TR_OLD_ONLY_RE.search(line)
        if m:
            return m.group(1).strip(), None, _currency_of(m.group(1))
        m = SUMMARY_PERCENT_SET_RE.search(line)
        if m:
            return None, (m.group(1) or m.group(2)), "%"
    if operation == "CREATE":
        for line in lines:
            m = SUMMARY_NEW_AMOUNT_RE.search(line)
            if m:
                return None, m.group(1).strip(), _currency_of(m.group(1))
    for line in lines:
        m = SUMMARY_TR_OLAN_TEXT_RE.search(line)
        if m:
            return m.group(1).strip('"'), m.group(2).strip('"'), None
    return None


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


def _norm_text(t):
    return " ".join(str(t or "").split())


def _sha(*parts):
    return hashlib.sha1("|".join(str(x or "") for x in parts).encode("utf-8")).hexdigest()[:20]


def assign_change_ids(rows, masked=False):
    """D-1 2026-11-10: change_id hashes ad group, operation and the full
    statement text too. Rows with no source_event_id are a multiset: the k-th
    identical row gets occurrence k in its hash, so nothing in one file is ever
    dropped and only a re-import of the same file collapses. A source_event_id
    is the source's own identity, so two rows sharing it with identical content
    stay real duplicates. Statements of one cell share event_id. With masking
    the user part is the mask label (S-2). Returns the possible-duplicate count."""
    occ, cell_occ, possible = Counter(), Counter(), 0
    i = 0
    while i < len(rows):
        n = max(1, int(rows[i].get("statement_count") or 1))
        cell = rows[i:i + n]
        first = cell[0]
        user_ref = first.get("user_name") if masked else (first.get("user_email") or first.get("user_name"))
        common = (first.get("account_id") or first.get("account_name"), first.get("timestamp_utc") or first.get("timestamp_iso"), user_ref,
                  first.get("campaign_id") or first.get("campaign_name"), first.get("ad_group_id") or first.get("ad_group_name"))
        cell_key = _sha("cell", *common, "\n".join(_norm_text(r.get("raw_summary")) for r in cell), first.get("source_event_id"))
        k = cell_occ[cell_key]
        cell_occ[cell_key] += 1
        if k and not first.get("source_event_id"):
            possible += 1
        event_id = first.get("source_event_id") or (cell_key if not k else _sha(cell_key, k))
        for r in cell:
            key = _sha(*common, r.get("resource_id") or r.get("resource_path") or r.get("resource_name"), r.get("field_name"),
                       r.get("old_value"), r.get("new_value"), r.get("operation"), _norm_text(r.get("raw_summary")), r.get("source_event_id"))
            j = occ[key]
            occ[key] += 1
            r["change_id"] = key if (not j or r.get("source_event_id")) else _sha(key, j)
            r["event_id"] = event_id
        i += n
    return possible


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


_STMT_KEY = "\x00statement"


def split_statement_rows(rows, mapping):
    """D-3 2026-11-10: one row per statement for summary-only rows (no field_name
    on the row); every statement row keeps the cell's other columns."""
    sh, fh = mapping.get("raw_summary"), mapping.get("field_name")
    if not sh:
        return rows
    out = []
    for r in rows:
        cell = r.get(sh)
        parts = split_statements(cell) if isinstance(cell, str) and "\n" in cell and not (fh and r.get(fh)) else None
        if not parts or len(parts) < 2:
            out.append(r)
            continue
        out.extend({**r, sh: part, _STMT_KEY: (i, len(parts))} for i, part in enumerate(parts))
    return out


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
    rows_after_field_split = len(rows)
    # D-4 guard 2026-11-10: a header mapped to raw_summary is never also the operation.
    if mapping.get("operation") and mapping.get("operation") == mapping.get("raw_summary"):
        mapping = {k: v for k, v in mapping.items() if k != "operation"}
    rows = split_statement_rows(rows, mapping)
    ts_header = mapping.get("timestamp")
    ts_values = [r.get(ts_header) for r in rows] if ts_header else []
    resolved_date_fmt = date_format or detect_date_format(ts_values)
    if resolved_date_fmt is None:
        # Fixed 2026-10-10 (6th audit round): sample_values was the first 5
        # timestamps, whatever they were. In a mostly-ISO file with a handful
        # of slash dates, the human was asked "DD/MM or MM/DD?" while being
        # shown five ISO strings that have no such ambiguity. Same for the
        # decimal-style question below.
        return date_format_question(ts_values)

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
    dst_gap = dst_fold = 0
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
                zi = ZoneInfo(tz)
                ts_utc = dt.replace(tzinfo=zi).astimezone(dt_timezone.utc).isoformat()
                # D-7 2026-11-10: count local times a DST change skips (gap) or repeats (fold).
                a, b = dt.replace(tzinfo=zi, fold=0), dt.replace(tzinfo=zi, fold=1)
                if a.utcoffset() != b.utcoffset():
                    if a.astimezone(dt_timezone.utc).astimezone(zi).replace(tzinfo=None) != dt:
                        dst_gap += 1
                    else:
                        dst_fold += 1
            except Exception:
                ts_utc = None

        raw_summary = g("raw_summary")
        stmt_index, stmt_count = r.get(_STMT_KEY, (0, 1))
        raw_operation = g("operation")
        operation = apply_value_alias("operation", raw_operation) if raw_operation else infer_operation(raw_summary)
        old_value, new_value = g("old_value"), g("new_value")
        # 2026-11-10: None when there are no values at all (was always "structured").
        value_confidence = "structured" if (old_value or new_value) else None
        parsed_currency = parsed_unit = None
        if (not old_value and not new_value) and raw_summary:
            parsed = parse_statement_values(raw_summary, operation)
            if parsed:
                old_value, new_value, unit = parsed
                value_confidence = "parsed_from_summary"
                if unit == "%":
                    parsed_unit = "%"
                elif unit:
                    parsed_currency = parsed_unit = unit

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
        campaign_confidence = "source" if campaign_name or campaign_id else None
        if not campaign_name and not campaign_id and not campaign_ref_mapped and raw_summary:
            # A-5 2026-11-10: "<Campaign>: ... Bütçe tutarı ..." names the campaign the cell leaves empty.
            for line in str(raw_summary).split("\n")[1:]:
                m_owner = SUMMARY_BUDGET_OWNER_RE.match(line[:SUMMARY_LINE_CAP])
                if m_owner:
                    campaign_name, campaign_confidence = m_owner.group(1).strip(), "inferred_from_summary"
                    break
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
        change_id = event_id = None  # set for all rows by assign_change_ids() below

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
            "value_unit": "units (converted from micros)" if is_micros_field else parsed_unit, "currency": parsed_currency,
            "value_confidence": value_confidence, "category": None, "subcategory": None,
            "category_confidence": None, "source": source_label, "raw_summary": raw_summary,
            "statement_index": stmt_index, "statement_count": stmt_count, "campaign_confidence": campaign_confidence,
        })

    possible_duplicates = assign_change_ids(out_rows)

    return {"status": "ok", "rows": out_rows, "rows_skipped_unparseable_date": unparseable, "orphan_names_dropped": orphan_names, "input_rows": source_row_count, "rows_added_by_field_split": rows_after_field_split - source_row_count,
            "rows_added_by_statement_split": len(rows) - rows_after_field_split, "possible_duplicate_rows": possible_duplicates,
            "dst_nonexistent_local_time": dst_gap, "dst_ambiguous_local_time": dst_fold, "date_format_used": resolved_date_fmt, "decimal_style_used": resolved_decimal_style}


def dedupe_rows(rows):
    """change_id is a content hash — identical (account, timestamp, user,
    resource, field, old, new) always produces the same id, on purpose, so a
    re-imported/overlapping file collapses instead of doubling. Until this
    function existed that collapse never actually happened: change_id was
    computed but nothing filtered on it. Fixed 2026-08-18. Returns
    (deduped_rows, duplicate_count). 2026-11-10 (D-1): ids now carry an
    occurrence index (assign_change_ids), so only same-source-event-id rows
    or a re-imported file collapse; distinct rows in one file never do."""
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
    """2026-11-10: per statement — headline rules see line 1 only, detail rules
    also need a matching indented line; case and repeated spaces ignored."""
    headline, details = split_statement(row.get("raw_summary") or "")
    headline = " ".join(headline.split())
    for rule in CATEGORY_RULES.get("summary_detail_rules", []):
        if re.search(rule["headline"], headline, re.IGNORECASE) and any(re.search(rule["detail"], d.strip(), re.IGNORECASE) for d in details):
            return rule["category"], rule["subcategory"]
    for rule in CATEGORY_RULES["summary_text_rules"]:
        if re.search(rule["pattern"], headline, re.IGNORECASE):
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


def semantic_data_quality(n, rows, other_pct):
    """Added 2026-11-10: data_quality fields from normalize/categorize — statement
    split, kept possible duplicates, DST, and warnings (L-6, B-13)."""
    warnings = []
    summary_rows = [r for r in rows if not r.get("field_name") and r.get("raw_summary")]
    if rows and other_pct >= 30 and summary_rows:
        known = sum(1 for r in summary_rows if KNOWN_SUMMARY_VOCAB_RE.search(split_statement(r["raw_summary"])[0]))
        if known < len(summary_rows) / 2:
            warnings.append(f"{other_pct}% of rows are Other and most summaries use words these rules don't know: the export is probably in a UI language without category rules (only English and Turkish have them). Re-export in English or Turkish for categories.")
        else:
            warnings.append(f"{other_pct}% of rows are Other: check for summary phrasings the rules don't cover.")
    if rows and not any(r.get("timestamp_utc") for r in rows):
        warnings.append("No --timezone given and the timestamps carry no offset: the timeline will be empty. Re-run with --timezone <IANA name>, e.g. Europe/Istanbul.")
    if n.get("dst_nonexistent_local_time") or n.get("dst_ambiguous_local_time"):
        warnings.append(f"{n.get('dst_nonexistent_local_time', 0)} row(s) fall in a daylight-saving gap and {n.get('dst_ambiguous_local_time', 0)} in a repeated hour; their UTC time may be off by the DST shift.")
    return {
        "rows_added_by_statement_split": n.get("rows_added_by_statement_split", 0),
        "possible_duplicate_rows_kept": n.get("possible_duplicate_rows", 0),
        "dst_nonexistent_local_time": n.get("dst_nonexistent_local_time", 0),
        "dst_ambiguous_local_time": n.get("dst_ambiguous_local_time", 0),
        "campaign_inferred_from_summary": sum(1 for r in rows if r.get("campaign_confidence") == "inferred_from_summary"),
        "warnings": warnings,
    }


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
    known = dict(mask_map)
    for r in rows:
        ident = r.get("user_email") or r.get("user_name")
        if r["actor_type"] == "human" and ident in mask_map:
            for alias in (r.get("user_name"), r.get("user_email")):
                if alias:
                    known.setdefault(alias, mask_map[ident])
    _scrub_identities(out, known)
    assign_change_ids(out, masked=True)
    return out


_SCRUB_SKIP_FIELDS = {"change_id", "event_id", "user_name", "user_email", "timestamp", "timestamp_iso", "timestamp_utc", "timezone",
                      "operation", "operation_confidence", "category", "subcategory", "category_confidence", "value_confidence", "source"}


def _scrub_identities(rows, mask_map):
    """S-1/T-3 2026-11-10: with --mask-users, known identities and any
    email-shaped string are replaced in every other string field too."""
    names = sorted((k for k in mask_map if len(k) >= 3), key=len, reverse=True)
    name_re = re.compile(r"(?<!\w)(" + "|".join(re.escape(k) for k in names) + r")(?!\w)") if names else None
    def scrub(v):
        if name_re:
            v = name_re.sub(lambda m: mask_map[m.group(1)], v)
        return EMAIL_RE.sub(lambda m: mask_map.get(m.group(0), "[email removed]"), v)
    for r in rows:
        for k, v in r.items():
            if k not in _SCRUB_SKIP_FIELDS and isinstance(v, str) and v:
                r[k] = scrub(v)


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

    # D-5 2026-11-10: bucket by local day when every placed row shares one known zone.
    zones = {r.get("timezone") for r in rows if r.get("timestamp_utc")}
    timeline_tz = next(iter(zones)) if len(zones) == 1 and next(iter(zones)) not in (None, "unknown") else "UTC"
    timeline_counter, excluded_from_timeline = Counter(), 0
    for r in rows:
        tsu = r.get("timestamp_utc")
        if tsu:
            timeline_counter[(r.get("timestamp_iso") or tsu)[:10] if timeline_tz != "UTC" else tsu[:10]] += 1
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
                "last_change_ts_utc": r.get("timestamp_utc"),
                "_sort_ts": ts,
            }
    window_end_date = datetime.fromisoformat(window_end[:19]).date() if window_end else date.today()
    untouched = []
    for entry in campaign_last.values():
        # D-6 2026-11-10: both ends on the chrono_key basis (UTC when known), never negative.
        last_date = datetime.fromisoformat(entry["_sort_ts"][:19]).date()
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
            "masked_users": masked_users, "timeline_reference_tz": timeline_tz,
            "timeline_basis": "local calendar day in " + timeline_tz if timeline_tz != "UTC" else "UTC calendar day",
            "days_since_basis": "timestamp_utc" if all(r.get("timestamp_utc") for r in rows) else "timestamp_utc where known, else local time",
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
  /* 2026-11-10: dashboard-only tokens, so dark mode can re-point them (pills, active buttons) */
  :root { --accent: var(--lab-brand); --text-dim: var(--lab-text-subtle);
    --dash-on-bg: var(--lab-ink-950); --dash-on-fg: #fff; --dash-cat-keep: 100%;
    --dash-auto-fg: var(--lab-amber-800); --dash-auto-bg: var(--lab-amber-50); --dash-auto-line: var(--lab-amber-200);
    --dash-rule-fg: var(--lab-violet-800); --dash-rule-bg: var(--lab-violet-50); --dash-rule-line: var(--lab-violet-200); }
  @media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
    --dash-on-bg: #f5ede9; --dash-on-fg: #1f1c1b; --dash-cat-keep: 38%;
    --dash-auto-fg: #fee685; --dash-auto-bg: color-mix(in oklab, #f99c00 18%, #1f1c1b); --dash-auto-line: color-mix(in oklab, #f99c00 40%, #1f1c1b);
    --dash-rule-fg: #ddd6ff; --dash-rule-bg: color-mix(in oklab, #8d54ff 22%, #1f1c1b); --dash-rule-line: color-mix(in oklab, #8d54ff 45%, #1f1c1b); } }
  :root[data-theme="dark"] {
    --dash-on-bg: #f5ede9; --dash-on-fg: #1f1c1b; --dash-cat-keep: 38%;
    --dash-auto-fg: #fee685; --dash-auto-bg: color-mix(in oklab, #f99c00 18%, #1f1c1b); --dash-auto-line: color-mix(in oklab, #f99c00 40%, #1f1c1b);
    --dash-rule-fg: #ddd6ff; --dash-rule-bg: color-mix(in oklab, #8d54ff 22%, #1f1c1b); --dash-rule-line: color-mix(in oklab, #8d54ff 45%, #1f1c1b); }
  header { max-width: 1440px; margin: 0 auto; padding: 36px 28px 8px; }
  .lab header h1 { margin: 0 0 6px; font-size: clamp(1.75rem, 1.1rem + 1.7vw, 2.5rem); line-height: 1.1; letter-spacing: -0.025em; font-weight: 600; color: var(--lab-text-strong); }
  header .meta { color: var(--lab-text-subtle); font-size: 13.5px; overflow-wrap: anywhere; }
  header details.note { margin-top: 10px !important; }
  header details summary { color: var(--lab-brand-ink); font-weight: 500; }
  main { padding: 12px 28px 56px; display: flex; flex-direction: column; gap: 16px; max-width: 1440px; margin: 0 auto; }
  section { background: var(--lab-surface); border-radius: var(--lab-radius-tile); box-shadow: 0 0 0 1px var(--lab-ring); padding: 22px 24px; min-width: 0; }
  .lab section h2 { margin: 0 0 12px; display: flex; flex-wrap: wrap; align-items: baseline; gap: 8px; font-size: 1.0625rem; line-height: 1.35; letter-spacing: -0.01em; font-weight: 600; color: var(--lab-text-strong); }
  section h2 .note { margin: 0; font-size: 13px; font-weight: 400; color: var(--lab-text-subtle); }
  .section-caption { margin: -4px 0 18px; font-size: 13.5px; line-height: 1.5; color: var(--lab-text-subtle); max-width: 80ch; }
  .filters { display: flex; flex-wrap: wrap; gap: 10px 8px; align-items: flex-end; }
  .fld { display: flex; flex-direction: column; gap: 4px; min-width: 0; position: relative; }
  .fld-grow { flex: 1 1 200px; }
  .fld-l { font-size: 12px; font-weight: 500; color: var(--lab-text-muted); }
  .filters select, .filters input[type=search], .explorer-search, input.ctl, .ms > summary { height: 36px; padding: 0 12px; border: 0; border-radius: 10px; background: var(--lab-surface); color: var(--lab-text); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 400 14px/1 var(--lab-font); }
  .filters select { min-width: 130px; }
  .filters select:focus, .filters input:focus, .explorer-search:focus, input.ctl:focus { outline: none; box-shadow: inset 0 0 0 2px var(--lab-brand); }
  .filters select option { color: #1f1c1b; background: #fff; }
  /* multi-select as a checkbox popover (2026-11-10, replaces <select multiple size=1>) */
  .ms { position: relative; min-width: 0; }
  .ms-opt > span { min-width: 0; }
  .ms > summary { display: flex; align-items: center; gap: 6px; min-width: 150px; max-width: 240px; cursor: pointer; list-style: none; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .ms > summary::-webkit-details-marker { display: none; }
  .ms > summary::after { content: "▾"; margin-left: auto; color: var(--lab-text-subtle); }
  .ms > summary.has-sel { box-shadow: inset 0 0 0 2px var(--lab-brand); }
  .ms-panel { position: absolute; z-index: 40; top: calc(100% + 4px); left: 0; width: max-content; min-width: 220px; max-width: min(360px, calc(100vw - 32px)); max-height: 320px; overflow: auto; padding: 8px; border-radius: 12px; background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring-strong), var(--lab-shadow-float); }
  .ms-opt { display: flex; align-items: center; gap: 8px; min-height: 32px; padding: 2px 6px; border-radius: 8px; font-size: 13.5px; cursor: pointer; overflow-wrap: anywhere; }
  .ms-opt:hover { background: var(--lab-surface-soft); }
  .ms-clear { margin-bottom: 4px; height: 30px; padding: 0 10px; border: 0; border-radius: 9999px; background: var(--lab-surface-soft); color: var(--lab-text-strong); font: 500 12.5px/1 var(--lab-font); cursor: pointer; }
  .rangebtns, .threshbtns { display: inline-flex; flex-wrap: wrap; gap: 4px; padding: 4px; border-radius: 9999px; background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring); }
  .rangebtns button, .threshbtns button { height: 30px; padding: 0 14px; border: 0; border-radius: 9999px; background: transparent; color: var(--lab-text-muted); font: 500 13px/1 var(--lab-font); cursor: pointer; }
  .rangebtns button:hover, .threshbtns button:hover { background: var(--lab-surface-soft); color: var(--lab-text-strong); }
  .rangebtns button.active, .threshbtns button.active { background: var(--dash-on-bg); color: var(--dash-on-fg); }
  .clearbtn { height: 36px; padding: 0 16px; border: 0; border-radius: 9999px; background: var(--lab-surface); color: var(--lab-text-strong); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 500 13.5px/1 var(--lab-font); cursor: pointer; }
  .clearbtn:hover { background: var(--lab-brand); color: #fff; box-shadow: inset 0 0 0 1px var(--lab-brand); }
  #filterContext { margin-top: 12px; min-height: 1em; font-size: 13px; color: var(--lab-brand-ink); overflow-wrap: anywhere; }
  .summary-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
  @media (max-width: 900px) { .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  .stat { display: flex; flex-direction: column; gap: 6px; padding: 16px 18px; border-radius: var(--lab-radius-card); background: var(--lab-surface-soft); min-width: 0; }
  .stat .n { font-size: 1.5rem; line-height: 1.15; letter-spacing: -0.02em; font-weight: 600; color: var(--lab-text-strong); overflow-wrap: anywhere; font-variant-numeric: tabular-nums; }
  .stat .l { font-size: 13px; font-weight: 500; color: var(--lab-text-subtle); }
  .bar-row { display: grid; grid-template-columns: 200px minmax(0, 1fr) 60px; gap: 12px; align-items: center; width: 100%; padding: 6px 4px; border: 0; border-radius: 8px; background: transparent; color: inherit; font: inherit; text-align: left; cursor: pointer; }
  .bar-row:hover .bar-label, .bar-row[aria-pressed="true"] .bar-label { color: var(--lab-brand-ink); }
  .bar-row[aria-pressed="true"] { background: var(--lab-brand-soft); }
  .bar-label { display: block; min-width: 0; font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--lab-text); }
  .bar-track { display: block; height: 8px; border-radius: 9999px; background: var(--lab-line); overflow: hidden; }
  .bar-fill { display: block; height: 100%; border-radius: 9999px; background: var(--lab-brand); }
  .bar-n { display: block; text-align: right; font-size: 13px; font-weight: 500; color: var(--lab-text-subtle); font-variant-numeric: tabular-nums; }
  .grp-label { margin: 0 0 6px; font-size: 13px; font-weight: 600; color: var(--lab-text-strong); }
  .grp-label ~ .grp-label { margin-top: 16px; }
  .timeline { display: flex; align-items: flex-end; gap: 3px; height: 128px; overflow-x: auto; padding: 30px 0 4px; }
  .tl-bar { position: relative; min-width: 8px; flex: 1 0 8px; border-radius: 6px 6px 2px 2px; background: var(--lab-brand); }
  .tl-bar.edge { background: var(--lab-primary-300); }
  .tl-bar:hover, .tl-bar:focus { background: var(--lab-primary-700); }
  .tl-bar .tip { display: none; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%); margin-bottom: 6px; padding: 4px 8px; border-radius: 8px; background: var(--lab-ink-950); color: #fff; font-size: 12px; white-space: nowrap; z-index: 5; }
  .tl-bar:hover .tip, .tl-bar:focus .tip { display: block; }
  #timelineReadout { min-height: 1.5em; margin-top: 4px; font-size: 13px; color: var(--lab-text-muted); }
  .acc-block { padding: 8px 0; border-bottom: 1px solid var(--lab-line); }
  .acc-block:last-child { border-bottom: 0; }
  .acc-head { display: grid; grid-template-columns: 16px minmax(0, 180px) minmax(0, 1fr) 50px; align-items: center; gap: 10px; width: 100%; padding: 4px; border: 0; border-radius: 8px; background: transparent; color: inherit; font: inherit; font-weight: 600; text-align: left; cursor: pointer; }
  .acc-head .chev { font-size: 11px; color: var(--lab-text-subtle); transition: transform var(--lab-fast) var(--lab-ease); }
  .acc-head[aria-expanded="true"] .chev { transform: rotate(90deg); }
  .camp-list { margin-top: 6px; padding-left: 22px; }
  .camp-list[hidden] { display: none; }
  .tbl-wrap { overflow-x: auto; max-width: 100%; }
  table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
  th, td { max-width: 220px; padding: 10px 12px; border-bottom: 1px solid var(--lab-line); text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  th { position: sticky; top: 0; background: var(--lab-surface); color: var(--lab-text-subtle); font-size: 12.5px; font-weight: 500; user-select: none; }
  .th-btn { padding: 0; border: 0; background: none; color: inherit; font: inherit; cursor: pointer; }
  th[aria-sort="ascending"], th[aria-sort="descending"] { color: var(--lab-text-strong); }
  th[aria-sort="descending"] .th-btn::after { content: " ▾"; color: var(--lab-brand); }
  th[aria-sort="ascending"] .th-btn::after { content: " ▴"; color: var(--lab-brand); }
  tr.datarow { cursor: pointer; }
  tr.datarow:hover, tr.datarow:focus { background: var(--lab-surface-soft); }
  tr.datarow:focus { outline: 2px solid var(--lab-brand); outline-offset: -2px; }
  .pill { display: inline-block; padding: 1px 9px; border-radius: 9999px; border: 1px solid transparent; font-size: 12px; font-weight: 600; line-height: 1.6; background: var(--lab-surface-soft); color: var(--lab-text-muted); }
  .pill.cat { color: color-mix(in oklab, var(--pc) var(--dash-cat-keep), #fff); border-color: color-mix(in srgb, var(--pc) 25%, transparent); background: color-mix(in srgb, var(--pc) 8%, transparent); }
  .pill-auto { color: var(--dash-auto-fg); background: var(--dash-auto-bg); border-color: var(--dash-auto-line); }
  .pill-rule { color: var(--dash-rule-fg); background: var(--dash-rule-bg); border-color: var(--dash-rule-line); }
  .parsed { margin-left: 4px; padding: 0 5px; border-radius: 6px; font-size: 11px; font-weight: 500; color: var(--lab-text-muted); box-shadow: inset 0 0 0 1px var(--lab-line-strong); cursor: help; }
  .rm-detail { display: block; margin-top: 2px; font-size: 12.5px; color: var(--lab-text-muted); }
  .pager { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-top: 12px; font-size: 13px; color: var(--lab-text-subtle); }
  .pager button { height: 32px; padding: 0 14px; border: 0; border-radius: 9999px; background: var(--lab-surface); color: var(--lab-text-strong); box-shadow: inset 0 0 0 1px var(--lab-line-strong); font: 500 13px/1 var(--lab-font); cursor: pointer; }
  .pager button:disabled { opacity: 0.4; cursor: default; }
  .explorer-search { width: 100%; }
  .explorer-fld { margin-bottom: 12px; }
  .cfg-label { margin-bottom: 6px; font-size: 13px; font-weight: 600; color: var(--lab-text-strong); }
  .cfg-field { display: flex; flex-direction: column; gap: 6px; font-size: 13px; color: var(--lab-text-muted); }
  .cfg-err { min-height: 1.2em; font-size: 12.5px; color: var(--lab-warn); }
  .cfg-check { display: flex; align-items: center; gap: 7px; cursor: pointer; font-size: 13.5px; }
  input.ctl { width: 80px; height: 32px; }
  input.ctl[aria-invalid="true"] { box-shadow: inset 0 0 0 2px var(--lab-warn); }
  .detail-overlay { display: none; position: fixed; inset: 0; z-index: 50; align-items: center; justify-content: center; padding: 16px; background: rgb(22 19 18 / 0.45); backdrop-filter: blur(2px); }
  .detail-overlay.open { display: flex; }
  .detail-card { width: min(600px, 100%); max-height: 85vh; overflow: auto; padding: 24px; border-radius: var(--lab-radius-tile); background: var(--lab-surface); box-shadow: 0 0 0 1px var(--lab-ring), var(--lab-shadow-lift); }
  .detail-card h3 { margin: 0 0 16px; font-size: 1.125rem; font-weight: 600; color: var(--lab-text-strong); }
  .detail-card dl { display: grid; grid-template-columns: 130px minmax(0, 1fr); gap: 8px 12px; margin: 0; font-size: 14px; }
  .detail-card dt { color: var(--lab-text-subtle); font-size: 13px; }
  .detail-card dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
  .detail-card dd.pre { white-space: pre-wrap; font-family: var(--lab-font-code); font-size: 12.5px; }
  .detail-card .close { float: right; width: 36px; height: 36px; border: 0; border-radius: 9999px; background: transparent; color: var(--lab-text-muted); font-size: 16px; cursor: pointer; }
  .detail-card .close:hover { background: var(--lab-surface-soft); }
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
    .bar-row { grid-template-columns: 110px minmax(0, 1fr) 44px; gap: 10px; }
    .acc-head { grid-template-columns: 16px minmax(0, 110px) minmax(0, 1fr) 44px; }
    .fld { flex: 1 1 140px; }
    .fld-range { flex-basis: 100%; }
    .rangebtns, .threshbtns { border-radius: 22px; }
    .ms > summary { max-width: none; min-width: 0; }
    .ms-panel { position: static; width: 100%; min-width: 0; max-width: none; margin-top: 4px; }
    .lab details:not(.ms) > summary { padding: 12px 0; }
    .filters select { min-width: 0; width: 100%; }
    th, td { max-width: 150px; }
    .detail-card { padding: 18px; }
    .detail-card dl { grid-template-columns: minmax(0, 1fr); gap: 2px; }
    .detail-card dd { margin-bottom: 8px; }
  }
  /* 44px tap targets on phones and touch screens (2026-11-10, C-4/F-11) */
  @media (max-width: 640px), (pointer: coarse) {
    .filters select, .filters input[type=search], .explorer-search, .ms > summary, .clearbtn, .rangebtns button, .threshbtns button, .pager button, .detail-card .close, .ms-clear { min-height: 44px; }
    .ms-opt, .bar-row, .acc-head { min-height: 44px; }
    .th-btn { min-height: 44px; }
    tr.datarow td { padding-top: 13px; padding-bottom: 13px; }
  }
  /* Print / "Save as PDF": the table gets the full landscape width and its
     cells wrap. 2026-11-10 (C-9): no break-inside:avoid on whole sections
     (pushed a tall section to page 2, leaving page 1 mostly blank); headers never break mid-word. */
  @media print {
    @page { size: A4 landscape; margin: 10mm; }
    header { padding: 0 0 6px; }
    main { display: block; padding: 0; }
    section { margin: 0 0 10px; padding: 12px 14px; border: 1px solid #dfe3ec; box-shadow: none; break-inside: auto; }
    .lab section h2 { break-after: avoid; }
    .summary-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; break-inside: avoid; }
    .stat, .bar-row, .acc-block, .tl-bar { break-inside: avoid; }
    .stat { padding: 10px 12px; }
    .stat .n { font-size: 1.1rem; overflow-wrap: anywhere; }
    .filters, .explorer-fld, .pager button, #detailOverlay, .threshbtns, #ruleMatchesConfig { display: none !important; }
    #filterContext { margin: 0; }
    .timeline { overflow: visible; }
    .tl-bar .tip, #timelineReadout { display: none !important; }
    .tbl-wrap { overflow: visible !important; }
    table { font-size: 9.5px; table-layout: auto; }
    th, td { max-width: none; padding: 4px 6px; white-space: normal; overflow: visible; text-overflow: clip; overflow-wrap: anywhere; vertical-align: top; }
    th { position: static; font-size: 9px; white-space: nowrap; overflow-wrap: normal; word-break: keep-all; hyphens: none; }
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
       Strictly factual, no dollar/quality claims: this tool doesn't judge,
       so the disclosure doesn't either. -->
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
  <section id="filtersSection" aria-labelledby="hFilters">
    <h2 id="hFilters">Filters</h2>
    <!-- 2026-11-10 (C-2/F-12): every control has a visible label; multi-selects are checkbox popovers -->
    <div class="filters">
      <div class="fld"><span class="fld-l" id="lAccount">Account</span><details class="ms" id="fAccount" data-state="accounts" data-all="All accounts"><summary aria-labelledby="lAccount fAccountSum"><span id="fAccountSum">All accounts</span></summary><div class="ms-panel" role="group" aria-labelledby="lAccount"></div></details></div>
      <div class="fld"><span class="fld-l" id="lCampaign">Campaign</span><details class="ms" id="fCampaign" data-state="campaigns" data-all="All campaigns"><summary aria-labelledby="lCampaign fCampaignSum"><span id="fCampaignSum">All campaigns</span></summary><div class="ms-panel" role="group" aria-labelledby="lCampaign"></div></details></div>
      <div class="fld"><span class="fld-l" id="lUser">User</span><details class="ms" id="fUser" data-state="users" data-all="All users"><summary aria-labelledby="lUser fUserSum"><span id="fUserSum">All users</span></summary><div class="ms-panel" role="group" aria-labelledby="lUser"></div></details></div>
      <label class="fld"><span class="fld-l">Actor type</span>
        <select id="fActor" aria-label="Actor type">
          <option value="">All actors</option>
          <option value="human">Human</option>
          <option value="automation">Automation</option>
          <option value="unknown">Unknown</option>
        </select></label>
      <div class="fld"><span class="fld-l" id="lCategory">Category</span><details class="ms" id="fCategory" data-state="categories" data-all="All categories"><summary aria-labelledby="lCategory fCategorySum"><span id="fCategorySum">All categories</span></summary><div class="ms-panel" role="group" aria-labelledby="lCategory"></div></details></div>
      <div class="fld"><span class="fld-l" id="lOperation">Operation</span><details class="ms" id="fOperation" data-state="operations" data-all="All types"><summary aria-labelledby="lOperation fOperationSum"><span id="fOperationSum">All types</span></summary><div class="ms-panel" role="group" aria-labelledby="lOperation"></div></details></div>
      <div class="fld"><span class="fld-l" id="lSource">Source</span><details class="ms" id="fSource" data-state="sources" data-all="All sources"><summary aria-labelledby="lSource fSourceSum"><span id="fSourceSum">All sources</span></summary><div class="ms-panel" role="group" aria-labelledby="lSource"></div></details></div>
      <label class="fld"><span class="fld-l">Rule match</span>
        <select id="fRuleMatch" aria-label="Rule match filter">
          <option value="">All changes</option>
          <option value="__matched__">Matched a rule</option>
        </select></label>
      <div class="fld fld-range"><span class="fld-l" id="lRange">Date range</span>
        <div class="rangebtns" id="rangeBtns" role="group" aria-labelledby="lRange">
          <button type="button" data-range="all" class="active" aria-pressed="true">All</button>
          <button type="button" data-range="30" aria-pressed="false">30d</button>
          <button type="button" data-range="7" aria-pressed="false">7d</button>
          <button type="button" data-range="1" aria-pressed="false">Last 24h</button>
        </div></div>
      <label class="fld"><span class="fld-l">Range ends at</span>
        <select id="fAnchor" aria-label="Date range ends at">
          <option value="data">Newest change in the data</option>
          <option value="today">Today</option>
        </select></label>
      <label class="fld fld-grow"><span class="fld-l">Search</span><input type="search" id="fSearch" aria-label="Search changes" placeholder="Campaign, user, summary text…"></label>
      <div class="fld"><span class="fld-l" aria-hidden="true">&nbsp;</span><button type="button" class="clearbtn" id="clearFilters">Clear filters</button></div>
    </div>
    <!-- A stated filter-context line, so a screenshot mid-filter is self-explanatory. -->
    <div id="filterContext" aria-live="polite"></div>
  </section>
  <section id="summarySection">
    <h2>Summary</h2>
    <div class="section-caption">Totals reflect the filters and date range currently applied above.</div>
    <div class="summary-grid" id="summaryGrid"></div>
  </section>
  <section>
    <h2>Activity Timeline <span class="note" id="timelineBasis">(day buckets)</span></h2>
    <div class="section-caption">Bar height is relative to the busiest day shown. Hover, tap or tab to a bar for its exact date and count.</div>
    <div class="timeline" id="timeline" role="list" aria-label="Changes per day"></div>
    <div id="timelineReadout" aria-live="polite"></div>
  </section>
  <section>
    <h2>User Activity</h2>
    <div class="section-caption">Split by actor type — human (named user), automation (script/rule/recommendation), or unknown (no identity in the source data). Select a bar to filter by it.</div>
    <div id="userActivity"></div>
  </section>
  <section>
    <h2>Account / Campaign Activity</h2>
    <div class="section-caption">Each account expands to the campaigns it contains, ranked by number of changes. Select a campaign to filter by it.</div>
    <div id="accountActivity"></div>
  </section>
  <section>
    <h2>Category Distribution</h2>
    <div class="section-caption">Categories come from rule matches against resource type, field, operation and summary text. "Other" means no rule matched yet — see coverage.txt.</div>
    <div id="categoryDist"></div>
  </section>
  <section>
    <h2>Rule Matches</h2>
    <div class="section-caption">Deterministic, user-adjustable rules — a match means the change crossed a threshold you set, not that it was good or bad. Off by default; every match always shows the exact number and rule next to it.</div>
    <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:10px;">
      <label style="display:flex;align-items:center;gap:7px;cursor:pointer;font-size:13px;min-height:36px;">
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
    <div class="threshbtns" id="threshBtns" role="group" aria-label="Minimum days since last change" style="margin-bottom:10px;">
      <button type="button" data-min="0" class="active" aria-pressed="true">All</button>
      <button type="button" data-min="7" aria-pressed="false">7+ days</button>
      <button type="button" data-min="14" aria-pressed="false">14+ days</button>
      <button type="button" data-min="30" aria-pressed="false">30+ days</button>
      <button type="button" data-min="60" aria-pressed="false">60+ days</button>
    </div>
    <div id="untouched"></div>
  </section>
  <section>
    <h2>Change Explorer</h2>
    <div class="section-caption" id="explorerCaption">Select any row (click, or Tab then Enter) for full before/after detail. Select a column header to sort by it.</div>
    <label class="fld explorer-fld"><span class="fld-l">Search within Explorer</span><input type="search" class="explorer-search" id="explorerSearch" aria-label="Search within Explorer" placeholder="Same search as the filter above…"></label>
    <div class="tbl-wrap" tabindex="0" role="region" aria-label="Change Explorer table">
    <table id="explorerTable">
      <thead><tr>
        <th data-key="timestamp_iso" aria-sort="descending"><button type="button" class="th-btn">Date</button></th><th data-key="user_name"><button type="button" class="th-btn">User</button></th><th data-key="account_name"><button type="button" class="th-btn">Account</button></th>
        <th data-key="campaign_name"><button type="button" class="th-btn">Campaign</button></th><th data-key="ad_group_name"><button type="button" class="th-btn">Ad Group</button></th><th data-key="category"><button type="button" class="th-btn">Category</button></th>
        <th data-key="field_name"><button type="button" class="th-btn">Change</button></th><th data-key="raw_summary"><button type="button" class="th-btn">Summary</button></th><th data-key="old_value"><button type="button" class="th-btn">Old Value</button></th><th data-key="new_value"><button type="button" class="th-btn">New Value</button></th><th>Matches</th><th data-key="source"><button type="button" class="th-btn">Source</button></th>
      </tr></thead>
      <tbody id="explorerBody"></tbody>
    </table>
    </div>
    <div class="pager">
      <button type="button" id="pagerPrev">Prev</button><span id="pagerInfo" aria-live="polite"></span><button type="button" id="pagerNext">Next</button>
    </div>
  </section>
</main>
<div class="detail-overlay" id="detailOverlay">
  <div class="detail-card" id="detailCard" role="dialog" aria-modal="true" aria-labelledby="detailTitle" tabindex="-1">
    <button type="button" class="close" id="detailClose" aria-label="Close detail">✕</button>
    <h3 id="detailTitle">Change Detail</h3>
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
// Every place that interpolates source-derived text into innerHTML goes
// through this (campaign/account/user names come from the file, not a closed set).
function escapeHtml(s){
  if (s === null || s === undefined) return '';
  return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
// Identity = *_id when present, else the name (mirrors the Python identity() helpers).
function accountIdentity(c){ return c.account_id || c.account_name; }
function campaignIdentity(c){ return c.campaign_id || c.campaign_name; }
function adGroupIdentity(c){ return c.ad_group_id || c.ad_group_name; }
function userIdentity(c){ return c.user_email || c.user_name; }
// 2026-11-10: rows with no account/campaign identity get this key, so a drill-down on them filters exactly.
const NONE = '__none__';
// One key per actor, human / automation / unknown alike (F-8: Unknown is a real group).
function actorKey(c){
  if(c.actor_type==='human'){ const u = userIdentity(c); return u ? 'h:'+u : 'u:'; }
  if(c.actor_type==='automation') return 'a:'+(c.client_type||c.user_name||'Automation');
  return 'u:';
}
function actorLabel(c){
  const k = actorKey(c);
  if(k.startsWith('h:')) return c.user_name||c.user_email;
  if(k.startsWith('a:')) return k.slice(2)+' (automation)';
  return 'Unknown (no identity in source)';
}
// Turkish-aware, accent-insensitive fold (F-5/B-2): İ/I/ı -> i, ş -> s, ğ -> g, NFD marks stripped. Used on both sides.
function fold(s){
  if(s===null || s===undefined) return '';
  return String(s).normalize('NFD').replace(/[̀-ͯ]/g,'').toLowerCase().replace(/ı/g,'i');
}
function rowHaystack(c){
  if(c._hay===undefined){
    c._hay = fold([c.campaign_name,c.account_name,c.user_name,c.user_email,c.category,c.subcategory,
      c.field_name,c.old_value,c.new_value,c.ad_group_name,c.operation,c.source,c.resource_type,
      c.client_type,c.change_id,c.raw_summary,c.value_unit,c.currency].filter(v=>v!==null && v!==undefined).join(' '));
  }
  return c._hay;
}
function firstLine(s){
  if(!s) return '';
  const line = String(s).split(/\r?\n/).map(x=>x.trim()).find(Boolean);
  return line || '';
}
// ---- time ------------------------------------------------------------
// 2026-11-10 (F-7): ranges compare on timestamp_utc, never on the viewer's
// clock zone. A naive timestamp (no timezone known) is read as UTC so the
// result is the same on every machine.
function rowTime(c){
  if(c._t!==undefined) return c._t;
  let t = NaN;
  if(c.timestamp_utc) t = Date.parse(c.timestamp_utc);
  if(isNaN(t) && c.timestamp_iso){
    const s = String(c.timestamp_iso);
    t = Date.parse(/(?:[zZ]|[+-]\d\d:?\d\d)$/.test(s) ? s : s+'Z');
  }
  c._t = isNaN(t) ? null : t;
  return c._t;
}
let _dataEnd = {rows:null, v:null};
function dataEndTime(){
  const rows = (DASH_DATA && DASH_DATA.changes) || [];
  if(_dataEnd.rows!==rows){
    let m = null;
    for(const c of rows){ const t = rowTime(c); if(t!=null && (m==null || t>m)) m = t; }
    _dataEnd = {rows, v:m};
  }
  return _dataEnd.v;
}
function rangeAnchor(){ return state.anchor==='today' ? Date.now() : dataEndTime(); }
function withinRange(c){
  if(state.range==='all') return true;
  const t = rowTime(c);
  if(t==null) return false;
  const a = rangeAnchor();
  if(a==null) return true;
  return t >= a - parseFloat(state.range)*86400000 && t <= a;
}
// Dates show the source's own wall-clock time (account-local when a timezone
// is known), never re-zoned to the viewer. dd.mm.yyyy hh:mm ('tr-TR') for
// Turkish data or a Turkish browser; otherwise the browser's locale (C-8).
let UI_LOCALE;
function detectLocale(){
  try { if(typeof navigator!=='undefined' && /^tr\b/i.test(navigator.language||'')) return 'tr-TR'; } catch(e){}
  const rows = ((DASH_DATA && DASH_DATA.changes) || []).slice(0, 500);
  const tr = rows.filter(c=>/[ğışĞŞİ]|değiştirildi|eklendi|kaldırıldı/.test((c.raw_summary||'')+(c.operation||''))).length;
  return rows.length && tr/rows.length > 0.2 ? 'tr-TR' : undefined;
}
function fmtWall(iso, withTime){
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/.exec(iso||'');
  if(!m) return iso ? String(iso) : '—';
  const d = new Date(Date.UTC(+m[1], +m[2]-1, +m[3], +(m[4]||0), +(m[5]||0)));
  const opt = {timeZone:'UTC', day:'2-digit', month:'2-digit', year:'numeric'};
  if(withTime && m[4]!==undefined){ opt.hour='2-digit'; opt.minute='2-digit'; if(UI_LOCALE==='tr-TR') opt.hour12=false; }
  try { return new Intl.DateTimeFormat(UI_LOCALE, opt).format(d); } catch(e){ return String(iso); }
}
function fmtTs(c){ return c.timestamp_iso ? fmtWall(c.timestamp_iso, true) : (c.timestamp || '—'); }
function fmtEpochDate(t){ return t==null ? '—' : fmtWall(new Date(t).toISOString(), false); }
// ---- values ----------------------------------------------------------
// Neutral display, no good/bad colouring. Units: micros-converted amounts
// show "display CUR (raw: …)"; a value_unit like "%" or "₺" is appended.
function valueParts(c, which){
  const raw = c[which+'_value'], disp = c[which+'_value_display'], unit = c.value_unit, cur = c.currency;
  if(raw===null || raw===undefined || raw==='') return null;
  const s = String(raw);
  const fromMicros = disp!==null && disp!==undefined && isFinite(Number(s)) && Number(s)!==0 && Math.abs(Number(s) - Number(disp)*1e6) < 0.5;
  if(unit && disp!==null && disp!==undefined && (/micros/i.test(unit) || (cur && unit===cur && fromMicros))){
    return {main: `${disp}${cur ? ' '+cur : ''}`, raw: s};
  }
  // a raw value that already carries its currency symbol (₺350,34) gets no second unit
  if(unit && !/micros/i.test(unit) && !s.includes(unit) && !(cur && /[₺€$£¥]/.test(s))) return {main: unit==='%' ? s+'%' : `${s} ${unit}`};
  return {main: s};
}
function valueText(c, which){ const p = valueParts(c, which); return p ? p.main + (p.raw ? ` (raw: ${p.raw})` : '') : ''; }
const PARSED_TIP = 'Read from the change summary text, not from a structured field';
function valueCellHtml(c, which){
  const p = valueParts(c, which);
  if(!p) return '—';
  return escapeHtml(p.main) + (p.raw ? ` <span class="note" style="margin:0;">(raw: ${escapeHtml(p.raw)})</span>` : '')
    + (c.value_confidence==='parsed_from_summary' ? `<span class="parsed" title="${PARSED_TIP}">parsed</span>` : '');
}
// Distinct accent per category — a visual identity aid, never a judgment.
// "Other" stays neutral on purpose. Dark mode lightens the hue via CSS (--dash-cat-keep).
const CATEGORY_COLORS = {
  Budget:'#1c4dc9', Bidding:'#7008e7', Keyword:'#00776e', Ad:'#c53c00', Asset:'#b75000',
  Targeting:'#c20039', Audience:'#0069a4', Status:'#007956', Campaign:'#432dd7',
  AdGroup:'#007492', Conversion:'#953d00', Feed:'#005f46', Account:'#5d0ec0',
};
function categoryPillAttrs(cat){
  const c = CATEGORY_COLORS[cat];
  return c ? ` class="pill cat" style="--pc:${c};"` : ' class="pill"';
}
const HUES = ['#2f6bff','#8d54ff','#00baa7','#f99c00','#ff2357','#00a5ef'];
function barFillStyle(color, pct){
  return `width:${Math.max(2,pct)}%;background:${color};`;
}
// =====================================================================
// RULE MATCHES (V1: Magnitude + State) — deterministic, user-adjustable
// thresholds. A match means "this row crossed a value you set," never a
// verdict; every match renders with the exact number and rule next to it.
// Matched on category/subcategory (resource_type/field_name are null for
// text-summary sources, category/subcategory are always resolved).
// =====================================================================
const MAGNITUDE_RULES = [
  { id:'budget_magnitude', label:'Budget change', defaultPct:50, match:c=>c.subcategory==='Budget changed' },
  { id:'target_cpa_magnitude', label:'Target CPA change', defaultPct:30, match:c=>c.subcategory==='Target CPA' },
  { id:'target_roas_magnitude', label:'Target ROAS change', defaultPct:30, match:c=>c.subcategory==='Target ROAS' },
  { id:'bid_magnitude', label:'Bid/CPC change', defaultPct:50, match:c=>c.subcategory==='Bid changed' },
];
// Twins of the Python summary_text_rules that make a pause/enable campaign-level
// (same patterns, case-sensitive, anchored at the start like re.search("^...")). Keep in sync.
const CAMPAIGN_PAUSED_TEXT = [/^\d+ campaigns? paused/, /^\d+ kampanya duraklat/];
const CAMPAIGN_ENABLED_TEXT = [/^\d+ campaigns? active/, /^\d+ kampanya etkin/];
function isCampaignResource(rt){ return /^(campaign|kampanya)$/i.test(String(rt||'').trim()); }
// Legacy "Durum Değişti: A -> B" carries old/new only in the text (F-1/B-16).
function statusFact(c, fallback){
  if(c.old_value && c.new_value) return `${c.old_value} → ${c.new_value}`;
  const m = /^Durum De[ğg]i[şs]ti:\s*(.+?)\s*->\s*(.+?)\s*$/m.exec(c.raw_summary||'');
  return m ? `${m[1]} → ${m[2]}` : fallback;
}
// PAUSED->ENABLED is off by default (campaign_enabled), an intentional asymmetry.
const STATE_RULES = [
  { id:'campaign_paused', label:'Campaign paused', defaultOn:true, match:c=>{
      if(c.subcategory!=='Paused') return false;
      if(c.resource_type) return isCampaignResource(c.resource_type);
      const s = c.raw_summary||'';
      return CAMPAIGN_PAUSED_TEXT.some(re=>re.test(s));
    },
    fact:c=> statusFact(c, 'status → PAUSED') },
  { id:'campaign_removed', label:'Campaign removed', defaultOn:true, match:c=> c.subcategory==='Campaign removed',
    fact:c=> `Operation: ${c.operation||'REMOVE'}` },
  { id:'ad_group_removed', label:'Ad group removed', defaultOn:true, match:c=> c.subcategory==='Ad group removed',
    fact:c=> `Operation: ${c.operation||'REMOVE'}` },
  { id:'campaign_enabled', label:'Campaign enabled', defaultOn:false, match:c=>{
      if(c.subcategory!=='Enabled') return false;
      if(c.resource_type) return isCampaignResource(c.resource_type);
      const s = c.raw_summary||'';
      return CAMPAIGN_ENABLED_TEXT.some(re=>re.test(s));
    },
    fact:c=> statusFact(c, 'status → ENABLED') },
];
function defaultRuleState(){
  return {
    enabled: false,
    pct: Object.fromEntries(MAGNITUDE_RULES.map(r=>[r.id, r.defaultPct])),
    stateOn: Object.fromEntries(STATE_RULES.map(r=>[r.id, r.defaultOn])),
  };
}
const ruleState = defaultRuleState();
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
      out.push({ id:r.id, label:r.label, pct:pctChange, detail:`${sign}${pctChange.toFixed(0)}% · rule: ≥±${threshold}%` });
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
// F-13: thresholds must be a number in (0, PCT_MAX]; anything else keeps the last valid value and says so.
const PCT_MAX = 1000;
function parseThreshold(v){
  const s = String(v===null || v===undefined ? '' : v).trim().replace(',', '.');
  if(s==='') return {ok:false, msg:'Enter a number'};
  const n = Number(s);
  if(!isFinite(n)) return {ok:false, msg:'Not a number'};
  if(n<=0) return {ok:false, msg:'Must be greater than 0'};
  if(n>PCT_MAX) return {ok:false, msg:`At most ${PCT_MAX}`};
  return {ok:true, value:n};
}
// F-6: turning highlighting off also drops the rule filter, so no hidden filter stays active.
function setRuleHighlighting(on){
  ruleState.enabled = !!on;
  if(!ruleState.enabled) state.ruleMatch = '';
  state.page = 1;
}
function defaultState(){
  return { accounts:new Set(), campaigns:new Set(), users:new Set(), actor:'', categories:new Set(), operations:new Set(), sources:new Set(), ruleMatch:'', range:'all', anchor:'data', search:'', page:1, pageSize:200, sortKey:'timestamp_iso', sortDir:-1, minDays:0 };
}
const state = defaultState();
// F-13: Clear filters resets everything, rule state and sort included.
function resetAll(){
  Object.assign(state, defaultState());
  const d = defaultRuleState();
  ruleState.enabled = d.enabled; Object.assign(ruleState.pct, d.pct); Object.assign(ruleState.stateOn, d.stateOn);
}
// F-2: drill-downs set identity filters, never a text search. Selecting the same thing again clears it.
function toggleOnly(key, value){
  const s = state[key];
  state[key] = (s.size===1 && s.has(value)) ? new Set() : new Set([value]);
  state.page = 1;
}
function drillCampaign(accKey, campKey){
  const same = state.campaigns.size===1 && state.campaigns.has(campKey) && state.accounts.size===1 && state.accounts.has(accKey);
  state.accounts = same ? new Set() : new Set([accKey]);
  state.campaigns = same ? new Set() : new Set([campKey]);
  state.page = 1;
}
function getFiltered(){
  const q = fold((state.search||'').trim());
  return DASH_DATA.changes.filter(c=>{
    if(state.accounts.size && !state.accounts.has(accountIdentity(c)||NONE)) return false;
    if(state.campaigns.size && !state.campaigns.has(campaignIdentity(c)||NONE)) return false;
    if(state.users.size && !state.users.has(actorKey(c))) return false;
    if(state.actor && c.actor_type !== state.actor) return false;
    if(state.categories.size && !state.categories.has(c.category)) return false;
    if(state.operations.size && !state.operations.has(c.operation)) return false;
    if(state.sources.size && !state.sources.has(c.source)) return false;
    if(state.ruleMatch){
      const matches = computeRuleMatches(c);
      if(state.ruleMatch==='__matched__'){ if(!matches.length) return false; }
      else if(!matches.some(m=>m.id===state.ruleMatch)) return false;
    }
    if(!withinRange(c)) return false;
    if(q && !rowHaystack(c).includes(q)) return false;
    return true;
  });
}
// ---- filter controls -------------------------------------------------
const MS_IDS = ['fAccount','fCampaign','fUser','fCategory','fOperation','fSource'];
const optionLabels = {};   // state key -> Map(value -> label)
function buildOptions(){
  const rows = DASH_DATA.changes;
  const add = (map, v, l)=>{ if(v!==null && v!==undefined && v!=='' && !map.has(v)) map.set(v, l); };
  const acc = new Map(), camp = new Map(), usr = new Map(), cat = new Map(), op = new Map(), src = new Map();
  rows.forEach(c=>{
    add(acc, accountIdentity(c)||NONE, c.account_name || (c.account_id ? `Account ${c.account_id}` : '(no account)'));
    add(camp, campaignIdentity(c)||NONE, c.campaign_name || (c.campaign_id ? `Campaign ${c.campaign_id}` : '(no campaign)'));
    add(usr, actorKey(c), actorLabel(c));
    add(cat, c.category, c.category);
    add(op, c.operation, (s=>s.length>80 ? s.slice(0,79)+'…' : s)(firstLine(c.operation)));
    add(src, c.source, c.source);
  });
  Object.assign(optionLabels, {accounts:acc, campaigns:camp, users:usr, categories:cat, operations:op, sources:src});
}
function msFill(el){
  const key = el.dataset.state;
  const opts = [...optionLabels[key].entries()].sort((a,b)=> String(a[1]).localeCompare(String(b[1])));
  el.querySelector('.ms-panel').innerHTML = `<button type="button" class="ms-clear">Clear selection</button>` + opts.map(([v,l])=>
    `<label class="ms-opt"><input type="checkbox" value="${escapeHtml(v)}"> <span>${escapeHtml(l)}</span></label>`).join('');
}
function labelFor(key, v){ const m = optionLabels[key]; return (m && m.has(v)) ? m.get(v) : String(v); }
// F-10: every control is re-synced from state on each render.
function syncControls(){
  MS_IDS.forEach(id=>{
    const el = document.getElementById(id); const key = el.dataset.state; const sel = state[key];
    el.querySelectorAll('input[type=checkbox]').forEach(cb=>{ cb.checked = sel.has(cb.value); });
    const sum = el.querySelector('summary');
    sum.classList.toggle('has-sel', sel.size>0);
    document.getElementById(id+'Sum').textContent = !sel.size ? el.dataset.all : sel.size===1 ? labelFor(key, [...sel][0]) : `${sel.size} selected`;
  });
  document.getElementById('fActor').value = state.actor;
  document.getElementById('fRuleMatch').value = state.ruleMatch;
  document.getElementById('fAnchor').value = state.anchor;
  document.getElementById('ruleMatchesToggle').checked = ruleState.enabled;
  ['fSearch','explorerSearch'].forEach(id=>{ const inp = document.getElementById(id); if(document.activeElement!==inp) inp.value = state.search; });
  document.querySelectorAll('#rangeBtns button').forEach(b=>{ const on = b.dataset.range===state.range; b.classList.toggle('active', on); b.setAttribute('aria-pressed', on); });
  document.querySelectorAll('#threshBtns button').forEach(b=>{ const on = parseInt(b.dataset.min)===state.minDays; b.classList.toggle('active', on); b.setAttribute('aria-pressed', on); });
}
function closePopovers(except){ document.querySelectorAll('details.ms[open]').forEach(d=>{ if(d!==except) d.open = false; }); }
let searchTimer = null;
function initFilters(){
  buildOptions();
  MS_IDS.forEach(id=>{
    const el = document.getElementById(id); const key = el.dataset.state;
    msFill(el);
    el.addEventListener('toggle', ()=>{ if(el.open) closePopovers(el); });
    el.addEventListener('change', e=>{
      if(e.target.type!=='checkbox') return;
      const s = new Set(state[key]);
      if(e.target.checked) s.add(e.target.value); else s.delete(e.target.value);
      state[key] = s; state.page = 1; render();
    });
    el.querySelector('.ms-clear').addEventListener('click', ()=>{ state[key] = new Set(); state.page = 1; render(); });
    el.addEventListener('keydown', e=>{ if(e.key==='Escape' && el.open){ e.stopPropagation(); el.open = false; el.querySelector('summary').focus(); } });
  });
  document.addEventListener('click', e=>{ if(!e.target.closest || !e.target.closest('details.ms')) closePopovers(null); });
  const fRuleMatch = document.getElementById('fRuleMatch');
  [...MAGNITUDE_RULES, ...STATE_RULES].forEach(r=>{
    const opt = document.createElement('option'); opt.value = r.id; opt.textContent = r.label; fRuleMatch.appendChild(opt);
  });
  fRuleMatch.addEventListener('change', e=>{
    state.ruleMatch = e.target.value; state.page = 1;
    // choosing a rule is an explicit ask to see rule reasoning: turn highlighting on
    if(state.ruleMatch && !ruleState.enabled) ruleState.enabled = true;
    render();
  });
  document.getElementById('fActor').addEventListener('change', e=>{ state.actor = e.target.value; state.page=1; render(); });
  document.getElementById('fAnchor').addEventListener('change', e=>{ state.anchor = e.target.value; state.page=1; render(); });
  document.getElementById('rangeBtns').addEventListener('click', e=>{
    const b = e.target.closest('button'); if(!b) return;
    state.range = b.dataset.range; state.page=1; render();
  });
  // C-5: debounced search (~150ms); both boxes share one query.
  const onSearch = e=>{
    state.search = e.target.value; state.page = 1;
    const other = document.getElementById(e.target.id==='fSearch' ? 'explorerSearch' : 'fSearch'); other.value = e.target.value;
    clearTimeout(searchTimer); searchTimer = setTimeout(render, 150);
  };
  document.getElementById('fSearch').addEventListener('input', onSearch);
  document.getElementById('explorerSearch').addEventListener('input', onSearch);
  document.getElementById('clearFilters').addEventListener('click', ()=>{
    resetAll(); closePopovers(null);
    document.querySelectorAll('#ruleMatchesMagnitudeInputs input').forEach(inp=>{ inp.value = ruleState.pct[inp.dataset.ruleId]; inp.removeAttribute('aria-invalid'); });
    document.querySelectorAll('#ruleMatchesMagnitudeInputs .cfg-err').forEach(e=>{ e.textContent=''; });
    document.querySelectorAll('#ruleMatchesStateInputs input').forEach(cb=>{ cb.checked = ruleState.stateOn[cb.dataset.ruleId]; });
    render();
  });
}
// ---- sections --------------------------------------------------------
function renderSummary(filtered){
  const humanCount = new Set(filtered.filter(c=>c.actor_type==='human').map(userIdentity)).size;
  const accountSet = new Set(filtered.map(accountIdentity).filter(Boolean));
  const campaigns = new Set(filtered.filter(c=>campaignIdentity(c)).map(c=>accountIdentity(c)+'||'+campaignIdentity(c))).size;
  const adgroups = new Set(filtered.filter(c=>adGroupIdentity(c)).map(c=>accountIdentity(c)+'||'+campaignIdentity(c)+'||'+adGroupIdentity(c))).size;
  let last = null;
  filtered.forEach(c=>{ const t = rowTime(c); if(t!=null && (!last || t>rowTime(last))) last = c; });
  const userCounts = {}, userDisplay = {};
  filtered.forEach(c=>{ if(c.actor_type==='human'){ const k=userIdentity(c); userCounts[k]=(userCounts[k]||0)+1; userDisplay[k]=c.user_name||c.user_email; }});
  const topUserKey = Object.entries(userCounts).sort((a,b)=>b[1]-a[1])[0];
  const campCounts = {}, campDisplay = {};
  filtered.forEach(c=>{
    if(!campaignIdentity(c)) return;
    const key = accountIdentity(c)+'||'+campaignIdentity(c);
    campCounts[key] = (campCounts[key]||0)+1;
    const name = c.campaign_name || `Campaign ${c.campaign_id}`;
    campDisplay[key] = accountSet.size <= 1 ? name : `${name} (${c.account_name||''})`;
  });
  const topCampKey = Object.entries(campCounts).sort((a,b)=>b[1]-a[1])[0];
  const stats = [
    ['Total Changes', filtered.length.toLocaleString()], ['Active Accounts', accountSet.size], ['Active Users', humanCount],
    ['Changed Campaigns', campaigns], ['Changed Ad Groups', adgroups],
    ['Last Change', last ? fmtTs(last) : '—'],
    ['Most Active User', topUserKey ? (userDisplay[topUserKey[0]]||'—') : '—'], ['Most Active Campaign', topCampKey ? campDisplay[topCampKey[0]] : '—'],
  ];
  // Plain tiles: no card's colour should read as meaning something.
  document.getElementById('summaryGrid').innerHTML = stats.map(([l,n])=>
    `<div class="stat"><div class="n">${escapeHtml(n)}</div><div class="l">${l}</div></div>`
  ).join('');
}
// Days are the source's own local days when its timezone is known (D-5);
// rows with no timezone can't be placed on a shared axis and are counted apart.
function renderTimeline(filtered){
  const counts = {}; let excluded = 0; const tzs = new Set();
  // same basis as the JSON (meta.timeline_reference_tz): UTC days only when the run had no zone
  const utcDays = !!(DASH_DATA.meta && DASH_DATA.meta.timeline_reference_tz==='UTC');
  filtered.forEach(c=>{
    if(c.timestamp_utc){ const d = String(utcDays ? c.timestamp_utc : (c.timestamp_iso || c.timestamp_utc)).slice(0,10); counts[d]=(counts[d]||0)+1; if(c.timezone) tzs.add(c.timezone); }
    else excluded++;
  });
  const basis = (DASH_DATA.meta && DASH_DATA.meta.timeline_basis) || (utcDays ? 'UTC day buckets' : tzs.size===1 ? `${[...tzs][0]} day buckets` : tzs.size>1 ? 'day buckets in each row\'s own timezone' : 'day buckets');
  document.getElementById('timelineBasis').textContent = `(${basis})`;
  const days = Object.keys(counts).sort();
  const max = Math.max(1,...days.map(d=>counts[d]));
  const el = document.getElementById('timeline');
  document.getElementById('timelineReadout').textContent = '';
  if(!days.length){ el.innerHTML = '<div class="empty">No timezone-resolved rows to plot.</div>'; return; }
  el.innerHTML = days.map((d,i)=>{
    const edge = i===0 || i===days.length-1;
    const h = Math.max(4, Math.round(counts[d]/max*100));
    const text = `${fmtWall(d,false)}: ${counts[d]} change${counts[d]===1?'':'s'}`;
    // one colour, height only; first/last day lighter (may be partial)
    return `<div class="tl-bar${edge?' edge':''}" role="listitem" tabindex="0" aria-label="${escapeHtml(text)}" data-text="${escapeHtml(text)}" style="height:${h}%;"><div class="tip" aria-hidden="true">${escapeHtml(text)}</div></div>`;
  }).join('') + (excluded ? `<div class="note" style="width:100%;margin-top:6px;">${excluded} row(s) excluded — timezone unknown.</div>` : '');
  const show = e=>{ const b = e.target.closest('.tl-bar'); if(b) document.getElementById('timelineReadout').textContent = b.dataset.text; };
  if(!el._bound){ el.addEventListener('click', show); el.addEventListener('focusin', show); el._bound = true; }
}
// items: {key, name, count, color?}; each bar is a real button (keyboard and screen-reader operable).
function barList(container, items, onClick, color, selected){
  const max = Math.max(1, ...items.map(i=>i.count));
  container.innerHTML = items.length ? items.map(i=>{
    const col = i.color || color || HUES[0];
    const key = i.key ?? i.name;
    return `<button type="button" class="bar-row" data-key="${escapeHtml(key)}" aria-pressed="${selected && selected.has(key) ? 'true' : 'false'}">
      <span class="bar-label" title="${escapeHtml(i.name)}">${escapeHtml(i.name)}</span>
      <span class="bar-track"><span class="bar-fill" style="${barFillStyle(col, i.count/max*100)}"></span></span>
      <span class="bar-n">${i.count.toLocaleString()}</span>
    </button>`;
  }).join('') : '<div class="empty">No data for current filters.</div>';
  if(onClick) container.querySelectorAll('.bar-row').forEach(row=> row.addEventListener('click', ()=> onClick(row.dataset.key)));
}
function renderUsers(filtered){
  const groups = {h:{}, a:{}, u:{}}, disp = {};
  filtered.forEach(c=>{ const k = actorKey(c); const g = k[0]; groups[g][k] = (groups[g][k]||0)+1; disp[k] = actorLabel(c); });
  const list = g => Object.entries(groups[g]).sort((a,b)=>b[1]-a[1]).map(([key,count])=>({key, name: g==='a' ? key.slice(2) : disp[key], count}));
  const el = document.getElementById('userActivity');
  el.innerHTML = '<div class="grp-label">Human</div><div id="humanBars"></div><div class="grp-label">Automation</div><div id="autoBars"></div><div class="grp-label">Unknown</div><div id="unknownBars"></div>';
  const pick = key=>{ toggleOnly('users', key); render(); };
  barList(document.getElementById('humanBars'), list('h'), pick, HUES[0], state.users);
  barList(document.getElementById('autoBars'), list('a'), pick, '#f99c00', state.users);
  barList(document.getElementById('unknownBars'), list('u'), pick, '#98918f', state.users);
}
const openAccounts = new Set();
// F-9: the tree is keyed by account identity, not display name.
function renderAccounts(filtered){
  const accMap = {}, accName = {}, campName = {};
  filtered.forEach(c=>{
    const ak = accountIdentity(c) || NONE;
    accName[ak] = c.account_name || (c.account_id ? `Account ${c.account_id}` : '(no account)');
    const ck = campaignIdentity(c) || NONE;
    accMap[ak] = accMap[ak] || {};
    accMap[ak][ck] = (accMap[ak][ck]||0)+1;
    campName[ck] = c.campaign_name || (c.campaign_id ? `Campaign ${c.campaign_id}` : '(no campaign)');
  });
  const accounts = Object.entries(accMap).map(([key,camps])=>({key, camps, total: Object.values(camps).reduce((a,b)=>a+b,0)})).sort((a,b)=>b.total-a.total);
  const el = document.getElementById('accountActivity');
  if(!accounts.length){ el.innerHTML='<div class="empty">No data for current filters.</div>'; return; }
  if(accounts.length===1) openAccounts.add(accounts[0].key);
  const maxAcc = Math.max(1, ...accounts.map(a=>a.total));
  const col = HUES[0];
  el.innerHTML = accounts.map((a,idx)=>{
    const open = openAccounts.has(a.key);
    const cmax = Math.max(...Object.values(a.camps));
    return `
    <div class="acc-block">
      <button type="button" class="acc-head" data-acc="${escapeHtml(a.key)}" aria-expanded="${open}" aria-controls="camp-${idx}">
        <span class="chev" aria-hidden="true">▶</span>
        <span class="bar-label">${escapeHtml(accName[a.key])}</span>
        <span class="bar-track"><span class="bar-fill" style="${barFillStyle(col, a.total/maxAcc*100)}"></span></span>
        <span class="bar-n">${a.total.toLocaleString()}</span>
      </button>
      <div class="camp-list" id="camp-${idx}"${open ? '' : ' hidden'}>
        ${Object.entries(a.camps).sort((x,y)=>y[1]-x[1]).map(([ck,n])=>{
          const on = state.campaigns.size===1 && state.campaigns.has(ck) && state.accounts.has(a.key);
          return `<button type="button" class="bar-row" data-acc="${escapeHtml(a.key)}" data-camp="${escapeHtml(ck)}" aria-pressed="${on}"><span class="bar-label" title="${escapeHtml(campName[ck])}">${escapeHtml(campName[ck])}</span><span class="bar-track"><span class="bar-fill" style="${barFillStyle(col, n/cmax*100)}opacity:.75;"></span></span><span class="bar-n">${n.toLocaleString()}</span></button>`;
        }).join('')}
      </div>
    </div>`;
  }).join('');
  el.querySelectorAll('.acc-head').forEach(h=> h.addEventListener('click', ()=>{
    const k = h.dataset.acc; if(openAccounts.has(k)) openAccounts.delete(k); else openAccounts.add(k);
    const open = openAccounts.has(k);
    h.setAttribute('aria-expanded', open); document.getElementById(h.getAttribute('aria-controls')).hidden = !open;
  }));
  el.querySelectorAll('.camp-list .bar-row').forEach(row=> row.addEventListener('click', ()=>{ drillCampaign(row.dataset.acc, row.dataset.camp); render(); }));
}
function renderCategories(filtered){
  const counts = {}; filtered.forEach(c=> counts[c.category]=(counts[c.category]||0)+1);
  const list = Object.entries(counts).sort((a,b)=>b[1]-a[1]).map(([name,count])=>({name,count,color:CATEGORY_COLORS[name]||'#98918f'}));
  barList(document.getElementById('categoryDist'), list, key=>{ toggleOnly('categories', key); render(); }, null, state.categories);
}
function bindRowOpen(container){
  container.querySelectorAll('tr.datarow').forEach(row=>{
    row.addEventListener('click', ()=> openDetail(Number(row.dataset.idx)));
    row.addEventListener('keydown', e=>{ if(e.key==='Enter' || e.key===' '){ e.preventDefault(); openDetail(Number(row.dataset.idx)); } });
  });
}
function rowIndex(c){ return c._i!==undefined ? c._i : DASH_DATA.changes.indexOf(c); }
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
  // C-7: the % and the old -> new values are visible text, not only a tooltip.
  el.innerHTML = `<div class="tbl-wrap" tabindex="0" role="region" aria-label="Rule matches table"><table><thead><tr><th>Date</th><th>Campaign</th><th>Change</th><th>Old → New</th><th>Matched rule(s)</th></tr></thead><tbody>${
    matched.slice(0, SHOWN).map(({c,matches})=>`<tr class="datarow" tabindex="0" data-idx="${rowIndex(c)}">
      <td>${escapeHtml(fmtTs(c))}</td>
      <td>${escapeHtml(c.campaign_name || (c.campaign_id ? `Campaign ${c.campaign_id}` : '—'))}</td>
      <td>${escapeHtml(c.field_name || c.subcategory || '—')}</td>
      <td>${escapeHtml(valueText(c,'old') || '—')} → ${escapeHtml(valueText(c,'new') || '—')}</td>
      <td style="white-space:normal;min-width:180px;">${matches.map(m=>`<span class="pill pill-rule">${escapeHtml(m.label)}</span><span class="rm-detail">${escapeHtml(m.detail)}</span>`).join('')}</td>
    </tr>`).join('')
  }</tbody></table></div>${matched.length > SHOWN ? `<div class="note">Showing first ${SHOWN} of ${matched.length} matches (still counted correctly above) — narrow with the top filters to see the rest.</div>` : ''}`;
  bindRowOpen(el);
}
function initRuleMatchesConfig(){
  const magEl = document.getElementById('ruleMatchesMagnitudeInputs');
  magEl.innerHTML = MAGNITUDE_RULES.map(r=>`
    <label class="cfg-field">
      ${escapeHtml(r.label)}
      <span style="display:flex;align-items:center;gap:6px;">±<input type="number" class="ctl" min="1" max="${PCT_MAX}" step="any" value="${r.defaultPct}" data-rule-id="${r.id}" aria-describedby="err-${r.id}" style="width:80px;">%</span>
      <span class="cfg-err" id="err-${r.id}" aria-live="polite"></span>
    </label>`).join('');
  magEl.querySelectorAll('input[type=number]').forEach(inp=>{
    inp.addEventListener('input', e=>{
      const id = e.target.dataset.ruleId; const res = parseThreshold(e.target.value);
      const err = document.getElementById('err-'+id);
      if(res.ok){ ruleState.pct[id] = res.value; e.target.removeAttribute('aria-invalid'); err.textContent = ''; render(); }
      else { e.target.setAttribute('aria-invalid', 'true'); err.textContent = `${res.msg} (1–${PCT_MAX}). Still using ${ruleState.pct[id]}%.`; }
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
  document.getElementById('ruleMatchesToggle').addEventListener('change', e=>{ setRuleHighlighting(e.target.checked); render(); });
}
// Recomputed from `filtered` on every render. "Days since" uses the same row
// time (timestamp_utc) and the browser's clock, same basis as "Range ends at: Today".
function renderUntouched(filtered){
  const campaignLast = {};
  filtered.forEach(c=>{
    const cid = campaignIdentity(c); const t = rowTime(c);
    if(!cid || t==null) return;
    const key = (accountIdentity(c)||NONE) + '||' + cid;
    if(!campaignLast[key] || t > campaignLast[key]._t){
      campaignLast[key] = { campaign_name: c.campaign_name || `Campaign ${c.campaign_id}`, account_name: c.account_name, row: c, _t: t };
    }
  });
  const now = Date.now();
  const rows = Object.values(campaignLast).map(u=>({...u, days_since: Math.floor((now - u._t) / 86400000)}))
    .sort((a,b)=> b.days_since - a.days_since).filter(u=>u.days_since >= state.minDays);
  const el = document.getElementById('untouched');
  if(!rows.length){ el.innerHTML='<div class="empty">No campaigns match this threshold (or current filters).</div>'; return; }
  el.innerHTML = `<div class="tbl-wrap" tabindex="0" role="region" aria-label="Campaign last changes table"><table><thead><tr><th>Campaign</th><th>Account</th><th>Last Change</th><th>Days Since (today)</th></tr></thead><tbody>${
    rows.map(u=>`<tr><td>${escapeHtml(u.campaign_name)}</td><td>${escapeHtml(u.account_name||'')}</td><td>${escapeHtml(fmtTs(u.row))}</td><td>${u.days_since}</td></tr>`).join('')
  }</tbody></table></div>`;
}
// Old/New sort numerically when both parsed as numbers (strings sort "100" before "20").
function explorerSortValue(c, k){
  if(k==='old_value' && c.old_value_num!=null) return c.old_value_num;
  if(k==='new_value' && c.new_value_num!=null) return c.new_value_num;
  if(k==='timestamp_iso'){ const t = rowTime(c); return t==null ? '' : t; }
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
  state.page = Math.max(1, Math.min(state.page, totalPages));
  const pageRows = sorted.slice((state.page-1)*state.pageSize, state.page*state.pageSize);
  const body = document.getElementById('explorerBody');
  body.innerHTML = pageRows.length ? pageRows.map(c=>{
    const summary = firstLine(c.raw_summary);
    return `
    <tr class="datarow" tabindex="0" data-idx="${rowIndex(c)}">
      <td>${escapeHtml(fmtTs(c))}</td>
      <td>${c.actor_type==='human' ? escapeHtml(c.user_name||c.user_email||'—') : c.actor_type==='automation' ? `<span class="pill pill-auto">${escapeHtml(c.client_type||c.user_name||'automation')}</span>` : '<span class="pill">unknown</span>'}</td>
      <td>${escapeHtml(c.account_name||'')}</td><td${c.campaign_confidence==='inferred_from_summary' ? ' title="Campaign name read from the summary text"' : ''}>${escapeHtml(c.campaign_name||'—')}</td><td>${escapeHtml(c.ad_group_name||'—')}</td>
      <td><span${categoryPillAttrs(c.category)}>${escapeHtml(c.category)}</span></td><td>${escapeHtml(c.field_name || c.subcategory || '—')}</td>
      <td title="${escapeHtml(c.raw_summary||'')}">${escapeHtml(summary||'—')}</td>
      <td>${valueCellHtml(c,'old')}</td>
      <td>${valueCellHtml(c,'new')}</td>
      <td>${ruleState.enabled ? (ruleMatchPillsHtml(computeRuleMatches(c)) || '—') : '—'}</td>
      <td>${escapeHtml(c.source)}</td>
    </tr>`; }).join('') : `<tr><td colspan="12" class="empty">No changes match current filters.</td></tr>`;
  bindRowOpen(body);
  document.getElementById('pagerInfo').textContent = `Page ${state.page} / ${totalPages} — ${sorted.length.toLocaleString()} rows`;
  document.getElementById('pagerPrev').disabled = state.page<=1;
  document.getElementById('pagerNext').disabled = state.page>=totalPages;
  document.querySelectorAll('#explorerTable th').forEach(th=>{
    if(th.dataset.key===state.sortKey) th.setAttribute('aria-sort', state.sortDir<0 ? 'descending' : 'ascending'); else th.removeAttribute('aria-sort');
  });
}
// Rows that share one event_id came from one source cell split into statements (D-3).
let eventGroups = null;
function eventGroup(c){
  if(!eventGroups){
    eventGroups = {};
    DASH_DATA.changes.forEach(r=>{ if(r.event_id){ (eventGroups[r.event_id] = eventGroups[r.event_id] || []).push(r); } });
  }
  const g = c.event_id ? eventGroups[c.event_id] : null;
  return g && g.length > 1 ? g : null;
}
let lastFocus = null;
function openDetail(ref){
  const c = typeof ref==='number' ? DASH_DATA.changes[ref] : DASH_DATA.changes.find(x=>x.change_id===ref);
  if(!c) return;
  const vText = which=>{ const t = valueText(c, which); return t ? t + (c.value_confidence==='parsed_from_summary' ? ' (parsed from summary text)' : '') : '—'; };
  const hasValue = valueParts(c,'old') || valueParts(c,'new');
  const grp = eventGroup(c);
  let stmt = null;
  let fullCell = null;
  if(grp){
    const ordered = grp.every(r=>typeof r.statement_index==='number') ? [...grp].sort((a,b)=>a.statement_index-b.statement_index) : grp;
    const pos = typeof c.statement_index==='number' ? c.statement_index+1 : ordered.indexOf(c)+1;
    stmt = `${pos} of ${c.statement_count || grp.length} change statements from one source cell`;
    fullCell = ordered.map(r=>r.raw_summary||'').join('\n');
  }
  const rows = [
    ['Date', fmtTs(c) + (c.timezone ? ` (${c.timezone})` : (c.timestamp_utc ? '' : ' (timezone unknown)'))],
    ['User', c.actor_type==='human' ? (c.user_name||c.user_email||'—') : c.actor_type==='automation' ? `${c.client_type||c.user_name||'Automation'} (automation)` : 'Unknown (no identity in source)'],
    ['Account', c.account_name||'—'],
    ['Campaign', (c.campaign_name||'—') + (c.campaign_confidence==='inferred_from_summary' ? ' (read from the summary text)' : '')],
    ['Ad Group', c.ad_group_name||'—'],
    ['Category', `${c.category} — ${c.subcategory}${c.category_confidence==='fallback_other' ? ' (no rule matched)' : ''}`], ['Field', c.field_name || '—'],
    ['Previous', vText('old')], ['New', vText('new')],
    ['Operation', c.operation_confidence==='inferred' ? `${c.operation} (inferred, not stated by source)` : c.operation],
    ['Source', c.source],
    ['Value read from', !hasValue ? 'no value in the source' : c.value_confidence==='parsed_from_summary' ? 'summary text (parsed, not a structured field)' : 'structured field'],
    ['Summary', c.raw_summary || '—'],
  ];
  if(stmt) rows.push(['Statement', stmt], ['Full source cell', fullCell]);
  rows.push(['Resource', c.resource_path || c.resource_id || '—'], ['Event ID', c.source_event_id || `${c.change_id} (derived, not from source)`]);
  document.getElementById('detailBody').innerHTML = rows.map(([k,v])=>`<dt>${escapeHtml(k)}</dt><dd${k==='Summary' || k==='Operation' || k==='Full source cell' ? ' class="pre"' : ''}>${escapeHtml(v)}</dd>`).join('');
  lastFocus = document.activeElement;
  document.getElementById('detailOverlay').classList.add('open');
  document.getElementById('detailClose').focus();
}
function closeDetail(){
  const ov = document.getElementById('detailOverlay');
  if(!ov.classList.contains('open')) return;
  ov.classList.remove('open');
  if(lastFocus && document.contains(lastFocus)) lastFocus.focus();
  lastFocus = null;
}
// The stated filter-context line, always present (F-6: includes the rule filter; F-7: names the range anchor).
function rangeLabel(){
  if(state.range==='all') return '';
  const span = state.range==='1' ? '24 hours' : `${state.range} days`;
  return state.anchor==='today' ? `last ${span} to today (${fmtEpochDate(Date.now())})` : `last ${span} of data, to ${fmtEpochDate(dataEndTime())}`;
}
function renderFilterContext(){
  const parts = [];
  const r = rangeLabel(); if(r) parts.push(r);
  const named = (key, one, many)=>{ const s = state[key]; if(s.size===1) parts.push(`${one}: ${labelFor(key, [...s][0])}`); else if(s.size) parts.push(`${s.size} ${many}`); };
  named('accounts','account','accounts'); named('campaigns','campaign','campaigns'); named('users','user','users');
  if(state.actor) parts.push(`actor: ${state.actor}`);
  named('categories','category','categories'); named('operations','operation','operation types'); named('sources','source','sources');
  if(state.ruleMatch){ const rr = [...MAGNITUDE_RULES, ...STATE_RULES].find(x=>x.id===state.ruleMatch); parts.push(`rule: ${rr ? rr.label : 'matched a rule'}`); }
  if(state.search) parts.push(`search: "${state.search}"`);
  document.getElementById('filterContext').textContent = parts.length ? `Showing: ${parts.join(' · ')}` : 'Showing: all changes, no filters applied';
}
// Re-rendering replaces the bar/row buttons; put keyboard focus back on the same item.
function focusMemo(){
  const ae = document.activeElement;
  if(!ae || !ae.dataset || !ae.closest) return null;
  const host = ae.closest('#userActivity,#accountActivity,#categoryDist,#explorerBody,#ruleMatches');
  return host ? {host: host.id, tag: ae.tagName, cls: ae.className, ds: {...ae.dataset}} : null;
}
function focusRestore(f){
  if(!f) return;
  const host = document.getElementById(f.host); if(!host) return;
  const hit = [...host.querySelectorAll(f.tag)].find(el=> el.className===f.cls && Object.keys(f.ds).every(k=>el.dataset[k]===f.ds[k]));
  if(hit) hit.focus();
}
function render(){
  clearTimeout(searchTimer);
  const memo = focusMemo();
  const filtered = getFiltered();
  syncControls();
  renderFilterContext();
  renderSummary(filtered); renderTimeline(filtered); renderUsers(filtered);
  renderAccounts(filtered); renderCategories(filtered); renderRuleMatches(filtered);
  renderUntouched(filtered); renderExplorer(filtered);
  focusRestore(memo);
}
document.addEventListener('DOMContentLoaded', ()=>{
  const m = DASH_DATA.meta;
  DASH_DATA.changes.forEach((c,i)=>{ c._i = i; });
  UI_LOCALE = detectLocale();
  let first = null, last = null;
  DASH_DATA.changes.forEach(c=>{ const t = rowTime(c); if(t==null) return; if(!first || t<rowTime(first)) first = c; if(!last || t>rowTime(last)) last = c; });
  const winStart = first ? fmtWall(first.timestamp_iso || first.timestamp_utc, false) : '—';
  const winEnd = last ? fmtWall(last.timestamp_iso || last.timestamp_utc, false) : '—';
  document.getElementById('metaLine').textContent =
    `${Number(m.row_count||0).toLocaleString()} changes · ${winStart} – ${winEnd} · Sources: ${(m.sources||[]).join(', ')} · Other: ${m.other_pct}%${m.masked_users ? ' · Users masked' : ''} · Generated ${fmtWall(m.generated_at, true)}`;
  // source_event_id is only ever populated by the API's own event-identity field.
  const caveats = [];
  if(DASH_DATA.changes.some(c=>c.source_event_id)){
    caveats.push('Includes data from the Google Ads ChangeEvent API. ChangeEvent may not include every entry from the Google Ads UI\'s Change History — treat counts from this source as a lower bound, not a complete history.');
  }
  if(m.rows_skipped_unparseable_date_pct > 5){
    caveats.push(`${m.rows_skipped_unparseable_date} row(s) (${m.rows_skipped_unparseable_date_pct}% of input) were silently excluded — their date column didn't parse under the format this run resolved. See coverage.txt.`);
  }
  // What the run read vs. what is shown here — counts only, no verdict. Every key optional.
  const dq = m.data_quality;
  if(dq){
    const n = x => Number(x||0).toLocaleString();
    const lim = dq.in_report_with_limits || {}, lo = dq.left_out || {};
    const flags = [['category_other','categorized "Other"'],['timezone_unknown','no timezone'],['operation_inferred','operation inferred'],
      ['value_parsed_from_summary','value read from summary text'],['multi_field_unsplit','several fields in one row']]
      .filter(([k])=>lim[k]>0).map(([k,label])=>`${n(lim[k])} ${label}`);
    const split = [];
    if(dq.rows_added_by_statement_split>0) split.push(`${n(dq.rows_added_by_statement_split)} added by splitting multi-statement cells`);
    if(dq.rows_added_by_field_split>0) split.push(`${n(dq.rows_added_by_field_split)} added by splitting multi-field rows`);
    const left = [`${n(lo.unreadable_date)} unreadable date`, `${n(lo.duplicate)} duplicate`];
    const dup = dq.possible_duplicate_rows_kept ?? dq.possible_duplicates;
    if(dup>0) flags.push(`${n(dup)} identical rows kept (possible duplicates)`);
    if(dq.campaign_inferred_from_summary>0) flags.push(`${n(dq.campaign_inferred_from_summary)} campaign read from summary text`);
    const dst = (dq.dst_nonexistent_local_time||0) + (dq.dst_ambiguous_local_time||0);
    if(dst>0) flags.push(`${n(dst)} in a daylight-saving gap or repeated hour`);
    document.getElementById('dataQuality').textContent =
      `Input: ${n(dq.input_rows)} rows · in this report: ${n(dq.rows_in_report)}` + (split.length ? ` (${split.join(', ')})` : '')
      + ` · left out: ${left.join(', ')}`
      + (flags.length ? ` · of the rows shown: ${flags.join(', ')}` : '')
      + ((dq.ignored_headers||[]).length ? ` · source columns not used: ${dq.ignored_headers.join(', ')}` : '');
    if(Array.isArray(dq.warnings)) dq.warnings.forEach(w=>{ if(w) caveats.push(typeof w==='string' ? w : (w.message || JSON.stringify(w))); });
    if(dq.rows_added_by_statement_split>0){
      document.getElementById('explorerCaption').textContent = '1 row = 1 change statement: a source cell that held several statements is shown as one row per statement (see the Statement line in the detail). ' + document.getElementById('explorerCaption').textContent;
    }
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
    const b = e.target.closest('button'); if(!b) return;
    state.minDays = parseInt(b.dataset.min) || 0; syncControls(); renderUntouched(getFiltered());
  });
  document.getElementById('pagerPrev').addEventListener('click', ()=>{ state.page--; renderExplorer(getFiltered()); });
  document.getElementById('pagerNext').addEventListener('click', ()=>{ state.page++; renderExplorer(getFiltered()); });
  document.querySelectorAll('#explorerTable th .th-btn').forEach(btn=> btn.addEventListener('click', ()=>{
    const th = btn.closest('th');
    if(state.sortKey===th.dataset.key) state.sortDir *= -1; else { state.sortKey=th.dataset.key; state.sortDir=-1; }
    state.page=1; renderExplorer(getFiltered());
  }));
  // C-2: the detail panel is a modal dialog — focus moves in, Esc closes, focus returns to the opener.
  document.getElementById('detailClose').addEventListener('click', closeDetail);
  document.getElementById('detailOverlay').addEventListener('click', e=>{ if(e.target.id==='detailOverlay') closeDetail(); });
  document.addEventListener('keydown', e=>{
    if(!document.getElementById('detailOverlay').classList.contains('open')) return;
    if(e.key==='Escape'){ e.preventDefault(); closeDetail(); }
    else if(e.key==='Tab'){ e.preventDefault(); document.getElementById('detailClose').focus(); }
  });
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
_TR_FOLD = str.maketrans({"İ": "i", "I": "i", "ı": "i"})


def _fold(text):
    """Case/accent-insensitive key for query matching, Turkish-aware (2026-11-10):
    İ/I/ı/i all fold to i, then accents are stripped (ş->s, ğ->g, ü->u)."""
    import unicodedata
    t = unicodedata.normalize("NFD", str(text or "").translate(_TR_FOLD)).casefold()
    return "".join(ch for ch in t if not unicodedata.combining(ch)).replace("ı", "i")


def _row_instant(r):
    """Aware UTC datetime for a row, or None. timestamp_utc first; a naive
    timestamp_iso is read as this machine's local time (2026-11-10)."""
    for key in ("timestamp_utc", "timestamp_iso"):
        val = r.get(key)
        if not val:
            continue
        try:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except ValueError:
            continue
        return (dt if dt.tzinfo else dt.astimezone()).astimezone(dt_timezone.utc)
    return None


def query_changes(rows, user=None, account=None, campaign=None, category=None, operation=None, source=None, since=None,
                  since_anchor="now", info=None):
    """user/account/campaign/source are substring matches (grep-style), on a
    Turkish-aware case/accent fold — deliberately more forgiving than the
    dashboard's exact-match dropdowns. --since counts back from now by
    default; since_anchor="newest" counts back from the newest row instead.
    The anchor used is written to info["since_anchor"] when info is a dict."""
    cutoff = None
    if since:
        if not SINCE_RE.match(since):
            raise ValueError(f"Invalid --since value: {since!r}. Expected a positive number followed by d/w/m, e.g. '7d', '2w', '1m'.")
        if since_anchor not in ("now", "newest"):
            raise ValueError(f"Invalid --since-anchor value: {since_anchor!r}. Expected 'now' or 'newest'.")
        n = int(since[:-1])
        unit = since[-1]
        days = n if unit == "d" else n * 7 if unit == "w" else n * 30
        if days > 3_650_000:
            raise ValueError(f"--since value {since!r} is too large; use at most 3650000 days.")
        instants = [t for t in (_row_instant(r) for r in rows) if t]
        if since_anchor == "newest" and instants:
            ref = max(instants)
        else:
            ref = datetime.now(dt_timezone.utc)
        cutoff = ref - timedelta(days=days)
        if isinstance(info, dict):
            info["since_anchor"] = {"basis": since_anchor if (since_anchor == "now" or instants) else "now", "anchor_utc": ref.isoformat(), "cutoff_utc": cutoff.isoformat()}

    def has(needle, *hay):
        return _fold(needle) in _fold(" ".join(str(h) for h in hay if h))

    def keep(r):
        if user and not has(user, r.get("user_name"), r.get("user_email")):
            return False
        if account and not has(account, r.get("account_name"), r.get("account_id")):
            return False
        if campaign and not has(campaign, r.get("campaign_name")):
            return False
        if category and _fold(category) != _fold(r.get("category")):
            return False
        if operation and operation.upper() != (r.get("operation") or "").upper():
            return False
        if source and not has(source, r.get("source")):
            return False
        if cutoff is not None:
            t = _row_instant(r)
            if t is None or t < cutoff:
                return False
        return True

    matched = [r for r in rows if keep(r)]
    matched.sort(key=lambda r: _row_instant(r) or datetime.min.replace(tzinfo=dt_timezone.utc), reverse=True)
    return matched


# =====================================================================
# ORCHESTRATION
# =====================================================================
RUN_OUTPUT_FILES = ("dashboard.html", "change_history.json", "changes.jsonl", "coverage.txt",
                    "resolved_mapping.json", "unknown-fields.json", "run_status.json", "digest.md")


def _prepare_out_dir(out_dir):
    """2026-11-10: refuse to write through a symlinked output file, and clear the
    previous run's outputs so a failed run never leaves them looking current."""
    for name in RUN_OUTPUT_FILES:
        if (out_dir / name).is_symlink():
            return {"status": "error", "message": f"Refusing to write through a symbolic link: {out_dir / name}. Remove it or choose another --out-dir."}
    for name in RUN_OUTPUT_FILES:
        try:
            (out_dir / name).unlink()
        except FileNotFoundError:
            pass
    return None


def _is_empty_event_list(input_path):
    """True for a JSON file holding an empty list: what tools/fetch_live_data.py
    writes when the API returned zero events (2026-11-10)."""
    p = Path(input_path)
    if p.suffix.lower() != ".json" or not p.is_file() or p.stat().st_size > 64:
        return False
    try:
        return json.loads(p.read_text(encoding="utf-8-sig")) == []
    except (ValueError, UnicodeDecodeError):
        return False


def _open_dashboard(path):
    try:
        return bool(webbrowser.open(Path(path).resolve().as_uri()))
    except Exception:
        return False


# 2026-11-14 (round 10): digest.md, a short factual Markdown summary to paste
# into Slack/e-mail. Counts and neutral words only; set from --digest-lang.
DIGEST_LANG = "auto"
_DIGEST_TR_RE = re.compile(r"(değiştirildi|oluşturuldu|eklendi|kaldırıldı|duraklatıldı|ayarlandı|güncellendi|olarak|reklam grubu|bütçe|teklif)", re.IGNORECASE)
_DIGEST_EN_RE = re.compile(r"\b(changed|created|added|removed|paused|enabled|updated|set to|ad group|budget|bid)\b", re.IGNORECASE)
_DIGEST_TEXT = {
    "en": {
        "title": "Google Ads change digest", "period": "Period", "sources": "Source files", "source_types": "Source type", "rows": "rows",
        "totals": "Totals", "metric": "Measure", "value": "Value", "changes": "Changes", "events": "Change events",
        "accounts": "Accounts", "humans": "People", "automation": "Automation actors", "campaigns": "Campaigns changed",
        "ad_groups": "Ad groups changed", "by_category": "Changes per category", "category": "Category", "count": "Changes",
        "share": "Share", "by_user": "Changes per user", "user": "User", "type": "Type", "human": "person",
        "auto": "automation", "masked": "User names are masked (--mask-users).", "by_account": "Changes per account",
        "account": "Account", "by_week": "Changes per ISO week", "week": "Week", "days": "Days",
        "top": "Most-changed campaigns (top 10 by count)", "campaign": "Campaign", "last": "Last change",
        "quiet": "Campaigns with no change in the last 14 / 30 days of the data",
        "quiet_counts": "Last change 30 or more days before the newest row: {n30} campaign(s). 14-29 days before it: {n14} campaign(s).",
        "quiet_scope": "Only campaigns with at least one change in this export are listed; a campaign not in the export may have had no change in its date range, or may not exist.",
        "days_before": "Days before newest row", "more": "{n} more not shown (see dashboard.html).",
        "coverage": "Coverage notes", "dq": "Data quality",
        "dq_line": "{inp} input row(s), {added} added by splitting bundled or multi-field changes, {left} left out ({why}), {rep} in the report. In the report with a limit: {other} categorized Other, {tz} without a time zone.",
        "unreadable_date": "unreadable date", "duplicate": "duplicate", "duplicate_across_files": "duplicate across files",
        "none": "none", "empty": "No change rows in this input.",
        "footer": "Counts from the given export only. Generated {at}.",
    },
    "tr": {
        "title": "Google Ads değişiklik özeti", "period": "Dönem", "sources": "Kaynak dosyalar", "source_types": "Kaynak türü", "rows": "satır",
        "totals": "Toplamlar", "metric": "Ölçü", "value": "Değer", "changes": "Değişiklik", "events": "Değişiklik olayı",
        "accounts": "Hesap", "humans": "Kişi", "automation": "Otomasyon", "campaigns": "Değişen kampanya",
        "ad_groups": "Değişen reklam grubu", "by_category": "Kategoriye göre değişiklikler", "category": "Kategori", "count": "Değişiklik",
        "share": "Pay", "by_user": "Kullanıcıya göre değişiklikler", "user": "Kullanıcı", "type": "Tür", "human": "kişi",
        "auto": "otomasyon", "masked": "Kullanıcı adları maskeli (--mask-users).", "by_account": "Hesaba göre değişiklikler",
        "account": "Hesap", "by_week": "ISO haftasına göre değişiklikler", "week": "Hafta", "days": "Günler",
        "top": "En çok değişiklik olan kampanyalar (sayıya göre ilk 10)", "campaign": "Kampanya", "last": "Son değişiklik",
        "quiet": "Verinin son 14 / 30 gününde değişikliği olmayan kampanyalar",
        "quiet_counts": "Son değişikliği en yeni satırdan 30 gün veya daha önce olan: {n30} kampanya. 14-29 gün önce olan: {n14} kampanya.",
        "quiet_scope": "Yalnız bu dışa aktarımda en az bir değişikliği olan kampanyalar listelenir. Dışa aktarımda olmayan bir kampanyada o tarih aralığında değişiklik olmamış olabilir ya da kampanya hiç olmayabilir.",
        "days_before": "En yeni satırdan önceki gün", "more": "{n} kampanya daha var, burada gösterilmedi (dashboard.html).",
        "coverage": "Kapsam notları", "dq": "Veri kalitesi",
        "dq_line": "{inp} girdi satırı, birden çok değişiklik içeren hücreler bölünerek {added} satır eklendi, {left} satır dışarıda ({why}), raporda {rep} satır. Raporda olup sınırlı olan: {other} satır Diğer kategorisinde, {tz} satırın saat dilimi yok.",
        "unreadable_date": "okunamayan tarih", "duplicate": "yinelenen", "duplicate_across_files": "dosyalar arası yinelenen",
        "none": "yok", "empty": "Bu girdide değişiklik satırı yok.",
        "footer": "Yalnız verilen dışa aktarımdan sayımlar. Oluşturulma: {at}.",
    },
}
# Turkish wording for the pipeline's coverage note ids; an unknown id keeps its English text.
_COVERAGE_NOTES_TR = {
    "editor_changes_absent": "API verisi Google Ads Editor ile yapılan değişikliklerin hepsini içermeyebilir.",
    "api_30_day_window": "API değişiklik geçmişi yalnız son 30 günü kapsar.",
    "auto_applied_may_be_missing": "Otomatik uygulanan öneriler bu veride eksik olabilir.",
    "ui_export_may_differ_from_api": "Arayüz dışa aktarımı ile API verisi aynı değişikliği farklı satırlarla gösterebilir.",
    "metrics_conversion_lag": "Dönüşümler gecikmeli raporlanır, değişiklik sonrasındaki son günlerin sayıları henüz tamamlanmamış olabilir.",
}


def _digest_lang(rows, meta, lang):
    if lang in ("tr", "en"):
        return lang
    labels = {r.get("source") for r in rows} | set(meta.get("source_labels") or []) | {s if isinstance(s, str) else (s or {}).get("source_label") for s in (meta.get("sources") or [])}
    if "legacy_summary_tr" in labels:
        return "tr"
    tr = en = 0
    for r in rows[:5000]:
        s = r.get("raw_summary") or ""
        tr += bool(_DIGEST_TR_RE.search(s))
        en += bool(_DIGEST_EN_RE.search(s))
    return "tr" if tr > en else "en"


def write_digest(out_dir, rows, aggregation, meta, lang):
    """Write digest.md: factual counts as Markdown tables (2026-11-14). lang is auto|tr|en;
    rows arrive already masked when --mask-users is on."""
    lang = _digest_lang(rows, meta, lang)
    t = _DIGEST_TEXT[lang]

    def num(n):
        return f"{n:,}".replace(",", "." if lang == "tr" else ",")

    def day(d):
        return f"{d[8:10]}.{d[5:7]}.{d[:4]}" if (d and lang == "tr") else (d or "-")

    def cell(v):
        s = "-" if v in (None, "") else str(v)
        return s.replace("\\", "\\\\").replace("|", "\\|").replace("\r", " ").replace("\n", " ").strip() or "-"

    def table(head, body):
        out = ["| " + " | ".join(cell(h) for h in head) + " |", "|" + "|".join("---" for _ in head) + "|"]
        out += ["| " + " | ".join(cell(c) for c in r) + " |" for r in body]
        return out + [""]

    total = len(rows)
    def pct(n):
        v = str(round(100 * n / total, 1)) if total else "-"
        return ("%" + v.replace(".", ",")) if lang == "tr" else v + "%"
    local_day = lambda r: (r.get("timestamp_iso") or r.get("timestamp_utc") or "")[:10]
    days = sorted(d for d in (local_day(r) for r in rows) if d)
    L = [f"# {t['title']}", ""]
    if days:
        L += [f"**{t['period']}:** {day(days[0])} - {day(days[-1])}", ""]
    srcs = meta.get("sources") or []
    if srcs and all(isinstance(s, dict) for s in srcs):
        files = [(s.get("file"), s.get("rows")) for s in srcs]
    else:
        fc = Counter(r.get("source_file") for r in rows if r.get("source_file"))
        files = list(fc.items())
    labels = meta.get("source_labels") or [x for x in srcs if isinstance(x, str)]
    if not files and labels:
        L += [f"**{t['source_types']}:** " + ", ".join(cell(x) for x in labels), ""]
    if files:
        L += [f"**{t['sources']}:** " + ", ".join(f"{cell(f)} ({num(n)} {t['rows']})" if n is not None else cell(f) for f, n in files), ""]
    if not rows:
        L += [t["empty"], ""]
    s = aggregation.get("summary", {})
    users = aggregation.get("users", {})
    L += [f"## {t['totals']}", ""] + table([t["metric"], t["value"]], [
        [t["changes"], num(total)], [t["events"], num(meta.get("event_count", 0))], [t["accounts"], num(s.get("active_accounts", 0))],
        [t["humans"], num(s.get("active_users", 0))], [t["automation"], num(len(users.get("automation", [])))],
        [t["campaigns"], num(s.get("changed_campaigns", 0))], [t["ad_groups"], num(s.get("changed_ad_groups", 0))]])
    if aggregation.get("categories"):
        L += [f"## {t['by_category']}", ""] + table([t["category"], t["count"], t["share"]],
                                                    [[c["category"], num(c["count"]), pct(c["count"])] for c in aggregation["categories"]])
    people = [[u["name"], t["human"], u["count"]] for u in users.get("human", [])] + [[u["name"], t["auto"], u["count"]] for u in users.get("automation", [])]
    if people:
        people.sort(key=lambda p: -p[2])
        L += [f"## {t['by_user']}", ""] + ([t["masked"], ""] if meta.get("masked_users") else [])
        L += table([t["user"], t["type"], t["count"]], [[n, ty, num(c)] for n, ty, c in people])
    if aggregation.get("accounts"):
        L += [f"## {t['by_account']}", ""] + table([t["account"], t["count"], t["campaigns"]],
                                                   [[a["account_name"], num(a["count"]), num(sum(1 for c in a["campaigns"] if c["campaign_name"] != "(no campaign)"))] for a in aggregation["accounts"]])
    weeks = Counter()
    week_days = defaultdict(set)
    for d in days:
        y, w, _ = date.fromisoformat(d).isocalendar()
        weeks[(y, w)] += 1
        week_days[(y, w)].add(d)
    if weeks:
        L += [f"## {t['by_week']}", ""] + table([t["week"], t["days"], t["count"]],
                                                [[f"{y}-W{w:02d}", f"{day(min(week_days[(y, w)]))} - {day(max(week_days[(y, w)]))}", num(weeks[(y, w)])] for y, w in sorted(weeks)])
    camp, camp_name, camp_acc, camp_last = Counter(), {}, {}, {}
    for r in rows:
        if r.get("campaign_name") or r.get("campaign_id"):
            k = (account_identity(r), campaign_identity(r))
            camp[k] += 1
            camp_name[k] = r.get("campaign_name") or f"Campaign {r.get('campaign_id')}"
            camp_acc[k] = r.get("account_name")
            camp_last[k] = max(camp_last.get(k, ""), local_day(r))
    if camp:
        top = sorted(camp.items(), key=lambda kv: (-kv[1], str(camp_name[kv[0]])))[:10]
        L += [f"## {t['top']}", ""] + table([t["campaign"], t["account"], t["count"], t["last"]],
                                            [[camp_name[k], camp_acc[k], num(n), day(camp_last[k])] for k, n in top])
    quiet = [u for u in aggregation.get("untouched", []) if u.get("days_since_last_change_at_generation", 0) >= 14]
    if aggregation.get("untouched"):
        n30 = sum(1 for u in quiet if u["days_since_last_change_at_generation"] >= 30)
        L += [f"## {t['quiet']}", "", t["quiet_counts"].format(n30=num(n30), n14=num(len(quiet) - n30)), "", t["quiet_scope"], ""]
        if quiet:
            L += table([t["campaign"], t["account"], t["last"], t["days_before"]],
                       [[u["campaign_name"], u.get("account_name"), day((u.get("last_change_ts") or "")[:10]), num(u["days_since_last_change_at_generation"])] for u in quiet[:25]])
            if len(quiet) > 25:
                L += [t["more"].format(n=num(len(quiet) - 25)), ""]
    notes = meta.get("coverage_notes") or []
    if notes:
        L += [f"## {t['coverage']}", ""] + [f"- {(_COVERAGE_NOTES_TR.get(n.get('id')) if lang == 'tr' else None) or n.get('text', '')}" for n in notes] + [""]
    dq = meta.get("data_quality") or {}
    if dq:
        left = dq.get("left_out", {})
        lim = dq.get("in_report_with_limits", {})
        why = ", ".join(f"{t.get(k, k.replace('_', ' '))}: {num(v)}" for k, v in left.items() if v) or t["none"]
        L += [f"## {t['dq']}", "", t["dq_line"].format(inp=num(dq.get("input_rows", total)), rep=num(dq.get("rows_in_report", total)),
                                                      left=num(sum(left.values())), why=why, added=num(dq.get("rows_added_by_field_split", 0) + dq.get("rows_added_by_statement_split", 0)), other=num(lim.get("category_other", 0)),
                                                      tz=num(lim.get("timezone_unknown", 0))), ""]
        L += [f"- {w}" for w in dq.get("warnings", [])] + ([""] if dq.get("warnings") else [])
    L += [f"_{t['footer'].format(at=str(meta.get('generated_at', ''))[:16].replace('T', ' '))}_", ""]
    text = "\n".join(L)
    if meta.get("masked_users"):
        text = EMAIL_RE.sub("[e-mail]", text)
    Path(out_dir, "digest.md").write_text(text, encoding="utf-8")
    return lang


def run_pipeline(input_path, out_dir, mapping_file=None, date_format=None, decimal_style=None,
                  tz="unknown", mask_users=False, allow_unknown_categories=False, force_review=False,
                  extra_rules=None, open_browser=False, generated_at=None, account_name=None, currency=None, column_map=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    blocked = _prepare_out_dir(out_dir)
    if blocked:
        return blocked
    if _is_empty_event_list(input_path):
        dq = {"input_rows": 0, "rows_added_by_field_split": 0, "left_out": {"unreadable_date": 0, "duplicate": 0}, "rows_in_report": 0,
              "in_report_with_limits": {}, "ignored_headers": []}
        (out_dir / "changes.jsonl").write_text("", encoding="utf-8")
        (out_dir / "coverage.txt").write_text("Input rows: 0\nThe input is an empty event list: no change in the requested range.\n", encoding="utf-8")
        build_dashboard([], out_dir / "dashboard.html", mask_users=mask_users, generated_at=generated_at, json_out=out_dir / "change_history.json", data_quality=dq)
        res = {"status": "ok", "out": str(out_dir / "dashboard.html"), "row_count": 0, "empty": True,
               "message": "The input holds zero change events (nothing changed in the requested range, or the range is outside the API's 30 days).",
               "data_quality": dq}
        if open_browser:
            res["opened"] = _open_dashboard(out_dir / "dashboard.html")
        return res

    if currency is not None and not CURRENCY_CODE_RE.match(currency):
        return {"status": "error", "message": f"Invalid --currency value: {currency!r}. Expected a 3-letter ISO 4217 code in capitals, e.g. 'TRY', 'USD', 'EUR'."}

    if extra_rules is not None:
        err = validate_extra_rules(extra_rules)
        if err:
            return {"status": "error", "message": err}

    v = validate_source(input_path, mapping_file=mapping_file, force=force_review, default_account_name=account_name, column_map=column_map)
    if v["status"] != "ok":
        return v
    profile = v.pop("_profile", None)  # saved only once the run is ok (2026-11-10, B-8)
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
    #   input_rows + rows_added_by_field_split + rows_added_by_statement_split - left_out = rows_in_report
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
    data_quality.update(semantic_data_quality(n, c["rows"], c["other_pct"]))
    with open(out_dir / "coverage.txt", "w", encoding="utf-8") as f:
        dq_left = data_quality["left_out"]
        f.write(f"Input rows: {data_quality['input_rows']}\n")
        if data_quality["rows_added_by_field_split"]:
            f.write(f"Rows added by splitting multi-field events: {data_quality['rows_added_by_field_split']}\n")
        if data_quality["rows_added_by_statement_split"]:
            f.write(f"Rows added by splitting bundled summary cells (1 row = 1 change statement): {data_quality['rows_added_by_statement_split']}\n")
        if data_quality["possible_duplicate_rows_kept"]:
            f.write(f"Identical rows kept (possible duplicates, nothing tells them apart): {data_quality['possible_duplicate_rows_kept']}\n")
        for w in data_quality["warnings"]:
            f.write(f"WARNING: {w}\n")
        f.write(f"Left out: {sum(dq_left.values())} (unreadable date: {dq_left['unreadable_date']}, duplicate: {dq_left['duplicate']})\n")
        f.write(f"In the report: {data_quality['rows_in_report']}\n")
        f.write("In the report, with a limit: " + ", ".join(f"{k.replace('_', ' ')}: {val}" for k, val in data_quality["in_report_with_limits"].items()) + "\n")
        f.write(f"Source columns not used: {', '.join(data_quality['ignored_headers']) or 'none'}\n\n")
        f.write(f"Total rows: {len(c['rows'])}\n")
        if duplicate_count:
            f.write(f"Duplicates removed (same source event id, identical content): {duplicate_count}\n")
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

    source_warnings = v.get("warnings", []) + save_mapping_profile(profile)
    data = build_dashboard(c["rows"], out_dir / "dashboard.html", mask_users=mask_users, generated_at=generated_at,
                            json_out=out_dir / "change_history.json", rows_skipped_unparseable_date=n["rows_skipped_unparseable_date"],
                            data_quality=data_quality)
    write_digest(out_dir, c["rows"], data, data["meta"], DIGEST_LANG)

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
    result["source_warnings"] = source_warnings
    if open_browser:
        result["opened"] = _open_dashboard(out_dir / "dashboard.html")
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


# Round 9 dashboard checks (2026-11-10): drill-down, search fold, ranges,
# rule-filter reset, thresholds, units, dates, Python/JS status-rule twins.
_JS_R9_DASHBOARD_SUITE = r"""
const assert = require('assert');
let passed = 0, failures = 0;
function check(desc, fn){ try { fn(); passed++; } catch(e){ failures++; console.error('FAIL: ' + desc + '\n  ' + e.message); } }
function row(over){ return Object.assign({ subcategory:null, resource_type:null, operation:null, old_value_num:null, new_value_num:null, old_value:null, new_value:null, raw_summary:'' }, over); }
function ids(){ return getFiltered().map(c=>c.change_id); }
check('fold is Turkish-aware and accent-insensitive', ()=>{
  assert.strictEqual(fold('İSTANBUL Şube Işık ğ'), 'istanbul sube isik g');
  assert.strictEqual(fold('ISTANBUL'), fold('istanbul'));
});
check('search reads raw_summary and folds both sides', ()=>{
  DASH_DATA = { changes:[ row({change_id:'a', raw_summary:'Kampanya değiştirildi\n  Hedef EBM'}), row({change_id:'b', raw_summary:'Bütçe'}) ], meta:{} };
  resetAll(); state.search = 'DEGISTIRILDI'; assert.deepStrictEqual(ids(), ['a']);
  state.search = 'hedef ebm'; assert.deepStrictEqual(ids(), ['a']); resetAll();
});
check('drill-down filters by account+campaign identity, not a search; same click clears', ()=>{
  DASH_DATA = { changes:[ row({change_id:'1', account_name:'A', campaign_name:'Kasko'}), row({change_id:'2', account_name:'A', campaign_name:'Kasko Plus'}),
    row({change_id:'3', account_name:'B', campaign_name:'Kasko'}), row({change_id:'4', account_name:'A'}) ], meta:{} };
  resetAll(); drillCampaign('A', 'Kasko');
  assert.deepStrictEqual(ids(), ['1']); assert.strictEqual(state.search, '');
  drillCampaign('A', 'Kasko'); assert.strictEqual(ids().length, 4);
  drillCampaign('A', NONE); assert.deepStrictEqual(ids(), ['4']); resetAll();
});
check('Unknown actors are their own group and filterable', ()=>{
  DASH_DATA = { changes:[ row({change_id:'h', actor_type:'human', user_name:'U'}), row({change_id:'u', actor_type:'unknown'}), row({change_id:'x', actor_type:'automation', client_type:'GOOGLE_ADS_SCRIPTS'}) ], meta:{} };
  resetAll(); toggleOnly('users', actorKey(DASH_DATA.changes[1])); assert.deepStrictEqual(ids(), ['u']);
  toggleOnly('users', 'a:GOOGLE_ADS_SCRIPTS'); assert.deepStrictEqual(ids(), ['x']); resetAll();
});
check('7d range compares timestamp_utc against the newest row (or today)', ()=>{
  DASH_DATA = { changes:[ row({change_id:'new', timestamp_utc:'2026-08-20T23:30:00+00:00', timestamp_iso:'2026-08-21T02:30:00'}),
    row({change_id:'in', timestamp_utc:'2026-08-14T00:00:00+00:00', timestamp_iso:'2026-08-14T03:00:00'}),
    row({change_id:'out', timestamp_utc:'2026-08-13T23:00:00+00:00', timestamp_iso:'2026-08-14T02:00:00'}) ], meta:{} };
  resetAll(); state.range = '7'; assert.deepStrictEqual(ids(), ['new','in']);
  state.anchor = 'today'; assert.ok(Date.now() > Date.parse('2026-09-01') ? ids().length === 0 : true); resetAll();
});
check('turning highlighting off clears the rule filter', ()=>{
  state.ruleMatch = '__matched__'; ruleState.enabled = true; setRuleHighlighting(false);
  assert.strictEqual(state.ruleMatch, ''); assert.strictEqual(ruleState.enabled, false);
});
check('threshold validation rejects empty/0/negative/non-numeric/over max', ()=>{
  for(const bad of ['', '  ', 'abc', '0', '-5', String(PCT_MAX+1)]) assert.strictEqual(parseThreshold(bad).ok, false, bad);
  assert.strictEqual(parseThreshold('30').value, 30); assert.strictEqual(parseThreshold('12,5').value, 12.5);
});
check('Clear filters resets rule state and sort too', ()=>{
  ruleState.enabled = true; ruleState.pct.budget_magnitude = 5; ruleState.stateOn.campaign_paused = false; state.sortKey = 'source'; state.sortDir = 1;
  resetAll();
  assert.deepStrictEqual([ruleState.enabled, ruleState.pct.budget_magnitude, ruleState.stateOn.campaign_paused, state.sortKey, state.sortDir], [false, 50, true, 'timestamp_iso', -1]);
});
check('legacy "Durum Değişti" campaign pause matches, fact read from the text', ()=>{
  const m = computeRuleMatches(row({subcategory:'Paused', resource_type:'CAMPAIGN', raw_summary:'Durum Değişti: ENABLED -> PAUSED'})).find(x=>x.id==='campaign_paused');
  assert.ok(m && m.detail.includes('ENABLED → PAUSED'), m && m.detail);
  assert.ok(computeRuleMatches(row({subcategory:'Paused', resource_type:'Kampanya'})).some(x=>x.id==='campaign_paused'));
  assert.ok(!computeRuleMatches(row({subcategory:'Paused', raw_summary:'Kampanya öğesi değiştirildi'})).some(x=>x.id==='campaign_paused'));
});
check('Target CPA / ROAS magnitude rules fire on their subcategories', ()=>{
  resetAll(); ruleState.pct.target_cpa_magnitude = 10; ruleState.pct.target_roas_magnitude = 10;
  const a = computeRuleMatches(row({subcategory:'Target CPA', old_value_num:350.34, new_value_num:402.89})).find(x=>x.id==='target_cpa_magnitude');
  assert.ok(a && a.detail.startsWith('+15%'), a && a.detail);
  assert.ok(computeRuleMatches(row({subcategory:'Target ROAS', old_value_num:400, new_value_num:300})).some(x=>x.id==='target_roas_magnitude'));
  resetAll();
});
check('units: %, parsed currency code, micros conversion', ()=>{
  assert.strictEqual(valueText(row({new_value:'+10', value_unit:'%'}), 'new'), '+10%');
  assert.strictEqual(valueText(row({new_value:'402,89', new_value_display:402.89, value_unit:'TRY', currency:'TRY'}), 'new'), '402,89 TRY');
  assert.strictEqual(valueText(row({new_value:'50000000', new_value_display:50, value_unit:'TRY', currency:'TRY'}), 'new'), '50 TRY (raw: 50000000)');
  assert.strictEqual(valueText(row({new_value:'₺402,89', new_value_display:402.89, value_unit:'TRY', currency:'TRY'}), 'new'), '₺402,89');
  assert.strictEqual(valueText(row({}), 'new'), '');
});
check('dates: dd.mm.yyyy hh:mm in tr-TR, source wall clock', ()=>{
  UI_LOCALE = 'tr-TR'; assert.strictEqual(fmtWall('2026-08-11T02:33:50', true), '11.08.2026 02:33'); UI_LOCALE = undefined;
});
check('status-rule twins agree with the Python summary rules', ()=>{
  for(const t of TWINS){
    const r = row({subcategory:t.subcategory, raw_summary:t.text});
    ruleState.stateOn.campaign_enabled = true;
    const got = computeRuleMatches(r).some(x=>x.id==='campaign_paused' || x.id==='campaign_enabled');
    assert.strictEqual(got, t.campaign_level, JSON.stringify(t));
  }
  resetAll();
});
if (failures > 0) { console.error(failures + ' of ' + (passed + failures) + ' round-9 dashboard JS assertions failed'); process.exit(1); }
console.log(passed + ' round-9 dashboard JS assertions passed');
"""


def _run_r9_dashboard_cases(td):
    """Round 9 (2026-11-10) dashboard fixes: static template checks plus a Node suite."""
    import shutil
    import subprocess
    with _test_case('r9 dashboard: no good/bad value colours, labelled filters, dialog semantics, wrapped tables'):
        t = DASHBOARD_TEMPLATE
        assert "var(--bad)" not in t and "var(--good)" not in t, "Old/New values must not be coloured red/green"
        assert "<select multiple" not in t.split("<body")[1] and 'details class="ms"' in t, "multi-select must be the checkbox popover"
        assert t.count('class="fld-l"') >= 11, "every filter needs a visible label"
        assert 'role="dialog"' in t and 'aria-modal="true"' in t and "closeDetail" in t
        assert t.count('class="tbl-wrap"') >= 3 and "@media print" in t
        print("[PASS] r9 dashboard: neutral values, labelled filters, dialog, wrapped tables")
    if not shutil.which("node"):
        print("[SKIP] r9 dashboard JS suite (Node not found on PATH)")
        return
    with _test_case('r9 dashboard JS suite (Node), incl. Python/JS status-rule twins'):
        twins = []
        for text in ["1 campaign paused in ad group X", "2 campaigns paused", "2 ad groups paused", "3 campaigns active",
                     "1 ad group active", "5 broad match keywords paused", "2 kampanya duraklatıldı", "1 kampanya etkinleştirildi",
                     "Durum Değişti: ENABLED -> PAUSED"]:
            rule = next((r for r in CATEGORY_RULES["summary_text_rules"] if re.search(r["pattern"], text)), None)
            if not rule or rule["subcategory"] not in ("Paused", "Enabled"):
                continue
            level = bool(re.search(r"campaign|kampanya", rule["pattern"], re.I)) and not re.search(r"ad group|keyword|kelime|reklam", rule["pattern"], re.I)
            twins.append({"text": text, "subcategory": rule["subcategory"], "campaign_level": level})
        assert len(twins) >= 6, f"twin samples no longer reach Python's status rules: {twins}"
        body = re.search(r"<script>(.*?)</script>", DASHBOARD_TEMPLATE, re.S).group(1)
        body = body.replace("const DASH_DATA = /*__DASH_DATA__*/null;", "let DASH_DATA = null;", 1)
        harness = ("global.document = { addEventListener: function(){} };\n" + body + "\nconst TWINS = "
                   + json.dumps(twins, ensure_ascii=False) + ";\n" + _JS_R9_DASHBOARD_SUITE)
        proc = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=15)
        assert proc.returncode == 0, f"r9 dashboard JS suite failed:\n{proc.stdout}\n{proc.stderr}"
        print(f"[PASS] r9 dashboard JS suite (Node): {proc.stdout.strip()}")


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


def _run_r9_ingest_cases(td):
    """2026-11-10 audit round 7 (INGEST): preambles, dates, encodings, mapping, profiles. Synthetic data only."""
    global PROFILES_DIR, INPUT_ENCODING, CSV_FIELD_SIZE_LIMIT
    tr_native = ('Değişiklik geçmişi raporu\n1 Ocak 2026 - 19 Ağustos 2026\n'
                 'Tarih ve saat,Kullanıcı,Kampanya,Reklam grubu,Değişiklik\n'
                 '11 Ağu 2026 02:33:50,user.a@example.test,Kampanya A,,"Kampanya değiştirildi\n  Durum: Etkin -> Duraklatıldı"\n'
                 '4 Ağu 2026 16:16:32,user.b@example.test,Kampanya B,Grup 1,"Anahtar kelime eklendi\n  örnek kelime"\n')
    with _test_case("r9 ingest: TR native export with a title preamble reaches ok with only --account-name/--timezone"):
        f = td / "r9_tr_native.csv"
        f.write_text(tr_native, encoding="utf-8")
        v = validate_source(f, default_account_name="Account A", save_profile=False)
        assert v["status"] == "ok", v
        assert v["mapping"].get("timestamp") == "Tarih ve saat" and v["mapping"].get("raw_summary") == "Değişiklik", v["mapping"]
        assert "operation" not in v["mapping"], "multi-line free text must not map to operation"
        assert any("preamble" in w for w in v["warnings"]), v["warnings"]
        r = run_pipeline(f, td / "r9_out_tr", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok" and r["date_format_used"] == "TEXT_MONTH" and r["rows_skipped_unparseable_date"] == 0, r
        r_map = run_pipeline(f, td / "r9_out_tr_map", account_name="Account A", tz="Europe/Istanbul", column_map={"raw_summary": "Değişiklik"}, generated_at="2026-08-17T00:00:00")
        assert r_map["status"] == "ok", f"--map on a preamble file must see the real header row: {r_map}"
        print("[PASS] r9 ingest: TR native export with a title preamble reaches ok with only --account-name/--timezone")

    with _test_case("r9 ingest: EN native export with preamble, CRLF/LF mix and a BOM inside a quoted cell"):
        en = ('﻿Change history report\r\nJan 1, 2026 - Aug 19, 2026\n'
              'Date & time,User,Campaign,Ad group,Changes\r\n'
              '"Aug 16, 2026, 10:42:26 PM",user.a@example.test,Campaign A,,"﻿Campaign paused\r\n  Status: Enabled -> Paused"\n'
              '"Aug 15, 2026, 9:01:00 AM",user.b@example.test,Campaign B,,Budget changed\r\n')
        f = td / "r9_en_native.csv"
        f.write_text(en, encoding="utf-8")
        headers, rows = read_rows(f)
        assert headers == ["Date & time", "User", "Campaign", "Ad group", "Changes"] and len(rows) == 2, (headers, len(rows))
        assert not rows[0]["Changes"].startswith("﻿"), "a BOM inside a cell must be stripped"
        r = run_pipeline(f, td / "r9_out_en", account_name="Account A", tz="UTC", generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok" and r["rows_skipped_unparseable_date"] == 0, r
        print("[PASS] r9 ingest: EN native export with preamble, CRLF/LF mix and a BOM inside a quoted cell")

    with _test_case("r9 ingest: spelled-out months in tr/en/de/es/fr/pt/it/nl, 12h clock, bidi marks, never DD/MM-ambiguous"):
        cases = {
            "11 Ağu 2026 02:33:50": datetime(2026, 8, 11, 2, 33, 50), "3 EKİM 2026 14:00": datetime(2026, 10, 3, 14, 0),
            "1 Şubat 2026": datetime(2026, 2, 1), "‎5 Kas 2026 10:00:00‏": datetime(2026, 11, 5, 10, 0),
            "Aug 16, 2026, 10:42:26 PM": datetime(2026, 8, 16, 22, 42, 26), "16 Aug 2026, 12:05 AM": datetime(2026, 8, 16, 0, 5),
            "16. Aug. 2026, 22:42:26": datetime(2026, 8, 16, 22, 42, 26), "3. März 2026 08:00": datetime(2026, 3, 3, 8, 0),
            "16 de ago. de 2026 22:42": datetime(2026, 8, 16, 22, 42), "2 dic 2026, 1:00 p. m.": datetime(2026, 12, 2, 13, 0),
            "16 août 2026 à 22:42": datetime(2026, 8, 16, 22, 42), "5 févr. 2026 07:00": datetime(2026, 2, 5, 7, 0),
            "16 de set. de 2026 22:42": datetime(2026, 9, 16, 22, 42), "7 out 2026 09:00": datetime(2026, 10, 7, 9, 0),
            "16 ago 2026 22:42": datetime(2026, 8, 16, 22, 42), "4 mag 2026 10:00": datetime(2026, 5, 4, 10, 0),
            "16 mrt. 2026 22:42": datetime(2026, 3, 16, 22, 42), "1 mei 2026 06:00": datetime(2026, 5, 1, 6, 0),
        }
        for raw, want in cases.items():
            assert detect_date_format([raw]) == "TEXT_MONTH" or detect_date_format([raw]) == "UI_EN", raw
            got = parse_timestamp(raw, detect_date_format([raw]))
            assert got == want, f"{raw!r}: {got} != {want}"
        for bad in ("11 Ağu 2026 JUNK", "Foo 3 2026", "13 Ağu 26"):
            try:
                parse_text_month_timestamp(bad)
                raise AssertionError(f"must not parse {bad!r}")
            except ValueError:
                pass
        q = date_format_question(["garbage", "more garbage"])
        assert q["message"].startswith("unrecognised date shape: garbage"), q
        assert "DD/MM" in date_format_question(["03/04/2026 10:00"])["message"]
        print("[PASS] r9 ingest: spelled-out months in tr/en/de/es/fr/pt/it/nl, 12h clock, bidi marks, never DD/MM-ambiguous")

    with _test_case("r9 ingest: Python 3.9 fromisoformat gaps and DMY with comma + AM/PM"):
        assert parse_timestamp("2026-08-01 10:00:00,123+0300", "ISO") == datetime(2026, 8, 1, 10, 0, 0, 123000, tzinfo=dt_timezone(timedelta(hours=3)))
        assert parse_timestamp("2026-8-1 9:05:00", "ISO") == datetime(2026, 8, 1, 9, 5)
        assert parse_timestamp("2026-08-01T10:00:00.1Z", "ISO").utcoffset() == timedelta(0)
        assert parse_timestamp("01/08/2026, 10:00:00 PM", "DMY") == datetime(2026, 8, 1, 22, 0)
        assert detect_date_format(["2026-8-1 9:05:00"]) == "ISO"
        print("[PASS] r9 ingest: Python 3.9 fromisoformat gaps and DMY with comma + AM/PM")

    with _test_case("r9 ingest: UTF-16, ';' and tab delimiters, NUL bytes, cp1254 ok, cp1251 rejected unless --encoding"):
        body = "Tarih,Hesap Adı,Kullanıcı,Değişiklik Özeti\n2026-08-01 10:00:00,Hesap A,Kullanıcı Ş,Bütçe değişti\n"
        for name, data in (("u16.csv", body.encode("utf-16")), ("semi.csv", body.replace(",", ";").encode("utf-8")),
                           ("tab.csv", body.replace(",", "\t").encode("utf-8")), ("nul.csv", body.replace("Hesap A", "Hesap\x00 A").encode("utf-8")),
                           ("c1254.csv", body.encode("cp1254"))):
            f = td / f"r9_{name}"
            f.write_bytes(data)
            headers, rows = read_rows(f)
            assert headers == ["Tarih", "Hesap Adı", "Kullanıcı", "Değişiklik Özeti"] and rows[0]["Hesap Adı"] == "Hesap A", (name, headers, rows)
        f = td / "r9_c1251.csv"
        f.write_bytes("Дата,Аккаунт,Изменения\n2026-08-01 10:00:00,Аккаунт А,Кампания приостановлена\n".encode("cp1251"))
        try:
            read_rows(f)
            raise AssertionError("cp1251 text must not be silently read as cp1254 mojibake")
        except ValueError as e:
            assert "--encoding" in str(e), e
        INPUT_ENCODING = "cp1251"
        try:
            assert read_rows(f)[0][0] == "Дата"
        finally:
            INPUT_ENCODING = None
        print("[PASS] r9 ingest: UTF-16, ';' and tab delimiters, NUL bytes, cp1254 ok, cp1251 rejected unless --encoding")

    with _test_case("r9 ingest: xlsx/gzip magic bytes, oversized field and bad --mapping-file give structured errors"):
        for name, data in (("x.csv", b"PK\x03\x04rest"), ("g.csv", b"\x1f\x8b\x08rest")):
            f = td / f"r9_{name}"
            f.write_bytes(data)
            v = validate_source(f, save_profile=False)
            assert v["status"] == "error" and "CSV" in v["message"], v
        f = td / "r9_big.csv"
        f.write_text("Tarih,Değişiklik Özeti\n2026-08-01 10:00:00," + "x" * 5000 + "\n", encoding="utf-8")
        saved_limit = CSV_FIELD_SIZE_LIMIT
        CSV_FIELD_SIZE_LIMIT = 1000
        try:
            v = validate_source(f, save_profile=False)
            assert v["status"] == "error" and "larger than" in v["message"], v
        finally:
            CSV_FIELD_SIZE_LIMIT = saved_limit
            csv.field_size_limit(saved_limit)
        fm = td / "r9_list_map.json"
        fm.write_text("[1, 2]", encoding="utf-8")
        v = validate_source(f, mapping_file=fm, save_profile=False)
        assert v["status"] == "error" and "JSON object" in v["message"], v
        fm.write_text('{"mapping": {"timestamp": "Tarih", "raw_summary": "Değişiklik Özeti", "account_name": "\\ud800"}}', encoding="utf-8")
        v = validate_source(f, mapping_file=fm, save_profile=False)
        assert v["status"] in ("ok", "needs_mapping", "needs_review", "error"), v
        json.dumps(v, ensure_ascii=False).encode("utf-8")
        fj = td / "r9_surrogate.json"
        fj.write_text('[{"change_date_time": "2026-08-01 10:00:00", "customer_id": "1", "user_email": "a@example.test", "change_resource_type": "CAMPAIGN", '
                      '"changed_fields": "name", "old_resource": "A\\udc80", "new_resource": "B"}]', encoding="utf-8")
        r = run_pipeline(fj, td / "r9_out_sur", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok", r
        print("[PASS] r9 ingest: xlsx/gzip magic bytes, oversized field and bad --mapping-file give structured errors")

    with _test_case("r9 ingest: --map drops other fields on the same header; alias collisions warn; needs_mapping wording"):
        f = td / "r9_change_col.csv"
        f.write_text("Date,Client,Change,Item type,Level,Summary\n2026-08-01 10:00:00,Account A,Created,Campaign,Campaign,x\n2026-08-02 10:00:00,Account A,Removed,Ad,Ad,y\n", encoding="utf-8")
        v = validate_source(f, save_profile=False)
        assert v["mapping"].get("operation") == "Change" and v["mapping"].get("account_name") == "Client", v["mapping"]
        assert any("Client" in w for w in v["warnings"]) and any("'Item type' and 'Level'" in w for w in v["warnings"]), v["warnings"]
        v2 = validate_source(f, column_map={"raw_summary": "Change"}, save_profile=False)
        assert "operation" not in v2["mapping"] and v2["mapping"]["raw_summary"] == "Change", v2["mapping"]
        f2 = td / "r9_noacct.csv"
        f2.write_text("Date,Changes\n2026-08-01 10:00:00,x\n", encoding="utf-8")
        nm = validate_source(f2, save_profile=False)
        assert nm["status"] == "needs_mapping" and "can't proceed" not in nm["instructions"] and "--account-name" in nm["instructions"], nm
        assert not any(m == "resource_type" for m in nm["missing_required"]), nm["missing_required"]
        f3 = td / "r9_dupe.csv"
        f3.write_text("Junk title\nDate,Account,Changes,Changes\n2026-08-01 10:00:00,A,x,y\n", encoding="utf-8")
        v3 = validate_source(f3, save_profile=False)
        assert any("Duplicate column header" in w and "changes" in w for w in v3["warnings"]), v3["warnings"]
        print("[PASS] r9 ingest: --map drops other fields on the same header; alias collisions warn; needs_mapping wording")

    with _test_case("r9 ingest: profiles use a user data dir, carry a schema version, are saved only after ok, never crash"):
        import os
        saved_env = os.environ.get("XDG_DATA_HOME")
        os.environ["XDG_DATA_HOME"] = str(td / "xdg")
        try:
            assert _default_state_dir() == td / "xdg" / "google-ads-change-history-dashboard" / "mapping-profiles"
        finally:
            if saved_env is None:
                os.environ.pop("XDG_DATA_HOME", None)
            else:
                os.environ["XDG_DATA_HOME"] = saved_env
        saved_dir = PROFILES_DIR
        PROFILES_DIR = td / "r9_profiles"
        try:
            f = td / "r9_prof.csv"
            f.write_text("When,Who,Acct,What\n03/04/2026 10:00,user.a@example.test,Account A,Budget changed\n", encoding="utf-8")
            ans = {"timestamp": "When", "user_email": "Who", "account_name": "Acct", "raw_summary": "What"}
            r = run_pipeline(f, td / "r9_out_prof1", column_map=ans, generated_at="2026-08-17T00:00:00")
            assert r["status"] == "needs_date_format" and not list(PROFILES_DIR.glob("*.json")), "a run that did not reach ok must not cache a profile"
            r = run_pipeline(f, td / "r9_out_prof2", column_map=ans, date_format="DMY", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
            assert r["status"] == "ok", r
            saved = [json.loads(p.read_text(encoding="utf-8")) for p in PROFILES_DIR.glob("*.json")]
            assert len(saved) == 1 and saved[0]["schema_version"] == PROFILE_SCHEMA_VERSION, saved
            assert validate_source(f, save_profile=False)["status"] == "ok", "a current-schema profile must be reused"
            for p in PROFILES_DIR.glob("*.json"):
                p.write_text(json.dumps({k: v for k, v in saved[0].items() if k != "schema_version"}), encoding="utf-8")
            assert validate_source(f, save_profile=False)["status"] == "needs_mapping", "a profile without the schema stamp must be ignored"
            blocker = td / "r9_blocker"
            blocker.write_text("not a dir", encoding="utf-8")
            PROFILES_DIR = blocker / "profiles"
            r = run_pipeline(f, td / "r9_out_prof3", column_map=ans, date_format="DMY", allow_unknown_categories=True, generated_at="2026-08-17T00:00:00")
            assert r["status"] == "ok" and any("Could not save" in w for w in r["source_warnings"]), r.get("source_warnings")
        finally:
            PROFILES_DIR = saved_dir
        print("[PASS] r9 ingest: profiles use a user data dir, carry a schema version, are saved only after ok, never crash")


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


def _r9_csv(td, name, rows, header="Date & time,User,Campaign,Ad group,Changes"):
    import csv as _csv
    p = td / name
    with open(p, "w", encoding="utf-8", newline="") as f:
        w = _csv.writer(f)
        w.writerow(header.split(","))
        w.writerows(rows)
    return p


def _r9_rows(out):
    with open(out / "changes.jsonl", encoding="utf-8") as fh:
        return [json.loads(l) for l in fh]


def _run_r9_semantics_cases(td):
    """Round 9 (2026-11-10), SEMANTICS area: dedupe, statement split,
    per-statement categories/operations/values, masking, timeline basis."""
    import time as _time
    ts = "2026-08-04 16:16:32"
    with _test_case("D-1: distinct rows sharing campaign+second are never dropped; identical rows in one file are kept and counted"):
        p = _r9_csv(td, "r9_dedupe.csv", [
            [ts, "a@example.test", "Campaign Alpha", "Ad Group 1", "1 dil eklendi\n  Dil A"],
            [ts, "a@example.test", "Campaign Alpha", "Ad Group 2", "1 dil eklendi\n  Dil A"],
            [ts, "a@example.test", "Campaign Alpha", "Ad Group 2", "1 ülke eklendi\n  Ülke A"],
            [ts, "a@example.test", "Campaign Alpha", "Ad Group 2", "1 ülke eklendi\n  Ülke A"],
        ])
        r = run_pipeline(p, td / "out_r9_dedupe", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok" and r["row_count"] == 4, f"no row may be dropped: {r.get('row_count')} {r.get('status')}"
        dq = r["data_quality"]
        assert dq["left_out"]["duplicate"] == 0 and dq["possible_duplicate_rows_kept"] == 1, f"dq: {dq}"
        assert len({x["change_id"] for x in _r9_rows(td / "out_r9_dedupe")}) == 4, "change_id must stay unique"
        print("[PASS] D-1: distinct rows sharing campaign+second are never dropped; identical rows in one file are kept and counted")

    with _test_case("D-3: a bundled cell becomes one row per statement, sharing event_id; each statement categorized on its own"):
        cell = ("1 sıralı eşlemeli anahtar kelime eklendi\n  \"anahtar a\"\n"
                "2 negatif sıralı eşlemeli anahtar kelimeler eklendi\n  -\"x\"\n  -\"y\"\n"
                "Kampanya değiştirildi\n  ₺208,20 olan Hedef EBM, ₺180,00 değerine düşürüldü\n"
                "1 teklif ayarlama değişikliği\n  Tabletler: Platform teklif ayarlaması %-50 olarak ayarlandı")
        p = _r9_csv(td, "r9_split.csv", [[ts, "a@example.test", "Campaign Alpha", "", cell]])
        r = run_pipeline(p, td / "out_r9_split", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok" and r["row_count"] == 4, f"expected 4 statement rows: {r}"
        assert r["data_quality"]["rows_added_by_statement_split"] == 3
        rows = sorted(_r9_rows(td / "out_r9_split"), key=lambda x: x["statement_index"])
        assert len({x["event_id"] for x in rows}) == 1 and [x["statement_count"] for x in rows] == [4] * 4
        got = [(x["category"], x["subcategory"], x["operation"]) for x in rows]
        assert got == [("Keyword", "Keyword added", "CREATE"), ("Keyword", "Negative keyword", "CREATE"),
                       ("Bidding", "Target CPA", "UPDATE"), ("Bidding", "Bid modifier changed", "UPDATE")], got
        assert (rows[2]["old_value_num"], rows[2]["new_value_num"], rows[2]["currency"]) == (208.2, 180.0, "TRY"), rows[2]
        assert (rows[3]["new_value_num"], rows[3]["value_unit"]) == (-50.0, "%"), rows[3]
        assert rows[0]["value_confidence"] is None, "no values -> value_confidence None"
        print("[PASS] D-3: a bundled cell becomes one row per statement, sharing event_id; each statement categorized on its own")

    with _test_case("statement categories use headline and detail lines (TR/EN, legacy, case-insensitive)"):
        cases = [
            ("Kampanya değiştirildi\n  Hedef ROAS: %300", "Bidding", "Target ROAS"),
            ("Kampanya değiştirildi\n  \"A\" olan Kampanyateklif stratejisi türü değeri, \"B\" olarak değiştirildi", "Bidding", "Strategy changed"),
            ("Kampanya değiştirildi\n  Ad: \"Yeni\"", "Campaign", "Campaign settings changed"),
            ("1 teklif ayarlama değişikliği\n  Cuma 12:00 - 16:00: Reklam zaman planlaması teklif ayarlaması %+10 olarak ayarlandı", "Bidding", "Bid modifier changed"),
            ("Kampanya öğesi değiştirildi\n  Etkin olan Durum, Kaldırıldı olarak değiştirildi", "Asset", "Asset removed"),
            ("Kampanya öğesi değiştirildi\n  Etkin olan Durum, Duraklatıldı olarak değiştirildi", "Status", "Paused"),
            ("Kampanya öğesi değiştirildi\n  Kaldırıldı olan Durum, Etkin olarak değiştirildi", "Status", "Enabled"),
            ("1 duyarlı arama ağı reklamı değiştirildi\n  etkin olan Durum, duraklatıldı olarak değiştirildi", "Status", "Paused"),
            ("Öğe oluşturuldu\n  Durum: \"Etkin\"", "Asset", "Asset added"),
            ("\"Hedef A\" adlı kampanya özel hedefi eklendi", "Conversion", "Conversion tracking changed"),
            ("Her zaman Hesap varsayılan hedefleri kullanıldı", "Conversion", "Conversion tracking changed"),
            ("2 diller eklendi", "Targeting", "Language targeting changed"),
            ("3 şehirler eklendi", "Targeting", "Location targeting changed"),
            ("2 platformlar eklendi", "Targeting", "Device targeting changed"),
            ("6 yaş aralıkları eklendi", "Audience", "Audience added"),
            ("1 negatif yaş grubu eklendi", "Audience", "Audience added"),
            ("2 negatif geniş eşlemeli anahtar kelime eklendi", "Keyword", "Negative keyword"),
            ("1 geniş eşlemeli anahtar kelime kaldırıldı", "Keyword", "Keyword removed"),
            ("Bütçe \"Bütçe A\" olarak ayarlandı", "Budget", "Budget changed"),
            ("Durum Değişti: ENABLED -> PAUSED", "Status", "Paused"),
            ("Durum Değişti: PAUSED -> ENABLED", "Status", "Enabled"),
            ("3 Campaigns Paused", "Status", "Paused"),
            ("2 Kampanya  duraklatıldı", "Status", "Paused"),
            ("Campaign changed\n  Target CPA changed from $5.00 to $6.00", "Bidding", "Target CPA"),
            ("1 phrase match keyword added\n  2 negative phrase match keywords added", "Keyword", "Keyword added"),
        ]
        bad = [(t, match_summary({"raw_summary": t}), (c, sc)) for t, c, sc in cases if match_summary({"raw_summary": t}) != (c, sc)]
        assert not bad, f"miscategorized: {bad}"
        print(f"[PASS] statement categories use headline and detail lines ({len(cases)} TR/EN/legacy phrasings)")

    with _test_case("infer_operation per statement: re-enable is not REMOVE, list applied is CREATE, es/fr/ru removal verbs"):
        cases = [("Kampanya öğesi değiştirildi\n  Kaldırıldı olan Durum, Etkin olarak değiştirildi", "UPDATE"),
                 ("Kampanya öğesi değiştirildi\n  Etkin olan Durum, Kaldırıldı olarak değiştirildi", "REMOVE"),
                 ("\"Liste A\" negatif anahtar kelime listesi uygulandı", "CREATE"),
                 ("1 kampanya etkin\n  duraklatıldı olan Durum, etkin olarak değiştirildi", "UPDATE"),
                 ("Campaña eliminada", "REMOVE"), ("Campagne supprimée", "REMOVE"), ("Кампания удалена", "REMOVE"),
                 ("Anzeige erstellt", "CREATE"), ("Kampanya değiştirildi\n  1 öğe eklendi", "UPDATE")]
        bad = [(t, infer_operation(t), e) for t, e in cases if infer_operation(t) != e]
        assert not bad, f"wrong operation: {bad}"
        print("[PASS] infer_operation per statement: re-enable is not REMOVE, list applied is CREATE, es/fr/ru removal verbs")

    with _test_case("values: TR old/new, old-only, percent, creation amount, currency; normalize_number edge cases"):
        cases = [("Kampanya değiştirildi\n  ₺460,00 olan Hedef EBM, ₺437,93 değerine düşürüldü", ("₺460,00", "₺437,93", "TRY")),
                 ("Kampanya değiştirildi\n  Hedef EBM değeri ₺139,00 idi", ("₺139,00", None, "TRY")),
                 ("1 teklif ayarlama değişikliği\n  X: teklif ayarlaması %+10 olarak ayarlandı", (None, "+10", "%")),
                 ("1 bütçenin tutarı azaltıldı\n  Kampanya B: 750,00 € olan Bütçe tutarı, 725,00 € olarak değiştirildi", ("750,00 €", "725,00 €", "EUR"))]
        bad = [(t, parse_statement_values(t), e) for t, e in cases if parse_statement_values(t) != e]
        assert not bad, f"value parse: {bad}"
        assert parse_statement_values("Bütçe oluşturuldu\n  Kampanya B: Bütçe tutarı değeri: ₺1.180,00", "CREATE") == (None, "₺1.180,00", "TRY")
        nums = [("0,001", "TR", 0.001), ("0,001", "US", 0.001), ("1,500", "US", 1500.0), ("130,00 ₺", "TR", 130.0),
                ("1 234,50", "TR", 1234.5), ("1'234.50", "US", 1234.5), ("%+10", "TR", 10.0), ("9" * 80, "TR", None)]
        bad = [(v, normalize_number(v, st), e) for v, st, e in nums if normalize_number(v, st) != e]
        assert not bad, f"normalize_number: {bad}"
        print("[PASS] values: TR old/new, old-only, percent, creation amount, currency; normalize_number edge cases")

    with _test_case("A-5: an empty campaign cell takes the campaign named in a budget detail line, marked inferred_from_summary"):
        p = _r9_csv(td, "r9_owner.csv", [[ts, "a@example.test", "", "", "1 bütçenin tutarı artırıldı\n  Campaign Beta: ₺200,00 olan Bütçe tutarı, ₺260,00 olarak değiştirildi"]])
        r = run_pipeline(p, td / "out_r9_owner", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        row = _r9_rows(td / "out_r9_owner")[0]
        assert (row["campaign_name"], row["campaign_confidence"]) == ("Campaign Beta", "inferred_from_summary"), row
        assert (row["old_value_num"], row["new_value_num"], row["category"]) == (200.0, 260.0, "Budget"), row
        print("[PASS] A-5: an empty campaign cell takes the campaign named in a budget detail line, marked inferred_from_summary")

    with _test_case("L-8: automation names match whole words only"):
        people = ["Kapil Rao", "Ann Abbott", "Lee Botha", "Napier Ltd", "Capital One Team", "Rulebook Fan"]
        bots = ["ads-budget-system", "Automated rules", "Google Ads Script", "Otomatik kurallar", "Skript Konto"]
        assert all(derive_actor_type(n, None) == "human" for n in people), [n for n in people if derive_actor_type(n, None) != "human"]
        assert all(derive_actor_type(n, None) == "automation" for n in bots), [n for n in bots if derive_actor_type(n, None) != "automation"]
        print("[PASS] L-8: automation names match whole words only")

    with _test_case("S-1/S-2: --mask-users scrubs emails and names from every string field; change_id is not hashed on the identity"):
        p = _r9_csv(td, "r9_mask.csv", [
            [ts, "user.a@example.test", "Campaign Alpha", "", "Updated user.a@example.test's notification setting\n  cc other.person@example.test"],
            [ts, "User Bee", "Campaign Alpha", "", "1 dil eklendi\n  added by User Bee"],
        ])
        run_pipeline(p, td / "out_r9_plain", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        r = run_pipeline(p, td / "out_r9_mask", account_name="Account A", tz="Europe/Istanbul", mask_users=True, generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok", r
        for name in ("changes.jsonl", "change_history.json", "dashboard.html"):
            text = (td / "out_r9_mask" / name).read_text(encoding="utf-8")
            assert "@example.test" not in text and "User Bee" not in text, f"identity leaked into {name}"
        plain = {x["statement_index"]: x["change_id"] for x in _r9_rows(td / "out_r9_plain")}
        assert not set(plain.values()) & {x["change_id"] for x in _r9_rows(td / "out_r9_mask")}, "masked change_id must not equal the identity-hashed one"
        print("[PASS] S-1/S-2: --mask-users scrubs emails and names from every string field; change_id is not hashed on the identity")

    with _test_case("D-5/D-6/D-7/B-13: local-day timeline, non-negative days_since, DST gap/fold counted, no-timezone warning"):
        p = _r9_csv(td, "r9_tz.csv", [
            ["2026-08-01 01:30:00", "a@example.test", "Campaign Alpha", "", "1 dil eklendi"],
            ["2026-08-03 23:30:00", "a@example.test", "Campaign Beta", "", "1 dil eklendi"],
        ])
        r = run_pipeline(p, td / "out_r9_tz", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        ch = json.loads((td / "out_r9_tz" / "change_history.json").read_text(encoding="utf-8"))
        assert [d["date"] for d in ch["timeline"]] == ["2026-08-01", "2026-08-03"], ch["timeline"]
        assert ch["meta"]["timeline_reference_tz"] == "Europe/Istanbul"
        assert all(e["days_since_last_change_at_generation"] >= 0 for e in ch["untouched"]), ch["untouched"]
        p = _r9_csv(td, "r9_dst.csv", [["2026-03-29 02:30:00", "a@example.test", "Campaign Alpha", "", "1 dil eklendi"],
                                       ["2026-10-25 02:30:00", "a@example.test", "Campaign Alpha", "", "1 ülke eklendi"]])
        dq = run_pipeline(p, td / "out_r9_dst", account_name="Account A", tz="Europe/Berlin", generated_at="2026-08-17T00:00:00")["data_quality"]
        assert (dq["dst_nonexistent_local_time"], dq["dst_ambiguous_local_time"]) == (1, 1), dq
        dq = run_pipeline(p, td / "out_r9_notz", account_name="Account A", generated_at="2026-08-17T00:00:00")["data_quality"]
        assert any("timeline will be empty" in w for w in dq["warnings"]), dq["warnings"]
        print("[PASS] D-5/D-6/D-7/B-13: local-day timeline, non-negative days_since, DST gap/fold counted, no-timezone warning")

    with _test_case("L-6: mostly-Other output in an unknown UI language carries a data_quality warning"):
        p = _r9_csv(td, "r9_es.csv", [[ts, "a@example.test", "Campaign Alpha", "", "Campaña eliminada"],
                                      [ts, "a@example.test", "Campaign Alpha", "", "Presupuesto modificado"]])
        r = run_pipeline(p, td / "out_r9_es", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-17T00:00:00")
        assert r["other_pct"] == 100.0 and any("UI language" in w for w in r["data_quality"]["warnings"]), r["data_quality"]
        assert _r9_rows(td / "out_r9_es")[0]["operation"] == "REMOVE"
        print("[PASS] L-6: mostly-Other output in an unknown UI language carries a data_quality warning")

    with _test_case("B-9: a 130k-character summary cell is parsed in well under a second"):
        t0 = _time.time()
        for cell in ("1" * 130000, "Durum Değişti: " + "9," * 65000, "Kampanya değiştirildi\n  " + "₺1 olan " * 16000, ("1 dil eklendi\n  " + "9" * 40 + "\n") * 3000):
            for st in split_statements(cell):
                op = infer_operation(st)
                parse_statement_values(st, op)
                match_summary({"raw_summary": st})
            normalize_number(cell, "TR")
        took = _time.time() - t0
        assert took < 1.0, f"took {took:.2f}s"
        print(f"[PASS] B-9: a 130k-character summary cell is parsed in well under a second ({took:.2f}s)")


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
            # 2026-11-10 (T-11): a passed sunset date is a warning, not a failed build of the offline skill.
            if date.today() >= tool.API_VERSION_SUNSET:
                print(f"[WARN] Google Ads API {tool.API_VERSION} was scheduled to sunset in {tool.API_VERSION_SUNSET:%B %Y}; update API_VERSION, "
                      "API_VERSION_SUNSET and tools/requirements.txt together (see Google's API sunset-dates page).")
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
            start7 = (date.today() - timedelta(days=6)).isoformat()
            assert f">= '{start7}'" in seen["query"] and "LIMIT 50" in seen["query"], seen["query"]
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


def _run_r9_tools_cases(td):
    """2026-11-10, round 7 audit fixes: fetch tool, query CLI, CLI envelope,
    out-dir safety, and the test gaps the mutation run found."""
    import subprocess
    import types
    me = str(Path(__file__).resolve())

    def cli(*argv):
        proc = subprocess.run([sys.executable, me, *argv], capture_output=True, text=True, encoding="utf-8", timeout=120)
        return proc.returncode, proc.stdout, proc.stderr

    tool_path = SCRIPT_DIR.parent.parent / "tools" / "fetch_live_data.py"
    if tool_path.exists():
        import importlib.util
        spec = importlib.util.spec_from_file_location("fetch_live_data_r9", tool_path)
        tool = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tool)
        with _test_case("fetch tool: proto-plus enums come out as names, not integers; range, limit, 0600 file, zero events"):
            names = {2: "ENABLED", 3: "PAUSED"}

            class ProtoLike:
                """Shaped like proto-plus 1.29: to_dict is a classmethod and defaults to integer enums."""
                def __init__(self, data):
                    self._data = data

                @classmethod
                def to_dict(cls, instance, *, use_integers_for_enums=True, preserving_proto_field_name=True):
                    conv = (lambda v: v) if use_integers_for_enums else (lambda v: names.get(v, v))
                    return {k: {f: conv(v) for f, v in inner.items()} for k, inner in instance._data.items()}

            ev = _fake_change_event(1, "CAMPAIGN", "customers/9990001/campaigns/2001", "UPDATE", ["status"], {}, {})
            ev.change_event.old_resource = ProtoLike({"campaign": {"status": 2}})
            ev.change_event.new_resource = ProtoLike({"campaign": {"status": 3}})
            ev.change_event.change_resource_type = "CAMPAIGN"      # a plain value, no .name
            seen = {}

            class FakeClient:
                @classmethod
                def load_from_env(cls, version=None):
                    return cls()

                def get_service(self, name):
                    return types.SimpleNamespace(search=lambda customer_id, query: seen.update(query=query) or [ev])

            fakes = {k: types.ModuleType(k) for k in ("google", "google.ads", "google.ads.googleads", "google.ads.googleads.client", "google.ads.googleads.errors")}
            fakes["google.ads.googleads.client"].GoogleAdsClient = FakeClient
            fakes["google.ads.googleads.errors"].GoogleAdsException = type("GoogleAdsException", (Exception,), {})
            saved = {k: sys.modules.get(k) for k in fakes}
            sys.modules.update(fakes)
            try:
                got = tool.fetch("9990001", days=14, limit=99999)
            finally:
                for k, v in saved.items():
                    sys.modules.pop(k, None) if v is None else sys.modules.__setitem__(k, v)
            assert (got[0]["old_resource"], got[0]["new_resource"]) == ("ENABLED", "PAUSED"), f"enum values must be names: {got[0]}"
            assert got[0]["change_resource_type"] == "CAMPAIGN"
            assert f">= '{(date.today() - timedelta(days=13)).isoformat()}'" in seen["query"] and "LIMIT 10000" in seen["query"], seen["query"]
            assert "DURING LAST_" not in seen["query"], "LAST_n_DAYS is only valid for 7/14/30 and excludes today"
            out = td / "r9_fetched.json"
            tool.write_private_json(str(out), [])
            if os.name == "posix":
                assert (out.stat().st_mode & 0o777) == 0o600, oct(out.stat().st_mode)
            r0 = run_pipeline(out, td / "out_r9_empty", generated_at="2026-11-10T00:00:00")
            assert r0["status"] == "ok" and r0["row_count"] == 0 and r0.get("empty"), f"zero events must give an empty, valid report: {r0}"
            assert (td / "out_r9_empty" / "dashboard.html").exists()
            assert json.loads((td / "out_r9_empty" / "change_history.json").read_text(encoding="utf-8"))["changes"] == []
            print("[PASS] fetch tool: proto-plus enums come out as names, not integers; explicit date range incl. today, limit clamped to 10000, 0600 output; [] builds an empty report")

    with _test_case("query: Turkish-aware matching, --since anchored on now by default, mixed naive/aware rows"):
        rows = [{"user_name": "İsmail Şahin", "campaign_name": "KIŞ Kampanyası", "timestamp_iso": "2000-01-05T10:00:00+00:00", "timestamp_utc": "2000-01-05T10:00:00+00:00"},
                {"user_name": "Irmak", "campaign_name": "Yaz", "timestamp_iso": "2000-01-01T10:00:00"}]
        assert len(query_changes(rows, user="ismail sahin")) == 1 and len(query_changes(rows, campaign="kış")) == 1
        assert len(query_changes(rows, campaign="kis kampanyasi")) == 1 and len(query_changes(rows, user="ırmak")) == 1
        info = {}
        assert query_changes(rows, since="7d", info=info) == [] and info["since_anchor"]["basis"] == "now"
        assert len(query_changes(rows, since="7d", since_anchor="newest")) == 2
        try:
            query_changes(rows, since="99999999999d")
            raise AssertionError("an absurd --since must raise ValueError, not OverflowError")
        except ValueError:
            pass
        print("[PASS] query: Turkish-aware matching, --since anchored on now by default, mixed naive/aware rows")

    with _test_case("query CLI: missing file, non-JSONL, lone surrogate and internal errors are JSON envelopes, never tracebacks"):
        code, out, err = cli("query", str(td / "nope.jsonl"))
        assert code == 2 and json.loads(out)["status"] == "error" and "Traceback" not in err, (code, out, err)
        bad = td / "r9_bad.jsonl"
        bad.write_text("Tarih,Kullanıcı\n2026-08-01,a\n", encoding="utf-8")
        code, out, err = cli("query", str(bad))
        assert code == 2 and "line 1" in json.loads(out)["message"], (code, out, err)
        sur = td / "r9_sur.jsonl"
        sur.write_text('{"user_name": "\\ud800x", "timestamp_iso": "2026-08-01T00:00:00"}\n', encoding="utf-8")
        code, out, err = cli("query", str(sur), "--format", "json")
        assert code == 0 and "Traceback" not in err, (code, out, err)
        code, out, err = cli("query", str(sur), "--since", "1d")
        assert code == 0 and "counted back from now" in out, (code, out, err)
        blocker = td / "r9_is_a_file"
        blocker.write_text("x", encoding="utf-8")
        code, out, err = cli("run", str(td / "nope.csv"), "--out-dir", str(blocker))
        assert code == 3 and json.loads(out)["status"] == "internal_error" and "Traceback" not in err, (code, out, err)
        print("[PASS] query CLI: missing file, non-JSONL, lone surrogate and internal errors are JSON envelopes, never tracebacks")

    with _test_case("out-dir: symlinked output refused, failed run clears stale outputs, --open uses a file URI"):
        f1 = td / "r9_legacy.csv"
        f1.write_text(SAMPLE_LEGACY_SUMMARY_TR_CSV, encoding="utf-8")
        od = td / "out r9 open"
        opened = []
        saved_open = webbrowser.open
        webbrowser.open = lambda url, *a, **k: opened.append(url) or True
        try:
            r1 = run_pipeline(f1, od, decimal_style="TR", open_browser=True, generated_at="2026-11-10T00:00:00")
        finally:
            webbrowser.open = saved_open
        assert r1["status"] == "ok" and r1["opened"] is True and opened == [(od / "dashboard.html").resolve().as_uri()], (r1.get("opened"), opened)
        r2 = run_pipeline(td / "nope.csv", od)
        assert r2["status"] != "ok" and not (od / "dashboard.html").exists(), "a failed run must not leave the previous dashboard looking current"
        code, out, err = cli("run", str(td / "nope.csv"), "--out-dir", str(od))
        assert json.loads((od / "run_status.json").read_text(encoding="utf-8"))["outputs_current"] is False
        if os.name == "posix":
            victim = td / "r9_victim.txt"
            victim.write_text("keep", encoding="utf-8")
            od2 = td / "out_r9_link"
            od2.mkdir()
            (od2 / "dashboard.html").symlink_to(victim)
            r3 = run_pipeline(f1, od2, decimal_style="TR")
            assert r3["status"] == "error" and "symbolic link" in r3["message"] and victim.read_text(encoding="utf-8") == "keep", r3
        print("[PASS] out-dir: symlinked output refused, failed run clears stale outputs, --open uses a file URI and reports opened")

    with _test_case("mutation gaps: >30%-empty gate, structured Budget rule, new_value in change_id, Turkish value phrases"):
        hdr = SAMPLE_LEGACY_SUMMARY_TR_CSV.splitlines()[0]
        sparse = td / "r9_sparse.csv"
        sparse.write_text(hdr + "\n" + "\n".join([
            ",,2026-08-01 09:00:00,User A,CAMPAIGN,Campaign Alpha,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED",
            ",,2026-08-01 09:01:00,User A,CAMPAIGN,Campaign Alpha,,Durum Değişti: PAUSED -> ENABLED,PAUSED,ENABLED",
            "Account A,111-111-1111,2026-08-01 09:02:00,User A,CAMPAIGN,Campaign Alpha,,Durum Değişti: ENABLED -> PAUSED,ENABLED,PAUSED"]) + "\n", encoding="utf-8")
        rs = run_pipeline(sparse, td / "out_r9_sparse")
        assert rs["status"] == "needs_review" and any(i["field"] == "account_name" for i in rs["issues"]), f"2/3 empty account_name must stop the run: {rs}"
        fa = td / "r9_api.json"
        fa.write_text(json.dumps(SAMPLE_API_CHANGE_EVENT_JSON), encoding="utf-8")
        ra = run_pipeline(fa, td / "out_r9_api", generated_at="2026-11-10T00:00:00", allow_unknown_categories=True)
        assert ra["status"] == "ok", ra
        rows_a = [json.loads(l) for l in (td / "out_r9_api" / "changes.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
        budget = [r for r in rows_a if r["resource_type"] == "CAMPAIGN_BUDGET" and r["operation"] == "UPDATE"]
        assert budget and all((r["category"], r["subcategory"]) == ("Budget", "Budget changed") for r in budget), [(r["category"], r["subcategory"]) for r in budget]
        twin = td / "r9_twin.csv"
        line = "Account A,111-111-1111,2026-08-01 09:12:03,User A,CAMPAIGN_BUDGET,Campaign Alpha,,Bütçe Değişti,150.000,{}"
        twin.write_text(hdr + "\n" + line.format("200.000") + "\n" + line.format("250.000") + "\n", encoding="utf-8")
        rt = run_pipeline(twin, td / "out_r9_twin", decimal_style="TR", force_review=True)
        assert rt["status"] == "ok" and rt["row_count"] == 2, f"rows differing only in new value are two changes: {rt}"
        ids = [json.loads(l)["change_id"] for l in (td / "out_r9_twin" / "changes.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(set(ids)) == 2, "change_id must depend on new_value"
        trv = td / "r9_trvalues.csv"
        trv.write_text(hdr + "\n" + "\n".join([
            'Account A,111-111-1111,2026-08-01 10:00:00,User A,CAMPAIGN,Campaign Alpha,,"₺100,00 olan Bütçe, ₺150,00 olarak değiştirildi",,',
            "Account A,111-111-1111,2026-08-01 10:01:00,User A,CAMPAIGN,Campaign Alpha,,\"40 olan Hedef EBM, 35 değerine azaltıldı\",,",
            "Account A,111-111-1111,2026-08-01 10:02:00,User A,CAMPAIGN,Campaign Alpha,,\"20 olan Hedef EBM, 25 değerine artırıldı\",,"]) + "\n", encoding="utf-8")
        rv = run_pipeline(trv, td / "out_r9_trv", decimal_style="TR", force_review=True, allow_unknown_categories=True)
        assert rv["status"] == "ok", rv
        got = sorted((json.loads(l)["old_value_num"], json.loads(l)["new_value_num"]) for l in (td / "out_r9_trv" / "changes.jsonl").read_text(encoding="utf-8").splitlines() if l.strip())
        assert got == [(20, 25), (40, 35), (100, 150)], f"Turkish 'olan ... değerine/olarak' values: {got}"
        print("[PASS] mutation gaps: >30%-empty gate, structured Budget rule, new_value in change_id, Turkish value phrases all asserted")

    with _test_case("needs_mapping: e-mail addresses in sample_values are redacted"):
        fu = td / "r9_unknown.csv"
        fu.write_text("When It Happened,Who Did It,Thing Touched\n2026-08-01 10:00:00,person.x@example.test,x\n", encoding="utf-8")
        ru = run_pipeline(fu, td / "out_r9_unknown")
        assert ru["status"] == "needs_mapping", ru
        assert "@example.test" not in json.dumps(ru.get("sample_values")), ru.get("sample_values")
        print("[PASS] needs_mapping: e-mail addresses in sample_values are redacted")


_DIGEST_JUDGEMENT_RE = re.compile(r"\b(good|bad|better|worse|improved|worsened|risky|healthy|poor|iyi|kötü|daha iyi|iyileşti|kötüleşti|riskli|başarılı|başarısız|sağlıklı)\b", re.IGNORECASE)


def _run_r10_docs_cases(td):
    """2026-11-14 round 10 (DOCS): digest.md and the fetch tool's v26 pin. Synthetic data only."""
    global DIGEST_LANG
    tr_csv = ('Değişiklik geçmişi raporu\n1 Ağustos 2026 - 19 Ağustos 2026\n'
              'Tarih ve saat,Kullanıcı,Kampanya,Reklam grubu,Değişiklik\n'
              '11 Ağu 2026 02:33:50,user.a@example.test,Kampanya A,,"Kampanya değiştirildi\n  Durum: Etkin -> Duraklatıldı"\n'
              '4 Ağu 2026 16:16:32,user.b@example.test,Kampanya | B,Grup 1,"Anahtar kelime eklendi\n  örnek kelime"\n'
              '4 Ağu 2026 16:10:00,user.a@example.test,Kampanya | B,Grup 1,"Anahtar kelime eklendi\n  başka kelime"\n')
    f = td / "r10_docs_tr.csv"
    f.write_text(tr_csv, encoding="utf-8")
    with _test_case("r10 docs: digest.md is written on ok, auto picks tr for a Turkish export, tables escape |"):
        r = run_pipeline(f, td / "r10_dg_tr", account_name="Account A", tz="Europe/Istanbul", generated_at="2026-08-20T00:00:00")
        assert r["status"] == "ok", r
        md = (td / "r10_dg_tr" / "digest.md").read_text(encoding="utf-8")
        assert md.startswith("# Google Ads değişiklik özeti"), md[:80]
        for want in ("04.08.2026 - 11.08.2026", "| 2026-W32 |", "| 2026-W33 |", "Kampanya \\| B", "| Account A | 3 |", "user.a@example.test"):
            assert want in md, (want, md)
        assert not _DIGEST_JUDGEMENT_RE.search(md), _DIGEST_JUDGEMENT_RE.search(md)
        print("[PASS] r10 docs: digest.md is written on ok, auto picks tr for a Turkish export, tables escape |")
    with _test_case("r10 docs: digest respects --mask-users and --digest-lang en"):
        DIGEST_LANG = "en"
        try:
            r = run_pipeline(f, td / "r10_dg_mask", account_name="Account A", tz="Europe/Istanbul", mask_users=True, generated_at="2026-08-20T00:00:00")
        finally:
            DIGEST_LANG = "auto"
        md = (td / "r10_dg_mask" / "digest.md").read_text(encoding="utf-8")
        assert r["status"] == "ok" and md.startswith("# Google Ads change digest"), md[:80]
        assert "@" not in md and re.search(r"\| User [A-Z]+ \| person \| 2 \|", md) and "masked" in md, md
        assert not _DIGEST_JUDGEMENT_RE.search(md), _DIGEST_JUDGEMENT_RE.search(md)
        print("[PASS] r10 docs: digest respects --mask-users and --digest-lang en")
    with _test_case("r10 docs: auto picks en for an English export; a stopped run removes the old digest"):
        e = _r9_csv(td, "r10_docs_en.csv", [["Aug 4, 2026, 4:16:32 PM", "user.c@example.test", "Campaign C", "", "Campaign paused"]])
        r = run_pipeline(e, td / "r10_dg_en", account_name="Account C", tz="UTC", generated_at="2026-08-20T00:00:00")
        assert r["status"] == "ok", r
        assert (td / "r10_dg_en" / "digest.md").read_text(encoding="utf-8").startswith("# Google Ads change digest")
        bad = td / "r10_docs_bad.csv"
        bad.write_text("Foo,Bar\n1,2\n", encoding="utf-8")
        r2 = run_pipeline(bad, td / "r10_dg_en")
        assert r2["status"] != "ok" and not (td / "r10_dg_en" / "digest.md").exists(), r2
        print("[PASS] r10 docs: auto picks en for an English export; a stopped run removes the old digest")
    with _test_case("r10 docs: digest reads meta.sources files, coverage notes (tr wording by id) and cross-file duplicates"):
        rows = [{"timestamp_iso": "2026-08-04T10:00:00", "category": "Budget", "campaign_name": "Kampanya A", "account_name": "Account A", "source_file": "a.csv"}]
        agg = {"summary": {"active_accounts": 1}, "users": {}, "categories": [{"category": "Budget", "count": 1}], "accounts": [], "untouched": []}
        meta = {"sources": [{"file": "a.csv", "rows": 1}, {"file": "b.csv", "rows": 0}], "generated_at": "2026-08-20T00:00:00",
                "coverage_notes": [{"id": "api_30_day_window", "text": "API change history covers 30 days."}, {"id": "new_note", "text": "A new note."}],
                "data_quality": {"input_rows": 2, "rows_in_report": 1, "left_out": {"unreadable_date": 0, "duplicate": 0, "duplicate_across_files": 1}, "in_report_with_limits": {}}}
        out = td / "r10_dg_direct"
        out.mkdir()
        assert write_digest(out, rows, agg, meta, "tr") == "tr"
        md = (out / "digest.md").read_text(encoding="utf-8")
        for want in ("a.csv (1 satır), b.csv (0 satır)", _COVERAGE_NOTES_TR["api_30_day_window"], "- A new note.", "dosyalar arası yinelenen: 1"):
            assert want in md, (want, md)
        assert write_digest(out, [], {"summary": {}, "users": {}}, {}, "auto") == "en"
        print("[PASS] r10 docs: digest reads meta.sources files, coverage notes (tr wording by id) and cross-file duplicates")
    with _test_case("r10 docs: --digest-lang is a run flag with auto|tr|en"):
        import subprocess
        p = subprocess.run([sys.executable, str(Path(__file__).resolve()), "run", str(f), "--out-dir", str(td / "r10_dg_cli"), "--account-name", "Account A",
                            "--timezone", "Europe/Istanbul", "--digest-lang", "en"], capture_output=True, text=True, env=dict(os.environ, XDG_DATA_HOME=str(td / "r10_xdg")))
        assert p.returncode == 0, p.stdout + p.stderr
        assert (td / "r10_dg_cli" / "digest.md").read_text(encoding="utf-8").startswith("# Google Ads change digest")
        print("[PASS] r10 docs: --digest-lang is a run flag with auto|tr|en")
    tool_path = SCRIPT_DIR.parent.parent / "tools" / "fetch_live_data.py"
    if tool_path.exists():
        with _test_case("r10 docs: fetch tool asks for v26 (sunset Nov 2027) and requirements need google-ads >= 34.0.0"):
            src = tool_path.read_text(encoding="utf-8")
            assert re.search(r'^API_VERSION = "v26"$', src, re.M) and "API_VERSION_SUNSET = date(2027, 11, 1)" in src
            req = (tool_path.parent / "requirements.txt").read_text(encoding="utf-8")
            assert re.search(r"^google-ads>=34\.0\.0$", req, re.M), req
            print("[PASS] r10 docs: fetch tool asks for v26 (sunset Nov 2027) and requirements need google-ads >= 34.0.0")


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
        assert query_changes([{"timestamp_iso": "2026-08-01T00:00:00"}], since="0d", since_anchor="newest") == [{"timestamp_iso": "2026-08-01T00:00:00"}]
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
        # 2026-11-10: "Campaign changed" + "Status changed ... to active" is an enable (detail rule).
        assert cats == {"Status", "AdGroup", "Budget"}, f"unexpected category set: {cats}"
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
    _run_r9_ingest_cases(td)
    _run_r9_tools_cases(td)
    _run_r9_semantics_cases(td)
    _run_r9_dashboard_cases(td)
    _run_r10_docs_cases(td)


# =====================================================================
# CLI
# =====================================================================
def _emit(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def _read_jsonl(path):
    """changes.jsonl -> rows, or a structured error dict (2026-11-10, B-12/S-7)."""
    p = Path(path)
    if not p.is_file():
        return {"status": "error", "message": f"Input file not found: {path}"}
    rows = []
    try:
        with open(p, encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except ValueError as e:
                    return {"status": "error", "message": f"{path} line {lineno} is not JSON ({e}). query reads the changes.jsonl a run wrote, one JSON object per line."}
                if not isinstance(row, dict):
                    return {"status": "error", "message": f"{path} line {lineno} is not a JSON object. query reads the changes.jsonl a run wrote."}
                rows.append(row)
    except UnicodeDecodeError as e:
        return {"status": "error", "message": f"{path} is not UTF-8 text ({e.reason}). query reads the changes.jsonl a run wrote."}
    return rows


def main():
    """CLI entry. 2026-11-10 (B-4): UTF-8 stdout on any console; an unexpected
    exception becomes a JSON envelope with status internal_error and exit 3."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except (AttributeError, ValueError):
            pass
    try:
        _main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as e:
        _emit({"status": "internal_error", "message": f"Unexpected {type(e).__name__}: {e}",
               "instructions": "This is a bug in the tool, not in the input. Report it with the command you ran (no customer data)."})
        sys.exit(3)


def _main():
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
    p_run.add_argument("--digest-lang", choices=["auto", "tr", "en"], default="auto", help="Language of digest.md: auto (default) is tr for a Turkish export, else en.")
    p_run.add_argument("--encoding", help="Input text encoding (e.g. cp1254, cp1251, utf-16) when auto-detection rejects the file as garbled.")

    p_query = sub.add_parser("query", help="Deterministic filter over a changes.jsonl.")
    p_query.add_argument("input")
    p_query.add_argument("--user")
    p_query.add_argument("--account")
    p_query.add_argument("--campaign")
    p_query.add_argument("--category")
    p_query.add_argument("--operation")
    p_query.add_argument("--source")
    p_query.add_argument("--since", help="e.g. 7d, 2w, 1m. Counts back from now unless --since-anchor newest.")
    p_query.add_argument("--since-anchor", choices=["now", "newest"], default="now", help="now (default): count --since back from the current time. newest: from the newest row in the file.")
    p_query.add_argument("--format", choices=["table", "json"], default="table")

    sub.add_parser("self-test", help="Run the built-in fixtures end-to-end and assert expected behavior.")

    args = ap.parse_args()

    if args.cmd == "self-test":
        ok = self_test()
        sys.exit(0 if ok else 1)

    if args.cmd == "run":
        global INPUT_ENCODING
        INPUT_ENCODING = args.encoding or None
        global DIGEST_LANG
        DIGEST_LANG = args.digest_lang
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
        _write_run_status(args.out_dir, result)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        sys.exit(0 if result.get("status") == "ok" else 2)

    if args.cmd == "query":
        rows = _read_jsonl(args.input)
        if isinstance(rows, dict):
            _emit(rows)
            sys.exit(2)
        info = {}
        try:
            matched = query_changes(rows, user=args.user, account=args.account, campaign=args.campaign,
                                     category=args.category, operation=args.operation, source=args.source, since=args.since,
                                     since_anchor=args.since_anchor, info=info)
        except (ValueError, OverflowError) as e:
            _emit({"status": "error", "message": str(e)})
            sys.exit(2)
        if args.format == "json":
            out = {"count": len(matched), "rows": matched}
            if info.get("since_anchor"):
                out["since_anchor"] = info["since_anchor"]
            _emit(out)
        else:
            print(f"{len(matched)} matching change(s)")
            if info.get("since_anchor"):
                a = info["since_anchor"]
                print(f"--since counted back from {a['basis']} ({a['anchor_utc']}); cutoff {a['cutoff_utc']}")
            print()
            for r in matched:
                actor = r.get("user_name") or r.get("user_email") or (f"[{r.get('client_type')}]" if r.get("actor_type") == "automation" else "unknown")
                print(f"{r.get('timestamp')}  {actor:<20}  {r.get('account_name') or '':<20}  {r.get('campaign_name') or '—':<25}  {r.get('category')}/{r.get('subcategory')}  {r.get('old_value')} -> {r.get('new_value')}")


def _write_run_status(out_dir, result):
    """run_status.json says whether the outputs next to it are from a run that
    reached ok (2026-11-10, B-11). Best effort: never fails the run."""
    try:
        p = Path(out_dir) / "run_status.json"
        if p.parent.is_dir() and not p.is_symlink():
            p.write_text(json.dumps({"status": result.get("status"), "written_at": datetime.now().isoformat(),
                                     "outputs_current": result.get("status") == "ok"}, indent=2), encoding="utf-8")
    except OSError:
        pass


if __name__ == "__main__":
    main()
