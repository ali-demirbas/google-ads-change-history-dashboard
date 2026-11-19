#!/usr/bin/env python3
"""
ads_change_history.py — Ads Change History, single file.

WHAT THIS IS
  Answers: who changed what, when, in which account/campaign/ad group, what the
  old and new value were, and what category the change falls into. It also
  reports which campaigns were touched recently and which weren't (within the
  scope of the data it was given).

  It flags facts against thresholds the user can set (12 rules, see
  references/rules.md), e.g. "budget changed by +132% in one step" or "Campaign
  X hasn't been touched in 23 days", worded as "worth a look". It never says
  whether a change was good, bad or risky, and never guesses intent.

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
        # 2026-11-14: EN mirror of the TR asset rule above.
        {"headline": r"^(Kampanya öğesi değiştirildi|Campaign asset changed)", "detail": r"(olan Durum, Kaldırıldı olarak|^Status changed from \S+ to Removed)", "category": "Asset", "subcategory": "Asset removed"},
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
# 2026-11-14: a Google staff edit made with the owner's permission is neither the owner nor automation.
GOOGLE_STAFF_USER_RE = re.compile(r"^\s*Google Ads Team\b", re.IGNORECASE)
AUTOMATION_USER_PATTERNS = re.compile(
    r"(^ads-|\b(system|automated|automation|rules?|scripts?|bot|api|recommendations?|auto-?appl(?:y|ied)|otomatik|otomasyon|kural|automático|automática|automatique|automatisch|automatico|Skript|скрипт|автоматическ\w*)\b)",
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
# 2026-11-15 R-5: the API does not return Editor changes; EDITOR stays listed in case a UI export or a future API version carries it.
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
# 2026-11-14: new-only target set, "Hedef EBM: ₺180,00" / "Hedef ROAS: %300" / "Teklif sınırı: ₺15,00".
SUMMARY_TARGET_SET_RE = re.compile(r"^\s*(?:Hedef EBM|Hedef ROAS|Teklif sınırı|Target CPA|Target ROAS|Bid limit)\s*:\s*(?:%\s?(\d[\d.,]{0,10})|(\d[\d.,]{0,10})\s?%|(" + _TR_AMOUNT + r"))\s*$", re.IGNORECASE)
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
    # 2026-11-15 R-1: target-set lines only on change headlines; a CREATE keeps its budget value (or none).
    is_change = (operation or infer_operation(text)) == "UPDATE" and not _OP_CREATE_RE.search(split_statement(str(text or ""))[0])
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
        m = SUMMARY_TARGET_SET_RE.search(line) if is_change else None
        if m:
            if m.group(3):
                return None, m.group(3).strip(), _currency_of(m.group(3))
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
    if user_name and GOOGLE_STAFF_USER_RE.search(user_name):
        return "unknown"
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
        # 2026-11-14: a cell cut short by a cross-file duplicate ends where its event_id does.
        n = next((j for j, r in enumerate(cell) if r.get("event_id") != first.get("event_id")), len(cell))
        cell = cell[:n]
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
/* 2026-11-19: Literata and Inter, SIL Open Font License 1.1 (OFL-1.1), subset
   woff2 with their name tables (copyright and licence) kept; no network. */
@font-face { font-family: 'Literata'; font-style: normal; font-weight: 300 400; font-display: swap; unicode-range: U+0020-007E,U+00A0-00FF,U+0131,U+0152-0153,U+02C6,U+02DA,U+02DC,U+2010-2027,U+2030-203A,U+2044,U+20AC,U+2122,U+2190-2193,U+2212,U+2215; src: url(data:font/woff2;base64,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) format('woff2'); }
@font-face { font-family: 'Literata'; font-style: normal; font-weight: 300 400; font-display: swap; unicode-range: U+0100-0130,U+0132-0151,U+0154-017F,U+0218-021B,U+20A0-20C0; src: url(data:font/woff2;base64,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) format('woff2'); }
@font-face { font-family: 'Literata'; font-style: italic; font-weight: 300 400; font-display: swap; unicode-range: U+0020,U+0025,U+002B-002E,U+0030-0039,U+00A0,U+2212; src: url(data:font/woff2;base64,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) format('woff2'); }
@font-face { font-family: 'Inter'; font-style: normal; font-weight: 400 600; font-display: swap; unicode-range: U+0020-007E,U+00A0-00FF,U+0131,U+0152-0153,U+02C6,U+02DA,U+02DC,U+2010-2027,U+2030-203A,U+2044,U+20AC,U+2122,U+2190-2193,U+2212,U+2215; src: url(data:font/woff2;base64,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) format('woff2'); }
@font-face { font-family: 'Inter'; font-style: normal; font-weight: 400 600; font-display: swap; unicode-range: U+0100-0130,U+0132-0151,U+0154-017F,U+0218-021B,U+20A0-20C0; src: url(data:font/woff2;base64,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) format('woff2'); }
/* 2026-11-19: Optimize Peak tokens (approved mockup); dark mode re-points them. */
:root {
  --paper: #fcfcf8; --paper-2: #f4f4ec; --card: #ffffff;
  --ink: #141511; --text: #2a2a27; --muted: #64645e;
  --line: #e4e4dc; --line-strong: #d3d3ca;
  --blue: #2a46c4; --link: #2a46c4; --night: #16213f; --ice: #cdd9ff; --ice-100: #eef2ff; --ice-200: #dbe4ff;
  --focus-bg: #16213f; --focus-v: #ffffff; --focus-l: #c3cbe3;
  --tag-bg: #eef2ff; --tag-fg: #16213f; --count-bg: #dbe4ff; --count-fg: #16213f;
  --quiet-bg: #f4f4ec; --quiet-fg: #5c5c56;
  --tab-on-bg: #141511; --tab-on-fg: #ffffff; --nav-bg: rgb(255 255 255 / .86);
  --s1: #16213f; --s2: #2a46c4; --s3: #6f84e0; --s4: #a9b8f2; --s5: #cdd9ff;
  --bar: #2a46c4; --bar-soft: #a9b8f2; --bar-on: #16213f;
  --overlay: rgb(20 21 17 / .42);
  --serif: 'Literata', ui-serif, Georgia, 'Times New Roman', serif;
  --sans: 'Inter', ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  --ring: 0 0 0 1px rgb(20 21 17 / .07);
  --shadow: 0 0 0 1px rgb(20 21 17 / .06), 0 18px 40px -24px rgb(20 21 17 / .30);
  --float: 0 0 0 1px #e4e4dc, 0 8px 24px -12px rgb(20 21 17 / .25);
  color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --paper: #101114; --paper-2: #1c1e23; --card: #17191d;
    --ink: #f2f2ec; --text: #dadad3; --muted: #a5a59d;
    --line: #2b2e34; --line-strong: #3b3f46;
    --blue: #9fb1ff; --link: #a9b9ff; --night: #e3e9ff; --ice: #3a4677; --ice-100: #1f2540; --ice-200: #2c365c;
    --focus-bg: #26346b; --focus-v: #ffffff; --focus-l: #cbd3f0;
    --tag-bg: #232a45; --tag-fg: #dbe3ff; --count-bg: #2c365c; --count-fg: #eef2ff;
    --quiet-bg: #23262c; --quiet-fg: #c2c2ba;
    --tab-on-bg: #f2f2ec; --tab-on-fg: #141511; --nav-bg: rgb(28 30 35 / .9);
    --s1: #e3e9ff; --s2: #8fa3ff; --s3: #5c70cf; --s4: #3d4b8c; --s5: #2c365c;
    --bar: #8fa3ff; --bar-soft: #4a5aa3; --bar-on: #e3e9ff;
    --overlay: rgb(0 0 0 / .6);
    --ring: 0 0 0 1px rgb(255 255 255 / .08);
    --shadow: 0 0 0 1px rgb(255 255 255 / .08), 0 18px 40px -24px rgb(0 0 0 / .6);
    --float: 0 0 0 1px #2b2e34, 0 8px 24px -12px rgb(0 0 0 / .6);
    color-scheme: dark;
  }
}
:root[data-theme="dark"] {
  --paper: #101114; --paper-2: #1c1e23; --card: #17191d;
  --ink: #f2f2ec; --text: #dadad3; --muted: #a5a59d;
  --line: #2b2e34; --line-strong: #3b3f46;
  --blue: #9fb1ff; --link: #a9b9ff; --night: #e3e9ff; --ice: #3a4677; --ice-100: #1f2540; --ice-200: #2c365c;
  --focus-bg: #26346b; --focus-v: #ffffff; --focus-l: #cbd3f0;
  --tag-bg: #232a45; --tag-fg: #dbe3ff; --count-bg: #2c365c; --count-fg: #eef2ff;
  --quiet-bg: #23262c; --quiet-fg: #c2c2ba;
  --tab-on-bg: #f2f2ec; --tab-on-fg: #141511; --nav-bg: rgb(28 30 35 / .9);
  --s1: #e3e9ff; --s2: #8fa3ff; --s3: #5c70cf; --s4: #3d4b8c; --s5: #2c365c;
  --bar: #8fa3ff; --bar-soft: #4a5aa3; --bar-on: #e3e9ff;
  --overlay: rgb(0 0 0 / .6);
  --ring: 0 0 0 1px rgb(255 255 255 / .08);
  --shadow: 0 0 0 1px rgb(255 255 255 / .08), 0 18px 40px -24px rgb(0 0 0 / .6);
  --float: 0 0 0 1px #2b2e34, 0 8px 24px -12px rgb(0 0 0 / .6);
  color-scheme: dark;
}

/* 2026-11-15 B-1: the hidden attribute wins over every display rule */
[hidden] { display: none !important; }
*, *::before, *::after { box-sizing: border-box; }
html { scroll-padding-top: 92px; -webkit-text-size-adjust: 100%; }
body { margin: 0; background: var(--paper); color: var(--text); font: 15px/1.55 var(--sans); -webkit-font-smoothing: antialiased; text-rendering: optimizeLegibility; }
h1, h2, h3, h4, p, dl, dd, figure { margin: 0; }
a { color: var(--link); text-underline-offset: 2px; }
button, input, select { font: inherit; color: inherit; }
:focus-visible { outline: 2px solid var(--blue); outline-offset: 2px; }
.sr { position: absolute !important; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
.skip { position: absolute; left: 12px; top: -80px; z-index: 60; padding: 8px 14px; border-radius: 999px; background: var(--ink); color: var(--paper); font-weight: 500; text-decoration: none; }
.skip:focus { top: 12px; }
.wrap { max-width: 72rem; margin: 0 auto; padding: 0 32px; }
.wrap.wide { max-width: 92rem; }
.num, table { font-variant-numeric: tabular-nums; }
.muted { color: var(--muted); }

/* floating pill nav */
.nav { position: sticky; top: 14px; z-index: 30; display: flex; justify-content: center; margin-top: 14px; padding: 0 12px; pointer-events: none; }
.navpill { pointer-events: auto; display: flex; align-items: center; gap: 6px; max-width: 100%; background: var(--nav-bg); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px); box-shadow: var(--float); border-radius: 999px; padding: 6px 6px 6px 18px; }
.brand { font-weight: 600; color: var(--ink); font-size: 14px; margin-right: 14px; display: flex; align-items: center; gap: 8px; white-space: nowrap; }
.dot { width: 20px; height: 20px; border-radius: 50%; background: var(--blue); display: inline-grid; place-items: center; color: var(--card); font: 600 11px var(--sans); }
.tab { display: inline-flex; align-items: center; min-height: 36px; border: 0; background: none; font: 500 14px var(--sans); color: var(--muted); padding: 8px 14px; border-radius: 999px; cursor: pointer; text-decoration: none; white-space: nowrap; }
.tab:hover { color: var(--ink); }
.tab[aria-current="page"] { background: var(--tab-on-bg); color: var(--tab-on-fg); }
.tab .n { font: 500 11px var(--mono); margin-right: 6px; }
.tab[aria-current="page"] .n { opacity: .75; }

/* page 1: hero */
.hero { padding: 64px 0 28px; display: grid; grid-template-columns: 1.35fr 1fr; gap: 48px; align-items: end; }
.eyebrow { font: 500 12px var(--mono); letter-spacing: .06em; text-transform: uppercase; color: var(--muted); overflow-wrap: anywhere; }
h1.headline { font: 300 54px/1.04 var(--serif); letter-spacing: -.025em; color: var(--ink); margin: 14px 0 18px; text-wrap: balance; overflow-wrap: anywhere; }
h1.headline em { font-style: italic; color: var(--blue); }
.lede { font-size: 17px; color: var(--text); max-width: 36rem; margin: 0; text-wrap: pretty; }
.lede b { color: var(--ink); font-weight: 600; }
.meta { display: grid; gap: 10px; justify-self: end; width: 100%; max-width: 320px; }
.meta > div { display: flex; justify-content: space-between; gap: 16px; border-bottom: 1px solid var(--line); padding-bottom: 8px; font-size: 13px; }
.meta dt { color: var(--muted); }
.meta dd { text-align: right; color: var(--text); overflow-wrap: anywhere; }

/* page 1: stats */
.stats { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin: 20px 0 40px; }
.stat { background: var(--card); box-shadow: var(--shadow); border-radius: 18px; padding: 20px 20px 18px; min-width: 0; }
.stat .v { font: 300 44px/1 var(--serif); color: var(--ink); letter-spacing: -.03em; font-variant-numeric: lining-nums; overflow-wrap: anywhere; }
.stat .l { margin-top: 10px; font-size: 13px; color: var(--muted); }
.stat.focus { background: var(--focus-bg); box-shadow: none; }
.stat.focus .v { color: var(--focus-v); }
.stat.focus .l { color: var(--focus-l); }

/* sections */
.sec { margin: 0 0 44px; min-width: 0; }
.sec-h { display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 4px 16px; margin-bottom: 16px; }
h2.h2 { font: 300 32px/1.1 var(--serif); letter-spacing: -.02em; color: var(--ink); margin: 0; }
.sec-h p, .sec-h a { margin: 0; color: var(--muted); font-size: 13px; }
.sec-h a { text-decoration: none; }
.sec-h a:hover { color: var(--ink); text-decoration: underline; }
.grid2 { display: grid; grid-template-columns: 1.25fr 1fr; gap: 18px; }
.aside { display: grid; gap: 18px; align-content: start; min-width: 0; }

/* page 1: flags */
.flags { display: grid; gap: 10px; }
.flag { background: var(--card); box-shadow: var(--ring); border-radius: 16px; padding: 16px 18px; display: grid; grid-template-columns: 44px minmax(0, 1fr) auto; gap: 14px; align-items: start; transition: box-shadow .2s; }
.flag:hover { box-shadow: var(--shadow); }
.flag .no { font: 500 12px var(--mono); color: var(--muted); padding-top: 3px; }
.flag h3 { margin: 0 0 4px; font: 500 15px/1.45 var(--sans); color: var(--ink); }
.flag h3 a { color: inherit; text-decoration: none; }
.flag h3 a:hover { text-decoration: underline; }
.flag p { margin: 0; font-size: 13px; color: var(--muted); }
.flag .ex { margin: 10px 0 0; padding: 0; list-style: none; display: grid; gap: 4px; }
.flag .ex a { font-size: 12.5px; color: var(--text); display: flex; gap: 10px; text-decoration: none; min-width: 0; }
.flag .ex a:hover span { text-decoration: underline; }
.flag .ex li.more a { color: var(--link); font-weight: 500; }
.flag-info { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; padding: 14px 18px; border-radius: 16px; background: var(--quiet-bg); color: var(--quiet-fg); font-size: 13px; }
.flag-info .fi-h { font-weight: 600; color: var(--ink); margin-right: 2px; }
.flag-info a { color: var(--text); text-decoration: none; display: inline-flex; align-items: center; gap: 6px; }
.flag-info a:hover { text-decoration: underline; }
.budget .v .sgn { font-family: var(--sans); font-weight: 300; margin-right: .06em; } /* 2026-11-19: the light serif's plus is too small */
.flag .ex time, .flist time { font-family: var(--mono); font-size: 11.5px; color: var(--muted); min-width: 92px; flex: none; padding-top: 1px; }
.flag .ex span { min-width: 0; overflow-wrap: anywhere; }
.count { font: 600 13px var(--sans); padding: 4px 11px; border-radius: 999px; white-space: nowrap; font-variant-numeric: tabular-nums; }
.count.attn { background: var(--count-bg); color: var(--count-fg); }
.count.info { background: var(--quiet-bg); color: var(--quiet-fg); }
.flag-empty { background: var(--card); box-shadow: var(--ring); border-radius: 16px; padding: 18px; font-size: 14px; color: var(--muted); }

/* cards */
.card { background: var(--card); box-shadow: var(--shadow); border-radius: 18px; padding: 20px; min-width: 0; }
.card h3 { margin: 0 0 4px; font: 500 15px/1.4 var(--sans); color: var(--ink); }
.card .sub { font-size: 13px; color: var(--muted); margin: 0 0 16px; }
.card .more { display: inline-block; margin-top: 12px; font-size: 13px; text-decoration: none; }
.card .more:hover { text-decoration: underline; }
.wbar { display: flex; height: 12px; border-radius: 999px; overflow: hidden; background: var(--quiet-bg); }
.wbar i { display: block; height: 100%; }
.legend { display: grid; gap: 8px; margin: 14px 0 0; padding: 0; list-style: none; font-size: 13px; }
.legend li { display: flex; align-items: center; gap: 8px; min-width: 0; }
.legend a, .legend .lt { color: var(--text); text-decoration: none; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.legend a:hover { text-decoration: underline; }
.legend b { margin-left: auto; font-weight: 500; color: var(--ink); font-variant-numeric: tabular-nums; }
.sw { width: 10px; height: 10px; border-radius: 3px; flex: none; }
.budget { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.budget .b { border-radius: 14px; padding: 14px; background: var(--ice-100); min-width: 0; }
.budget .b.alt { background: var(--quiet-bg); }
.budget .v { font: 300 28px/1 var(--serif); color: var(--ink); overflow-wrap: anywhere; }
.budget .l { font-size: 12px; color: var(--muted); margin-top: 6px; }
.stale { display: grid; gap: 8px; margin: 0; padding: 0; list-style: none; font-size: 13px; }
.stale li { display: flex; justify-content: space-between; align-items: center; gap: 10px; border-bottom: 1px solid var(--line); padding-bottom: 8px; min-width: 0; }
.stale li:last-child { border: 0; padding: 0; }
.stale a { color: var(--text); text-decoration: none; min-width: 0; overflow-wrap: anywhere; }
.stale a:hover { text-decoration: underline; }
.stale small { display: block; color: var(--muted); font-size: 12px; }
.dpill { font-family: var(--mono); font-size: 12px; color: var(--tag-fg); background: var(--tag-bg); border-radius: 999px; padding: 2px 9px; white-space: nowrap; flex: none; }
.p1note { font-size: 12.5px; color: var(--muted); border-top: 1px solid var(--line); padding: 18px 0 48px; display: flex; gap: 8px 24px; flex-wrap: wrap; }
.p1note span { max-width: 62ch; }

/* page 2 */
.layout2 { display: grid; grid-template-columns: 220px minmax(0, 1fr); gap: 40px; padding: 56px 0 64px; }
.side { position: sticky; top: 92px; align-self: start; }
.side ol { list-style: none; margin: 0; padding: 0; font-size: 14px; }
.side a { display: flex; justify-content: space-between; gap: 10px; padding: 10px 0; border-bottom: 1px solid var(--line); color: var(--muted); text-decoration: none; }
.side a:hover { color: var(--ink); }
.side a.on { color: var(--ink); border-bottom-color: var(--ink); }
.side .k { font-family: var(--mono); font-size: 12px; }
.p2s { margin: 0 0 56px; min-width: 0; scroll-margin-top: 92px; }
.p2s > .sec-h { margin-bottom: 14px; }
.caption { margin: -6px 0 16px; font-size: 13px; color: var(--muted); max-width: 80ch; }
.h3 { margin: 28px 0 8px; font: 500 15px/1.4 var(--sans); color: var(--ink); }
#untouchedBlock h2 { margin: 28px 0 8px; font: 500 15px/1.4 var(--sans); color: var(--ink); }
.tcard { background: var(--card); box-shadow: var(--shadow); border-radius: 18px; padding: 6px 8px; min-width: 0; }
.pad { padding: 18px 20px; }
.fctx { position: sticky; top: 84px; z-index: 5; display: flex; flex-wrap: wrap; align-items: center; gap: 6px 14px; margin: 0 0 22px; padding: 10px 14px; border-radius: 14px; background: var(--ice-100); color: var(--tag-fg); font-size: 13px; box-shadow: var(--ring); }
.fctx span { overflow-wrap: anywhere; min-width: 0; }
.fctx a { color: var(--tag-fg); font-weight: 500; }
.linkish { padding: 0; border: 0; background: none; color: var(--link); font: 500 13px var(--sans); text-decoration: underline; text-underline-offset: 2px; cursor: pointer; }
.fctx .linkish { color: var(--tag-fg); }
.tag { display: inline-block; font-size: 11.5px; line-height: 1.6; padding: 1px 8px; border-radius: 999px; background: var(--tag-bg); color: var(--tag-fg); margin: 1px 4px 1px 0; white-space: nowrap; }
.tag.info { background: var(--quiet-bg); color: var(--quiet-fg); }
.fgroup { background: var(--card); box-shadow: var(--ring); border-radius: 16px; padding: 16px 18px; margin-bottom: 12px; }
.fgroup-h { display: flex; justify-content: space-between; align-items: start; gap: 12px; }
.fgroup h3 { font: 500 15px/1.45 var(--sans); color: var(--ink); }
.fgroup p { font-size: 13px; color: var(--muted); }
.fgroup .rule { margin-top: 4px; font-size: 12.5px; color: var(--muted); }
.flist { list-style: none; margin: 10px 0 0; padding: 0; display: grid; gap: 2px; }
.flist a { display: grid; grid-template-columns: 92px minmax(0, 1fr); gap: 2px 10px; padding: 6px 8px; margin: 0 -8px; border-radius: 10px; color: var(--text); text-decoration: none; font-size: 13px; }
.flist a:hover { background: var(--paper-2); }
.flist .fw { min-width: 0; overflow-wrap: anywhere; }
.flist .fw small { display: block; color: var(--muted); font-size: 12px; }

/* tables */
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { max-width: 240px; padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; vertical-align: top; }
th { position: sticky; top: 0; background: var(--card); color: var(--muted); font: 500 12px var(--sans); border-bottom-color: var(--line-strong); user-select: none; }
tbody tr:last-child td { border-bottom: 0; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
td.wrapc { white-space: normal; min-width: 160px; }
.th-btn { padding: 0; border: 0; background: none; color: inherit; font: inherit; cursor: pointer; }
th[aria-sort="ascending"], th[aria-sort="descending"] { color: var(--ink); }
th[aria-sort="descending"] .th-btn::after { content: " \25BE"; color: var(--blue); }
th[aria-sort="ascending"] .th-btn::after { content: " \25B4"; color: var(--blue); }
tr.datarow { cursor: pointer; }
tr.datarow:hover, tr.datarow:focus { background: var(--paper-2); }
tr.datarow:focus { outline: 2px solid var(--blue); outline-offset: -2px; }
td a { color: var(--ink); text-decoration: none; }
td a:hover { text-decoration: underline; }
.tbl-wrap { overflow-x: auto; max-width: 100%; }
.pill { display: inline-block; padding: 1px 9px; border-radius: 999px; font-size: 12px; font-weight: 500; line-height: 1.6; background: var(--quiet-bg); color: var(--quiet-fg); }
.pill-auto { background: var(--ice-100); color: var(--tag-fg); }
.pill-rule { background: var(--ice-200); color: var(--count-fg); }
.parsed { margin-left: 4px; padding: 0 5px; border-radius: 6px; font-size: 11px; font-weight: 500; color: var(--muted); box-shadow: inset 0 0 0 1px var(--line-strong); cursor: help; }
.rm-detail { display: block; margin-top: 2px; font-size: 12.5px; color: var(--muted); }
.morebtn { margin: 10px 12px 8px; }

/* bars (people, categories, accounts) */
.bars { display: grid; gap: 2px; }
.bar-row { display: grid; grid-template-columns: 220px minmax(0, 1fr) 60px; gap: 12px; align-items: center; width: 100%; padding: 6px 8px; border: 0; border-radius: 10px; background: transparent; color: inherit; font: inherit; text-align: left; cursor: pointer; }
.bar-row:hover { background: var(--paper-2); }
.bar-row[aria-pressed="true"] { background: var(--ice-100); }
.bar-label { display: block; min-width: 0; font-size: 14px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--text); }
.bar-track { display: block; height: 8px; border-radius: 999px; background: var(--quiet-bg); overflow: hidden; }
.bar-fill { display: block; height: 100%; border-radius: 999px; background: var(--bar); }
.bar-n { display: block; text-align: right; font-size: 13px; font-weight: 500; color: var(--muted); font-variant-numeric: tabular-nums; }
.grp-label { margin: 0 0 6px; font-size: 13px; font-weight: 600; color: var(--ink); }
.grp-label ~ .grp-label { margin-top: 16px; }
.acc-block { padding: 6px 0; border-bottom: 1px solid var(--line); }
.acc-block:last-child { border-bottom: 0; }
.acc-head { display: grid; grid-template-columns: 16px minmax(0, 200px) minmax(0, 1fr) 50px; align-items: center; gap: 10px; width: 100%; padding: 6px 8px; border: 0; border-radius: 10px; background: transparent; color: inherit; font: inherit; font-weight: 600; text-align: left; cursor: pointer; }
.acc-head .chev { font-size: 11px; color: var(--muted); transition: transform .12s; }
.acc-head[aria-expanded="true"] .chev { transform: rotate(90deg); }
.camp-list { margin-top: 4px; padding-left: 22px; }

/* timeline */
.tl-scroll { position: relative; overflow-x: auto; padding-top: 30px; }
.timeline { display: flex; align-items: flex-end; gap: 3px; height: 110px; padding: 0 0 4px; width: max-content; min-width: 100%; }
.tl-bar { position: relative; min-width: 8px; flex: 1 0 8px; padding: 0; border: 0; border-radius: 6px 6px 2px 2px; background: var(--bar); font: inherit; color: inherit; cursor: pointer; }
.tl-slot { min-width: 8px; flex: 1 0 8px; }
.tl-bar.edge { background: var(--bar-soft); }
.tl-bar:hover, .tl-bar:focus { background: var(--bar-on); }
.tl-bar[aria-pressed="true"] { background: var(--bar-on); outline: 2px solid var(--ink); outline-offset: 1px; }
.tl-bar:focus-visible { outline: 2px solid var(--ink); outline-offset: 2px; }
.tl-bar .tip { display: none; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%); margin-bottom: 6px; padding: 4px 8px; border-radius: 8px; background: var(--ink); color: var(--paper); font-size: 12px; white-space: nowrap; z-index: 5; }
.tl-bar:hover .tip, .tl-bar:focus .tip { display: block; }
#tlMetric { display: block; height: 44px; margin-top: 4px; }
#tlMetric .ml { fill: none; stroke: var(--muted); stroke-width: 1.5; }
#tlMetric .mk { fill: var(--card); stroke: var(--ink); stroke-width: 1.5; }
.tl-axis { display: flex; justify-content: space-between; gap: 12px; margin-top: 6px; font: 500 11.5px var(--mono); color: var(--muted); }
.fgroup-more { margin-top: 6px; }
.grp-none { margin: 0 0 4px 8px; }
#timelineReadout { min-height: 1.5em; margin-top: 4px; font-size: 13px; color: var(--muted); }

/* page 3: search, chips, filters */
.p3top { padding: 56px 0 0; }
.searchrow { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; }
.bigsearch { flex: 1 1 320px; min-width: 0; height: 52px; font: 15px var(--sans); padding: 0 20px; border-radius: 999px; border: 0; box-shadow: var(--float); background: var(--card); color: var(--ink); }
.bigsearch::placeholder { color: var(--muted); opacity: 1; }
.bigsearch:focus { outline: 2px solid var(--blue); outline-offset: 2px; }
.ms { position: relative; min-width: 0; }
.ms > summary { list-style: none; cursor: pointer; }
.ms > summary::-webkit-details-marker { display: none; }
.chipms > summary, .chipsel, .chipbtn { display: inline-flex; align-items: center; gap: 6px; height: 44px; padding: 0 16px; border: 0; border-radius: 999px; background: var(--card); box-shadow: var(--ring); color: var(--text); font: 500 13.5px var(--sans); white-space: nowrap; max-width: 280px; overflow: hidden; text-overflow: ellipsis; cursor: pointer; }
.chipms > summary::after, .chipbtn::after { content: "\25BE"; color: var(--muted); font-size: 11px; }
.chipbtn[aria-expanded="true"]::after { content: "\25B4"; }
.chipms > summary .fld-l { font: inherit; color: inherit; }
.chipms > summary .ms-val:not(:empty)::before { content: ": "; }
.chipms > summary:not(.has-sel) .ms-val { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); }
.chipms > summary.has-sel, .chipsel.has-sel, .chipbtn.has-sel { box-shadow: inset 0 0 0 2px var(--blue); color: var(--ink); }
.chipsel select { height: 40px; border: 0; background: transparent; color: var(--ink); font: 500 13.5px var(--sans); max-width: 200px; cursor: pointer; }
.chipsel .fld-l { color: var(--muted); font: 500 13.5px var(--sans); }
.adv { margin-top: 14px; padding: 16px 18px; border-radius: 18px; background: var(--card); box-shadow: var(--ring); }
.filters { display: flex; flex-wrap: wrap; gap: 12px 10px; align-items: flex-end; }
.fld { display: flex; flex-direction: column; gap: 4px; min-width: 0; position: relative; }
.fld-l { font-size: 12px; font-weight: 500; color: var(--muted); }
.box, .adv .ms > summary, .filters select { height: 36px; padding: 0 12px; border: 0; border-radius: 10px; background: var(--card); color: var(--text); box-shadow: inset 0 0 0 1px var(--line-strong); font: 400 14px/1 var(--sans); }
.adv .ms > summary { display: flex; align-items: center; gap: 6px; min-width: 150px; max-width: 240px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.adv .ms > summary::after { content: "\25BE"; margin-left: auto; color: var(--muted); }
.adv .ms > summary.has-sel { box-shadow: inset 0 0 0 2px var(--blue); }
.filters select { min-width: 130px; }
.filters select:focus, .box:focus { outline: none; box-shadow: inset 0 0 0 2px var(--blue); }
.filters select option, .chipsel select option { color: #141511; background: #fff; }
.ms-panel { position: absolute; z-index: 40; top: calc(100% + 6px); left: 0; width: max-content; min-width: 220px; max-width: min(360px, calc(100vw - 32px)); max-height: 320px; overflow: auto; padding: 8px; border-radius: 14px; background: var(--card); box-shadow: var(--float); }
.ms-opt { display: flex; align-items: center; gap: 8px; min-height: 32px; padding: 2px 6px; border-radius: 8px; font-size: 13.5px; cursor: pointer; overflow-wrap: anywhere; }
.ms-opt > span { min-width: 0; }
.ms-opt:hover { background: var(--paper-2); }
.ms-clear { margin-bottom: 4px; height: 30px; padding: 0 10px; border: 0; border-radius: 999px; background: var(--paper-2); color: var(--ink); font: 500 12.5px/1 var(--sans); cursor: pointer; }
.rangebtns, .threshbtns { display: inline-flex; flex-wrap: wrap; gap: 4px; padding: 4px; border-radius: 999px; background: var(--card); box-shadow: var(--ring); }
.rangebtns button, .threshbtns button { height: 30px; padding: 0 14px; border: 0; border-radius: 999px; background: transparent; color: var(--muted); font: 500 13px/1 var(--sans); cursor: pointer; }
.rangebtns button:hover, .threshbtns button:hover { background: var(--paper-2); color: var(--ink); }
.rangebtns button.active, .threshbtns button.active { background: var(--tab-on-bg); color: var(--tab-on-fg); }
.clearbtn, .pager button, .plainbtn { height: 36px; padding: 0 16px; border: 0; border-radius: 999px; background: var(--card); color: var(--ink); box-shadow: inset 0 0 0 1px var(--line-strong); font: 500 13.5px/1 var(--sans); cursor: pointer; }
.clearbtn:hover, .plainbtn:hover { box-shadow: inset 0 0 0 2px var(--blue); }
#filterContext { margin-top: 14px; min-height: 1em; font-size: 13px; color: var(--muted); overflow-wrap: anywhere; }
.chip-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.chip-row:not(:empty) { margin: 10px 0 0; }
.chip { display: inline-flex; align-items: center; gap: 6px; height: 32px; padding: 0 12px; border: 0; border-radius: 999px; background: var(--ice-100); color: var(--tag-fg); font: 500 13px/1 var(--sans); cursor: pointer; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.chip:hover { box-shadow: inset 0 0 0 2px var(--blue); }
.fsum { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0; margin: 22px 0 18px; background: var(--card); border-radius: 18px; box-shadow: var(--ring); overflow: hidden; }
.fstat { padding: 12px 16px; min-width: 0; border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); }
.fstat:nth-child(4n) { border-right: 0; }
.fstat:nth-last-child(-n+4) { border-bottom: 0; }
.fstat .n { font-size: 15px; font-weight: 600; color: var(--ink); overflow-wrap: anywhere; font-variant-numeric: tabular-nums; }
.fstat .l { font-size: 12px; color: var(--muted); }
.tryown { margin: 0 0 18px; border-radius: 18px; background: var(--card); box-shadow: var(--ring); }
.tryown > summary { list-style: none; cursor: pointer; padding: 14px 18px; font: 500 14px var(--sans); color: var(--ink); display: flex; align-items: center; gap: 8px; }
.tryown > summary::-webkit-details-marker { display: none; }
.tryown > summary::before { content: "\25B8"; color: var(--muted); font-size: 12px; transition: transform .12s; }
.tryown[open] > summary::before { transform: rotate(90deg); }
.tryown-b { padding: 0 18px 18px; }
.cfg-label { margin: 10px 0 6px; font-size: 13px; font-weight: 600; color: var(--ink); }
.cfg-field { display: flex; flex-direction: column; gap: 6px; font-size: 13px; color: var(--muted); }
.cfg-err { min-height: 1.2em; font-size: 12.5px; color: var(--ink); }
.cfg-check { display: flex; align-items: center; gap: 7px; cursor: pointer; font-size: 13.5px; min-height: 36px; }
input.ctl { width: 80px; height: 32px; padding: 0 10px; border: 0; border-radius: 10px; background: var(--card); color: var(--text); box-shadow: inset 0 0 0 1px var(--line-strong); }
input.ctl[aria-invalid="true"] { box-shadow: inset 0 0 0 2px var(--ink); }
.explorer-fld { margin: 0 0 12px; align-items: center; }
.pager { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin: 12px 0 64px; font-size: 13px; color: var(--muted); }
.pager button:disabled { opacity: .4; cursor: default; }
.ba { margin-top: 18px; }
.ba h4 { margin: 0 0 6px; font-size: 15px; font-weight: 600; color: var(--ink); }
.ba th, .ba td { max-width: none; }
.ba ul, .cov-list { margin: 6px 0 0 18px; padding: 0; font-size: 13.5px; }
.ba li, .cov-list li { margin: 4px 0; }
.link-btn { padding: 0; border: 0; background: none; color: var(--link); font: inherit; text-align: left; text-decoration: underline; cursor: pointer; overflow-wrap: anywhere; }
.note { margin-top: 8px; font-size: 13px; color: var(--muted); }
.warn-box { margin: 10px 0 4px; padding: 10px 14px; border-radius: 12px; background: var(--ice-100); color: var(--tag-fg); font-size: 13.5px; }
.empty { padding: 20px 0; text-align: center; color: var(--muted); }
.dq { font-size: 13px; color: var(--text); overflow-wrap: anywhere; }
.limits summary { cursor: pointer; color: var(--link); font-weight: 500; font-size: 13.5px; padding: 6px 0; }
.limits ul { margin: 6px 0 0 18px; padding: 0; font-size: 13px; color: var(--muted); }

/* detail dialog */
.detail-overlay { display: none; position: fixed; inset: 0; z-index: 50; align-items: center; justify-content: center; padding: 16px; background: var(--overlay); backdrop-filter: blur(2px); }
.detail-overlay.open { display: flex; }
.detail-card { width: min(640px, 100%); max-height: 86vh; overflow: auto; padding: 24px; border-radius: 22px; background: var(--card); box-shadow: var(--shadow); }
.detail-card h3 { margin: 0 0 16px; font: 300 26px/1.15 var(--serif); color: var(--ink); }
.detail-card dl { display: grid; grid-template-columns: 140px minmax(0, 1fr); gap: 8px 12px; margin: 0; font-size: 14px; }
.detail-card dt { color: var(--muted); font-size: 13px; }
.detail-card dd { margin: 0; min-width: 0; overflow-wrap: anywhere; }
.detail-card dd.pre { white-space: pre-wrap; font-family: var(--mono); font-size: 12.5px; }
.detail-card .close { float: right; width: 40px; height: 40px; border: 0; border-radius: 999px; background: transparent; color: var(--muted); font-size: 16px; cursor: pointer; }
.detail-card .close:hover { background: var(--paper-2); color: var(--ink); }
.dflags { margin: 0; padding: 0; list-style: none; display: grid; gap: 6px; }

@media (max-width: 860px) {
  .wrap { padding: 0 16px; }
  .hero, .grid2, .layout2 { grid-template-columns: minmax(0, 1fr); }
  .hero { padding-top: 40px; gap: 28px; }
  .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
  h1.headline { font-size: 38px; }
  .meta { justify-self: start; max-width: none; }
  .side { position: static; }
  .side ol { display: grid; grid-template-columns: 1fr 1fr; column-gap: 16px; }
  .layout2 { padding-top: 32px; gap: 24px; }
  .navpill { padding-left: 8px; }
  .brand span:last-child { display: none; }
  .brand { margin-right: 2px; }
  .tab { padding: 8px 10px; font-size: 13px; }
  .tab .n { display: none; }
  .fsum { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .fstat:nth-child(4n) { border-right: 1px solid var(--line); }
  .fstat:nth-child(2n) { border-right: 0; }
  .fstat:nth-last-child(-n+4) { border-bottom: 1px solid var(--line); }
  .fstat:nth-last-child(-n+2) { border-bottom: 0; }
}
@media (max-width: 640px) {
  .stat { padding: 16px; }
  .stat .v { font-size: 34px; }
  h2.h2 { font-size: 26px; }
  .flag { grid-template-columns: 28px minmax(0, 1fr) auto; gap: 10px; padding: 14px; }
  .flag .ex a { flex-direction: column; gap: 0; }
  .bar-row { grid-template-columns: 110px minmax(0, 1fr) 44px; gap: 10px; }
  .acc-head { grid-template-columns: 16px minmax(0, 110px) minmax(0, 1fr) 44px; }
  .fld { flex: 1 1 140px; }
  .fld-range { flex-basis: 100%; }
  .rangebtns, .threshbtns { border-radius: 22px; }
  .adv .ms > summary { max-width: none; min-width: 0; }
  .ms-panel { position: static; width: 100%; min-width: 0; max-width: none; margin-top: 4px; }
  .chipms, .chipsel { flex: 1 1 auto; }
  .chipms > summary, .chipsel { max-width: none; width: 100%; }
  .filters select { min-width: 0; width: 100%; }
  th, td { max-width: 160px; }
  .detail-card { padding: 18px; }
  .detail-card dl { grid-template-columns: minmax(0, 1fr); gap: 2px; }
  .detail-card dd { margin-bottom: 8px; }
  .flist a { grid-template-columns: minmax(0, 1fr); }
  .p3top { padding-top: 32px; }
}
/* 44px tap targets on phones and touch screens (2026-11-10, C-4/F-11) */
@media (max-width: 640px), (pointer: coarse) {
  .filters select, .box, .adv .ms > summary, .clearbtn, .plainbtn, .rangebtns button, .threshbtns button, .pager button, .detail-card .close, .ms-clear, .tab { min-height: 44px; }
  .ms-opt, .bar-row, .acc-head, .chip { min-height: 44px; }
  .link-btn, .linkish { display: inline-flex; align-items: center; min-height: 44px; }
  .th-btn { min-height: 44px; }
  tr.datarow td { padding-top: 13px; padding-bottom: 13px; }
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { transition: none !important; animation: none !important; scroll-behavior: auto !important; }
}
/* Print: page 1 on one A4 portrait, page 2 as a document, page 3 as the full
   table on landscape. Only the page on screen prints. */
@page { size: A4 portrait; margin: 11mm; }
@page wide { size: A4 landscape; margin: 10mm; }
@media print {
  :root { color-scheme: light; --paper: #fff; --card: #fff; --shadow: 0 0 0 1px #dcdcd4; --ring: 0 0 0 1px #e4e4dc; }
  body { background: #fff; font-size: 10px; }
  .nav, .skip, .side, .fctx, .detail-overlay, .searchrow, .adv, .chip-row, .tryown, .pager button, #baCtl, #tlMetricCtl, .threshbtns, .morebtn, .tl-bar .tip, #timelineReadout { display: none !important; }
  .wrap, .wrap.wide { max-width: none; padding: 0; }
  #p1 .hero { padding: 0 0 8px; gap: 20px; grid-template-columns: 1.4fr 1fr; }
  #p1 h1.headline { font-size: 26px; margin: 6px 0 8px; }
  #p1 .lede { font-size: 11px; }
  #p1 .meta { gap: 4px; }
  #p1 .meta > div { font-size: 9.5px; padding-bottom: 3px; }
  #p1 .stats { margin: 6px 0 12px; gap: 8px; grid-template-columns: repeat(4, minmax(0, 1fr)); }
  #p1 .flag .ex a { flex-direction: row; }
  #p1 .stat { padding: 9px 12px; border-radius: 10px; box-shadow: 0 0 0 1px #dcdcd4; }
  #p1 .stat.focus { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  #p1 .stat .v { font-size: 24px; }
  #p1 .stat .l { margin-top: 4px; font-size: 9.5px; }
  #p1 .grid2 { grid-template-columns: 1.25fr 1fr; gap: 10px; }
  #p1 .sec { margin: 0 0 8px; }
  #p1 .sec-h { margin-bottom: 6px; }
  #p1 .sec-h a { display: none; }
  #p1 h2.h2 { font-size: 17px; }
  #p1 .flags { gap: 5px; }
  #p1 .flag { padding: 7px 10px; gap: 8px; grid-template-columns: 22px minmax(0, 1fr) auto; border-radius: 10px; break-inside: avoid; }
  #p1 .flag h3 { font-size: 10.5px; margin: 0 0 1px; }
  #p1 .flag p { font-size: 9px; }
  #p1 .flag .ex { margin-top: 3px; gap: 1px; }
  #p1 .flag .ex li:nth-child(n+2) { display: none; }
  #p1 .flag .ex a { font-size: 9px; gap: 6px; }
  #p1 .flag .ex time { font-size: 8.5px; min-width: 62px; }
  #p1 .count { font-size: 9.5px; padding: 1px 7px; }
  #p1 .aside { gap: 8px; }
  #p1 .card { padding: 10px 12px; border-radius: 10px; break-inside: avoid; }
  #p1 .card h3 { font-size: 10.5px; }
  #p1 .card .sub { font-size: 9px; margin-bottom: 6px; }
  #p1 .card .more { display: none; }
  #p1 .legend { gap: 2px; margin-top: 6px; font-size: 9.5px; }
  #p1 .wbar { height: 8px; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  #p1 .budget .b { padding: 8px; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  #p1 .budget .v { font-size: 17px; }
  #p1 .budget .l { font-size: 9px; }
  #p1 .stale { gap: 3px; font-size: 9.5px; }
  #p1 .stale li { padding-bottom: 3px; }
  #p1 .stale li:nth-child(n+5) { display: none; }
  #p1 .dpill { font-size: 8.5px; }
  #p1 .p1note { padding: 6px 0 0; font-size: 8.5px; gap: 2px 14px; }
  #p2 .layout2 { display: block; padding: 0; }
  #p2 .p2s { margin: 0 0 14px; break-inside: auto; }
  #p2 h2.h2 { font-size: 18px; break-after: avoid; }
  #p3 { page: wide; }
  #p3 .p3top { padding: 0; }
  #p3 .fsum { margin: 6px 0; }
  .tcard, .card, .fgroup { box-shadow: 0 0 0 1px #dcdcd4; }
  .tl-scroll, .timeline, .tbl-wrap { overflow: visible !important; }
  table { font-size: 8.5px; table-layout: auto; }
  th, td { max-width: none; padding: 3px 5px; white-space: normal; overflow: visible; text-overflow: clip; overflow-wrap: anywhere; vertical-align: top; }
  th { position: static; font-size: 8px; white-space: nowrap; overflow-wrap: normal; word-break: keep-all; hyphens: none; }
  thead { display: table-header-group; }
  tr { break-inside: avoid; }
  .pill, .tag { padding: 0 5px; font-size: 8px; white-space: nowrap; }
  td .note { font-size: inherit; }
}
</style>
</head>
<body>
<a class="skip" href="#main" data-t="skip">Skip to content</a>
<!-- 2026-11-19: three pages behind a floating pill nav; links change the hash, so back/forward work. -->
<nav class="nav" aria-label="Pages" data-ta="aria-label:nav_aria">
  <div class="navpill">
    <span class="brand"><span class="dot" aria-hidden="true">&#8710;</span><span data-t="brand">Change History</span></span>
    <a class="tab" href="#summary" data-page="p1" id="tab1"><span class="n" aria-hidden="true">01</span><span data-t="tab1">Summary</span></a>
    <a class="tab" href="#details" data-page="p2" id="tab2"><span class="n" aria-hidden="true">02</span><span data-t="tab2">Details</span></a>
    <a class="tab" href="#all" data-page="p3" id="tab3"><span class="n" aria-hidden="true">03</span><span data-t="tab3">All changes</span></a>
  </div>
</nav>
<main id="main" tabindex="-1">

<!-- ============ PAGE 1: SUMMARY ============ -->
<div class="page wrap" id="p1">
  <header class="hero">
    <div>
      <div class="eyebrow" id="eyebrow">Google Ads</div>
      <h1 class="headline" id="headline">Ads Change History</h1>
      <p class="lede" id="lede"></p>
    </div>
    <dl class="meta" id="metaList" aria-label="Report details" data-ta="aria-label:meta_aria"></dl>
  </header>
  <section class="stats" id="statCards" aria-label="Key figures" data-ta="aria-label:stats_aria"></section>
  <div class="grid2">
    <section class="sec" aria-labelledby="hFlags1">
      <div class="sec-h"><h2 class="h2" id="hFlags1" data-t="flags_h">Worth a look</h2><a href="#details/flags" id="threshLink" data-t="flags_adjust">Thresholds are adjustable</a></div>
      <div class="flags" id="flagCards"></div>
    </section>
    <div class="aside sec">
      <section class="card" id="whoCard" aria-labelledby="hWho">
        <h3 id="hWho" data-t="who_h">Who made the changes</h3><p class="sub" data-t="who_sub">Where the changes came from</p>
        <div id="whoBody"></div>
      </section>
      <section class="card" id="budgetCard" aria-labelledby="hBudget">
        <h3 id="hBudget" data-t="budget_h">Budget movement</h3><p class="sub" data-t="budget_sub">Total change in daily budgets</p>
        <div id="budgetBody"></div>
      </section>
      <section class="card" id="staleCard" aria-labelledby="hStale">
        <h3 id="hStale" data-t="stale_h">Untouched for a long time</h3><p class="sub" id="staleSub" data-t="stale_sub">Days since the last change</p>
        <div id="staleBody"></div>
      </section>
    </div>
  </div>
  <footer class="p1note" id="p1Notes"></footer>
</div>

<!-- ============ PAGE 2: DETAILS ============ -->
<div class="page wrap" id="p2" hidden>
  <h1 class="sr" id="hP2" data-t="tab2">Details</h1>
  <div class="layout2">
    <nav class="side" aria-label="Sections" data-ta="aria-label:side_aria"><ol id="sideList">
      <li><a href="#details/campaigns" data-sec="s1"><span data-t="s1">Campaigns</span><span class="k"></span></a></li>
      <li><a href="#details/flags" data-sec="s2"><span data-t="s2">Worth a look</span><span class="k"></span></a></li>
      <li><a href="#details/timeline" data-sec="s3"><span data-t="s3">Timeline</span><span class="k"></span></a></li>
      <li><a href="#details/people" data-sec="s4"><span data-t="s4">People</span><span class="k"></span></a></li>
      <li><a href="#details/categories" data-sec="s5"><span data-t="s5">Categories</span><span class="k"></span></a></li>
      <li><a href="#details/before-after" data-sec="s6"><span data-t="s6">Before / after</span><span class="k"></span></a></li>
      <li><a href="#details/coverage" data-sec="s7"><span data-t="s7">Coverage and data</span><span class="k"></span></a></li>
    </ol></nav>
    <div class="p2body">
      <div class="fctx" id="p2Filter" aria-live="polite" hidden><span id="p2FilterText"></span><button type="button" class="linkish" id="p2Clear" data-t="clear_filters">Clear filters</button><a id="p2ToAll" href="#all"></a></div>

      <section class="p2s" id="s1" aria-labelledby="hS1">
        <div class="sec-h"><h2 class="h2" id="hS1" data-t="s1">Campaigns</h2><p data-t="s1_sub">Change summary for each campaign</p></div>
        <p class="caption" id="campCaption" data-t="s1_cap">Whole report period, not affected by filters. Day counts are measured to the last day of the data.</p>
        <div class="tcard"><div class="tbl-wrap" tabindex="0" role="region" aria-label="Campaigns table" data-ta="aria-label:camp_tbl_aria" id="campTableWrap"></div></div>
        <section id="rollupSection" aria-labelledby="hRollup" hidden>
          <h3 class="h3" id="hRollup" data-t="rollup_h">Per-account rollup</h3>
          <p class="caption" data-t="rollup_cap">One row per account under the current filters. Counts only. Select an account name to filter by it.</p>
          <div class="tcard" id="accountRollup"></div>
        </section>
        <h3 class="h3" data-t="acc_h">Account and campaign activity</h3>
        <p class="caption" data-t="acc_cap">Each account expands to the campaigns it contains, ranked by number of changes. Select a campaign to filter this page by it.</p>
        <div class="tcard pad" id="accountActivity"></div>
        <div id="untouchedBlock">
          <h2>Campaign Last Changes</h2>
          <p class="caption" data-t="untouched_cap">Lists only campaigns with at least one change in this export. A campaign that doesn't appear in the export is not listed: it may have had no change in the export's date range, or may not exist; this report can't tell which. "Days since" is counted to the current time in your browser and under the current filters, not to the moment this file was generated.</p>
          <div class="threshbtns" id="threshBtns" role="group" aria-label="Minimum days since last change" data-ta="aria-label:thresh_aria" style="margin-bottom:10px;">
            <button type="button" data-min="0" class="active" aria-pressed="true" data-t="thresh_all">All</button>
            <button type="button" data-min="7" aria-pressed="false" data-t="thresh_7">7+ days</button>
            <button type="button" data-min="14" aria-pressed="false" data-t="thresh_14">14+ days</button>
            <button type="button" data-min="30" aria-pressed="false" data-t="thresh_30">30+ days</button>
            <button type="button" data-min="60" aria-pressed="false" data-t="thresh_60">60+ days</button>
          </div>
          <div id="untouched"></div>
        </div>
      </section>

      <section class="p2s" id="s2" aria-labelledby="hS2">
        <div class="sec-h"><h2 class="h2" id="hS2" data-t="s2">Worth a look</h2><p data-t="s2_sub">Every flag, grouped by the rule that fired</p></div>
        <p class="caption" data-t="s2_cap">A flag is a fact measured against a threshold set for this report. It says the change is worth a look, not what it means. Select a line to see its changes.</p>
        <div id="flagGroups"></div>
        <h3 class="h3" id="hThresh" data-t="thr_h">Thresholds used</h3>
        <p class="caption" data-t="thr_cap">The results above use the settings this report was built with. They can be changed with a rules file when the report is rebuilt; to experiment right away, use "Try your own thresholds" on All changes.</p>
        <div class="tcard"><div class="tbl-wrap" tabindex="0" role="region" aria-label="Thresholds table" data-ta="aria-label:thr_tbl_aria" id="threshTable"></div></div>
      </section>

      <section class="p2s" id="s3" aria-labelledby="hS3">
        <div class="sec-h"><h2 class="h2" id="hS3" data-t="s3">Timeline</h2><p id="timelineBasis"></p></div>
        <p class="caption" data-t="tl_cap">Bar height is relative to the busiest day shown. Hover, tap or tab to a bar for its exact date and count; select a bar to show only that day.</p>
        <div class="tcard pad">
          <div class="tl-scroll" id="tlScroll">
            <div class="timeline" id="timeline" role="group" aria-label="Changes per day" data-ta="aria-label:tl_aria"></div>
            <svg id="tlMetric" role="img" aria-label="" style="display:none;" xmlns="http://www.w3.org/2000/svg"></svg>
          </div>
          <div class="tl-axis" id="tlAxis" aria-hidden="true"></div>
          <div id="timelineNote"></div>
          <div id="timelineReadout" aria-live="polite"></div>
          <div class="chip-row day-chip"></div>
          <div class="filters" id="tlMetricCtl" hidden style="margin-top:10px;"><label class="fld"><span class="fld-l" data-t="metric_line">Metrics line</span><select id="fMetricLine"></select></label><span class="note" id="tlMetricNote"></span></div>
        </div>
      </section>

      <section class="p2s" id="s4" aria-labelledby="hS4">
        <div class="sec-h"><h2 class="h2" id="hS4" data-t="s4">People</h2><p data-t="s4_sub">Who made the changes</p></div>
        <p class="caption" data-t="users_cap">Split by actor type: human (named user), automation (script, rule, recommendation) or unknown (no identity in the source data). Select a bar to filter by it.</p>
        <div class="tcard pad" id="userActivity"></div>
      </section>

      <section class="p2s" id="s5" aria-labelledby="hS5">
        <div class="sec-h"><h2 class="h2" id="hS5" data-t="s5">Categories</h2><p data-t="s5_sub">What kind of change</p></div>
        <p class="caption" data-t="cat_cap">Categories come from rule matches against resource type, field, operation and summary text. "Other" means no rule matched yet; see coverage.txt.</p>
        <div class="tcard pad" id="categoryDist"></div>
      </section>

      <section class="p2s" id="s6" aria-labelledby="hS6" hidden>
        <div class="sec-h"><h2 class="h2" id="hS6" data-t="s6">Before / after</h2><p data-t="s6_sub">Metrics around each change</p></div>
        <p class="caption" id="ba2Note"></p>
        <div class="filters" style="margin-bottom:12px;"><label class="fld"><span class="fld-l" data-t="ba_metric_l">Before/After metric</span><select id="fBaMetric2"></select></label></div>
        <div class="tcard" id="ba2Table"></div>
      </section>

      <section class="p2s" id="s7" aria-labelledby="hS7">
        <div class="sec-h"><h2 class="h2" id="hS7" data-t="s7">Coverage and data</h2><p data-t="s7_sub">What this report read and what it can show</p></div>
        <div class="tcard pad">
          <div class="dq" id="metaLine"></div>
          <div class="dq note" id="dataQuality"></div>
          <div id="apiCaveat"></div>
          <section id="coverageSection" aria-labelledby="hCoverage" hidden>
            <h3 class="h3" id="hCoverage" data-t="cov_h">Coverage notes</h3>
            <p class="caption" data-t="cov_cap">What this input can and can't show, as stated for this run.</p>
            <ul class="cov-list" id="coverageList"></ul>
            <div class="note" id="metricsNote"></div>
          </section>
          <div id="inputFiles"></div>
          <!-- Permanent, run-independent disclosure: always shown, strictly factual. -->
          <details class="limits" style="margin-top:12px;">
            <summary data-t="limits_h">What this report can't do</summary>
            <ul>
              <li data-t="limits_1">Only knows what's in the input file. It doesn't call the Google Ads API live and doesn't know about changes made after the export or outside its date range.</li>
              <li data-t="limits_2">A campaign that doesn't appear in the input isn't reported as "unchanged": no row means no evidence either way, including whether it exists.</li>
              <li data-t="limits_3">Reports what changed and when, never whether a change was good, risky, or should have happened differently. Flags only say a change crossed a threshold set for this report.</li>
              <li data-t="limits_4">If the input includes Google Ads ChangeEvent API data: that API keeps only 30 days of history, caps at 10,000 rows per query, can lag up to about 3 minutes behind a very recent change, and may not include every entry the Google Ads UI's own Change History page shows.</li>
            </ul>
          </details>
        </div>
      </section>
    </div>
  </div>
</div>

<!-- ============ PAGE 3: ALL CHANGES ============ -->
<div class="page wrap wide" id="p3" hidden>
  <h1 class="sr" id="hP3" data-t="tab3">All changes</h1>
  <section class="p3top" id="filtersSection" aria-labelledby="hFilters">
    <h2 class="sr" id="hFilters" data-t="filters_h">Filters</h2>
    <!-- 2026-11-10 (C-2/F-12): every control has a visible label; multi-selects are checkbox popovers -->
    <div class="searchrow">
      <input type="search" class="bigsearch" id="fSearch" aria-label="Search changes" data-ta="aria-label:search_aria;placeholder:search_ph" placeholder="Campaign, user, change text…">
      <details class="ms chipms" id="fCampaign" data-state="campaigns" data-all="All campaigns"><summary aria-labelledby="lCampaign fCampaignSum"><span class="fld-l" id="lCampaign" data-t="f_campaign">Campaign</span><span class="ms-val" id="fCampaignSum"></span></summary><div class="ms-panel" role="group" aria-labelledby="lCampaign"></div></details>
      <details class="ms chipms" id="fUser" data-state="users" data-all="All users"><summary aria-labelledby="lUser fUserSum"><span class="fld-l" id="lUser" data-t="f_user">User</span><span class="ms-val" id="fUserSum"></span></summary><div class="ms-panel" role="group" aria-labelledby="lUser"></div></details>
      <details class="ms chipms" id="fCategory" data-state="categories" data-all="All categories"><summary aria-labelledby="lCategory fCategorySum"><span class="fld-l" id="lCategory" data-t="f_category">Category</span><span class="ms-val" id="fCategorySum"></span></summary><div class="ms-panel" role="group" aria-labelledby="lCategory"></div></details>
      <label class="chipsel" id="fldFlag" hidden><span class="fld-l" data-t="f_flag">Worth a look</span><select id="fFlag"></select></label>
      <button type="button" class="chipbtn" id="moreFilters" aria-expanded="false" aria-controls="advFilters"><span data-t="more_filters">More filters</span><span id="moreCount"></span></button>
    </div>
    <div class="adv" id="advFilters" hidden>
      <div class="filters">
        <div class="fld"><span class="fld-l" id="lAccount" data-t="f_account">Account</span><details class="ms" id="fAccount" data-state="accounts" data-all="All accounts"><summary aria-labelledby="lAccount fAccountSum"><span id="fAccountSum">All accounts</span></summary><div class="ms-panel" role="group" aria-labelledby="lAccount"></div></details></div>
        <label class="fld"><span class="fld-l" data-t="f_actor">Actor type</span>
          <select id="fActor">
            <option value="" data-t="actor_all">All actors</option>
            <option value="human" data-t="actor_human">Human</option>
            <option value="automation" data-t="actor_auto">Automation</option>
            <option value="unknown" data-t="actor_unknown">Unknown</option>
          </select></label>
        <div class="fld"><span class="fld-l" id="lOperation" data-t="f_operation">Operation</span><details class="ms" id="fOperation" data-state="operations" data-all="All types"><summary aria-labelledby="lOperation fOperationSum"><span id="fOperationSum">All types</span></summary><div class="ms-panel" role="group" aria-labelledby="lOperation"></div></details></div>
        <div class="fld"><span class="fld-l" id="lSource" data-t="f_source">Source</span><details class="ms" id="fSource" data-state="sources" data-all="All sources"><summary aria-labelledby="lSource fSourceSum"><span id="fSourceSum">All sources</span></summary><div class="ms-panel" role="group" aria-labelledby="lSource"></div></details></div>
        <div class="fld" id="fldSourceFile" hidden><span class="fld-l" id="lSourceFile" data-t="f_file">Source file</span><details class="ms" id="fSourceFile" data-state="sourceFiles" data-all="All files"><summary aria-labelledby="lSourceFile fSourceFileSum"><span id="fSourceFileSum">All files</span></summary><div class="ms-panel" role="group" aria-labelledby="lSourceFile"></div></details></div>
        <label class="fld"><span class="fld-l" data-t="f_rule">Rule match</span>
          <select id="fRuleMatch">
            <option value="" data-t="rm_all">All changes</option>
            <option value="__matched__" data-t="rm_any">Matched a rule</option>
          </select></label>
        <div class="fld fld-range"><span class="fld-l" id="lRange" data-t="f_range">Date range</span>
          <div class="rangebtns" id="rangeBtns" role="group" aria-labelledby="lRange">
            <button type="button" data-range="all" class="active" aria-pressed="true" data-t="range_all">All</button>
            <button type="button" data-range="30" aria-pressed="false" data-t="range_30">30d</button>
            <button type="button" data-range="7" aria-pressed="false" data-t="range_7">7d</button>
            <button type="button" data-range="1" aria-pressed="false" data-t="range_1">Last 24h</button>
          </div></div>
        <label class="fld"><span class="fld-l" data-t="f_anchor">Range ends at</span>
          <select id="fAnchor">
            <option value="data" data-t="anchor_data">Newest change in the data</option>
            <option value="today" data-t="anchor_today">Today</option>
          </select></label>
        <div class="fld"><span class="fld-l" aria-hidden="true">&nbsp;</span><button type="button" class="clearbtn" id="clearFilters" data-t="clear_filters">Clear filters</button></div>
      </div>
    </div>
    <!-- A stated filter-context line, so a screenshot mid-filter is self-explanatory. -->
    <div id="filterContext" aria-live="polite"></div>
    <div class="chip-row day-chip"></div>
    <div class="chip-row" id="flagChip"></div>
  </section>
  <section id="summarySection" aria-labelledby="hSummary">
    <h2 class="sr" id="hSummary" data-t="sum_h">Summary</h2>
    <div class="fsum" id="summaryGrid"></div>
  </section>
  <details class="tryown" id="ruleMatchesPanel">
    <summary data-t="try_h">Try your own thresholds</summary>
    <div class="tryown-b">
      <p class="caption" style="margin:0 0 10px;" data-t="try_cap">A match means the change crossed a value you set here. Every match shows the exact number and the rule next to it. Off by default; the report's own flags are not changed by this.</p>
      <label class="cfg-check"><input type="checkbox" id="ruleMatchesToggle"> <span data-t="try_toggle">Rule-based change highlighting</span></label>
      <div id="ruleMatchesConfig">
        <div class="cfg-label" data-t="try_mag">Magnitude (±% change)</div>
        <div id="ruleMatchesMagnitudeInputs" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-bottom:12px;"></div>
        <div class="cfg-label" data-t="try_struct">Structural</div>
        <div id="ruleMatchesStateInputs" style="display:flex;flex-wrap:wrap;gap:14px;"></div>
      </div>
      <div class="note" id="ruleMatchesCount"></div>
      <div id="ruleMatches"></div>
    </div>
  </details>
  <section id="explorerSection" aria-labelledby="hExplorer">
    <h2 class="sr" id="hExplorer" data-t="exp_h">Change Explorer</h2>
    <p class="caption" id="explorerCaption" data-t="exp_cap">Select any row (click, or Tab then Enter) for full before/after detail. Select a column header to sort by it.</p>
    <div class="filters explorer-fld" id="baCtl" hidden>
      <label class="cfg-check"><input type="checkbox" id="baColToggle"> <span data-t="ba_col">Before/After column</span></label>
      <label class="fld"><span class="fld-l" data-t="ba_metric_l">Before/After metric</span><select id="fBaMetric"></select></label>
      <span class="note" id="baLegend" data-t="ba_legend">Column shows: before | after | difference | % difference. * = the after window includes days still maturing (conversion lag).</span>
    </div>
    <div class="tcard"><div class="tbl-wrap" tabindex="0" role="region" aria-label="Change Explorer table" data-ta="aria-label:exp_tbl_aria">
    <table id="explorerTable">
      <thead><tr>
        <th data-key="timestamp_iso" aria-sort="descending"><button type="button" class="th-btn" data-t="c_date">Date</button></th><th data-key="user_name"><button type="button" class="th-btn" data-t="c_user">User</button></th><th data-key="account_name" data-col="account"><button type="button" class="th-btn" data-t="c_account">Account</button></th>
        <th data-key="campaign_name"><button type="button" class="th-btn" data-t="c_campaign">Campaign</button></th><th data-key="ad_group_name"><button type="button" class="th-btn" data-t="c_adgroup">Ad Group</button></th><th data-key="category"><button type="button" class="th-btn" data-t="c_category">Category</button></th>
        <th data-key="field_name"><button type="button" class="th-btn" data-t="c_change">Change</button></th><th data-key="raw_summary"><button type="button" class="th-btn" data-t="c_summary">Summary</button></th><th data-key="old_value"><button type="button" class="th-btn" data-t="c_old">Old Value</button></th><th data-key="new_value"><button type="button" class="th-btn" data-t="c_new">New Value</button></th><th data-col="flags" data-t="c_flags">Worth a look</th><th data-col="matches" data-t="c_matches">Matches</th><th id="baTh" class="num" hidden data-t="c_ba">Before/After</th><th data-key="source" data-col="source"><button type="button" class="th-btn" data-t="c_source">Source</button></th>
      </tr></thead>
      <tbody id="explorerBody"></tbody>
    </table>
    </div></div>
    <div class="pager">
      <button type="button" id="pagerPrev" data-t="prev">Prev</button><span id="pagerInfo" aria-live="polite"></span><button type="button" id="pagerNext" data-t="next">Next</button>
    </div>
  </section>
</div>
</main>
<div class="detail-overlay" id="detailOverlay">
  <div class="detail-card" id="detailCard" role="dialog" aria-modal="true" aria-labelledby="detailTitle" tabindex="-1">
    <button type="button" class="close" id="detailClose" aria-label="Close detail" data-ta="aria-label:close_aria">&#10005;</button>
    <h3 id="detailTitle" data-t="d_title">Change Detail</h3>
    <dl id="detailBody"></dl>
    <div id="detailFlags"></div>
    <div id="detailBA"></div>
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
// ---- i18n (2026-11-19): every UI string comes from T, picked by DASH_DATA.ui_lang
// (a report without it: Turkish when its rows are Turkish, else English).
const T = {
en: {
  brand:'Change History', skip:'Skip to content', nav_aria:'Pages', tab1:'Summary', tab2:'Details', tab3:'All changes',
  meta_aria:'Report details', stats_aria:'Key figures', side_aria:'Sections',
  headline_fb_one:'{n} change in this period.', headline_fb:'{n} changes in this period.',
  lede_fb:'Campaigns changed: {c}. People who made changes: {p}. Automation changes: {a}.',
  m_period:'Period', m_source:'Source', m_tz:'Time zone', m_generated:'Generated',
  n_files_one:'{n} file', n_files:'{n} files', n_events_one:'{n} event', n_events:'{n} events',
  n_accounts_one:'{n} account', n_accounts:'{n} accounts', tz_unknown:'Unknown',
  st_changes:'Changes', st_campaigns:'Campaigns changed', st_people:'People · {a} automation', st_attention:'Worth a look', st_adgroups:'Ad groups changed',
  flags_h:'Worth a look', flags_adjust:'Thresholds are adjustable', flags_sr:'flags',
  flags_nodata:'This report has no rule results; it may have been built with an older version. You can still try your own thresholds on',
  flags_nodata2:'This report has no rule results.', flags_zero:'No rule crossed its threshold in this period.',
  more_n_one:'+{n} more', more_n:'+{n} more', info_h:'For information',
  who_h:'Who made the changes', who_sub:'Where the changes came from', who_seg:'{label} {p}%',
  other_people_one:'{n} other person', other_people:'{n} other people', auto_google:'Automation and Google',
  budget_h:'Budget movement', budget_sub:'Total change in daily budgets', n_increases_one:'{n} increase', n_increases:'{n} increases',
  n_decreases_one:'{n} decrease', n_decreases:'{n} decreases', budget_none:'No daily budget change in this period.', budget_more:'See budget changes',
  stale_h:'Untouched for a long time', stale_sub:'Days since the last change', stale_sub_m:'Days since the last change; some measured against the metrics file',
  n_days_one:'{n} day', n_days:'{n} days', stale_none:'No campaign has gone {d} days or more without a change.', n_more_one:'{n} more', n_more:'{n} more',
  p1_scope:'This report shows only the records in the change history; it does not interpret their effect on performance.', p1_notes_link:'Data notes ({n})',
  s1:'Campaigns', s1_sub:'Change summary for each campaign', s1_cap:'Whole report period, not affected by filters. Day counts are measured to the last day of the data.',
  ct_campaign:'Campaign', ct_account:'Account', ct_changes:'Changes', ct_budget:'Budget changes', ct_last:'Last change', ct_flags:'Worth a look',
  days_ago_0:'Last day', days_ago_one:'{n} day ago', days_ago:'{n} days ago', show_all:'Show all ({n})', show_more_one:'Show {n} more', show_more:'Show {n} more', grp_none:'No changes in this group.', show_less:'Show fewer', camp_tbl_aria:'Campaigns table',
  rollup_h:'Per-account rollup', rollup_cap:'One row per account under the current filters. Counts only. Select an account name to filter by it.',
  ro_aria:'Per-account rollup table', ro_camps:'Campaigns changed', ro_users:'Named users', ro_auto:'Automation changes', ro_first:'First change', ro_files:'Files',
  acc_h:'Account and campaign activity', acc_cap:'Each account expands to the campaigns it contains, ranked by number of changes. Select a campaign to filter this page by it.',
  untouched_h:'Campaign Last Changes',
  untouched_cap:'Lists only campaigns with at least one change in this export. A campaign that doesn\'t appear in the export is not listed: it may have had no change in the export\'s date range, or may not exist; this report can\'t tell which. "Days since" is counted to the current time in your browser and under the current filters, not to the moment this file was generated.',
  thresh_aria:'Minimum days since last change', thresh_all:'All', thresh_7:'7+ days', thresh_14:'14+ days', thresh_30:'30+ days', thresh_60:'60+ days',
  ut_empty:'No campaigns match this threshold (or current filters).', ut_tbl_aria:'Campaign last changes table', ut_days:'Days since (today)',
  s2:'Worth a look', s2_sub:'Every flag, grouped by the rule that fired',
  s2_cap:'A flag is a fact measured against a threshold set for this report. It says the change is worth a look, not what it means. Select a line to see its changes.',
  rule_fired:'Threshold: {p}', n_more_all_one:'{n} more on All changes', n_more_all:'{n} more on All changes',
  thr_h:'Thresholds used', thr_cap:'The results above use the settings this report was built with. They can be changed with a rules file when the report is rebuilt; to experiment right away, use "Try your own thresholds" on All changes.',
  thr_tbl_aria:'Thresholds table', th_rule:'Rule', th_tier:'Level', th_status:'Status', th_threshold:'Threshold', th_hits:'Results',
  tier_attention:'Worth a look', tier_info:'Info', st_on:'On', st_off:'Off', thr_none:'No rule settings in this report.',
  s3:'Timeline', tl_cap:'Bar height is relative to the busiest day shown. Hover, tap or tab to a bar for its exact date and count; select a bar to show only that day.',
  tl_aria:'Changes per day', tl_basis_utc:'UTC days', tl_basis_tz:'Days in {tz}', tl_basis_multi:'Days in each row\'s own time zone', tl_basis:'Days',
  tl_bar_one:'{d}: {n} change', tl_bar:'{d}: {n} changes', tl_excluded_one:'{n} row left out: time zone unknown.', tl_excluded:'{n} rows left out: time zone unknown.',
  tl_empty:'No rows with a known time zone to plot.', day_chip:'Day: {d}', day_chip_aria:'Day: {d}, clear this filter', metric_line:'Metrics line', off:'Off',
  ml_none:'No daily {m} in the metrics file for these days.', ml_aria:'Daily {m} from the metrics file, all metrics rows (not filtered): lowest {lo}, highest {hi}. Dots mark days with at least one change.',
  ml_note:'Grey line: daily {m} (all metrics rows, not filtered), {lo} to {hi}. Dots: days with changes.',
  s4:'People', s4_sub:'Who made the changes', users_cap:'Split by actor type: human (named user), automation (script, rule, recommendation) or unknown (no identity in the source data). Select a bar to filter by it.',
  s5:'Categories', s5_sub:'What kind of change', cat_cap:'Categories come from rule matches against resource type, field, operation and summary text. "Other" means no rule matched yet; see coverage.txt.',
  s6:'Before / after', s6_sub:'Metrics around each change', ba2_none:'No change in view has a before/after window.', ba2_aria:'Before and after table', ba2_first:'Newest {n} of {m} changes with a window.',
  s7:'Coverage and data', s7_sub:'What this report read and what it can show', cov_h:'Coverage notes', cov_cap:'What this input can and can\'t show, as stated for this run.',
  limits_h:'What this report can\'t do',
  limits_1:'Only knows what\'s in the input file. It doesn\'t call the Google Ads API live and doesn\'t know about changes made after the export or outside its date range.',
  limits_2:'A campaign that doesn\'t appear in the input isn\'t reported as "unchanged": no row means no evidence either way, including whether it exists.',
  limits_3:'Reports what changed and when, never whether a change was good, risky, or should have happened differently. Flags only say a change crossed a threshold set for this report.',
  limits_4:'If the input includes Google Ads ChangeEvent API data: that API keeps only 30 days of history, caps at 10,000 rows per query, can lag up to about 3 minutes behind a very recent change, and may not include every entry the Google Ads UI\'s own Change History page shows.',
  if_h:'Input files', if_aria:'Input files table', if_file:'File', if_rows:'Rows', if_accounts:'Accounts', if_first:'First date', if_last:'Last date',
  meta_line:'{n} changes · {a} – {b} · Sources: {src}{files} · Other: {o}%{masked} · Generated {g}', meta_files:' ({n} input files)', meta_masked:' · Users masked',
  cav_api:'Includes data from the Google Ads ChangeEvent API. ChangeEvent may not include every entry from the Google Ads UI\'s Change History, so treat counts from this source as a lower bound, not a complete history.',
  cav_dates:'{n} row(s) ({p}% of input) were left out: their date column didn\'t parse under the format this run resolved. See coverage.txt.',
  cav_tz:'{n} row(s) ({p}%) carry a timestamp with no time zone and are left out of the Timeline; every other view still includes them. Re-run with --timezone <IANA name> if those timestamps share one known zone.',
  dq_line:'Input: {in} rows · in this report: {rep}{split} · left out: {left}{flags}{ign}', dq_flags:' · of the rows shown: {x}', dq_ignored:' · source columns not used: {x}',
  'dq.category_other':'{n} categorized "Other"', 'dq.timezone_unknown':'{n} no time zone', 'dq.operation_inferred':'{n} operation inferred',
  'dq.value_parsed_from_summary':'{n} value read from summary text', 'dq.multi_field_unsplit':'{n} several fields in one row',
  dq_split_stmt:'{n} added by splitting multi-statement cells', dq_split_field:'{n} added by splitting multi-field rows',
  dq_unreadable:'{n} unreadable date', dq_duplicate:'{n} duplicate', dq_dup_files:'{n} duplicate across files',
  dq_ba:'{a} with a before/after metrics window, {b} without matching metrics', dq_ba_unmatched:', {c} campaign(s) not in the metrics files',
  dq_dup_kept:'{n} identical rows kept (possible duplicates)', dq_camp_inferred:'{n} campaign read from summary text', dq_dst:'{n} in a daylight-saving gap or repeated hour',
  filters_h:'Filters', search_aria:'Search changes', search_ph:'Campaign, user, change text…', more_filters:'More filters',
  f_campaign:'Campaign', f_user:'User', f_category:'Category', f_flag:'Worth a look', f_account:'Account', f_actor:'Actor type', f_operation:'Operation',
  f_source:'Source', f_file:'Source file', f_rule:'Rule match', f_range:'Date range', f_anchor:'Range ends at',
  actor_all:'All actors', actor_human:'Human', actor_auto:'Automation', actor_unknown:'Unknown',
  rm_all:'All changes', rm_any:'Matched a rule', range_all:'All', range_30:'30d', range_7:'7d', range_1:'Last 24h',
  anchor_data:'Newest change in the data', anchor_today:'Today', clear_filters:'Clear filters',
  all_accounts:'All accounts', all_campaigns:'All campaigns', all_users:'All users', all_categories:'All categories', all_types:'All types', all_sources:'All sources', all_files:'All files',
  n_selected:'{n} selected', ms_clear:'Clear selection', flag_all:'All changes', flag_any:'Any flag',
  flag_chip:'Flag: {x}', flag_chip_aria:'Flag: {x}, clear this filter',
  many_accounts:'{n} accounts', many_campaigns:'{n} campaigns', many_users:'{n} users', many_categories:'{n} categories', many_operations:'{n} operation types', many_sources:'{n} sources', many_files:'{n} files',
  span_24h:'24 hours', span_days:'{n} days', range_today:'Last {span} to today ({d})', range_data:'Last {span} of data, to {d}',
  ctx_search:'Search: "{q}"', showing:'Showing: {x}', showing_all:'Showing: all changes, no filters applied', open_all:'Open in All changes ({n})',
  sum_h:'Summary', fs_changes:'Changes', fs_accounts:'Accounts', fs_users:'People', fs_campaigns:'Campaigns changed', fs_adgroups:'Ad groups changed',
  fs_last:'Last change', fs_top_user:'Most active user', fs_top_campaign:'Most active campaign',
  try_h:'Try your own thresholds', try_cap:'A match means the change crossed a value you set here. Every match shows the exact number and the rule next to it. Off by default; the report\'s own flags are not changed by this.',
  try_toggle:'Rule-based change highlighting', try_mag:'Magnitude (±% change)', try_struct:'Structural',
  rm_off:'Rule-based change highlighting is off.', rm_count:'{a} of {b} changes in view match a rule', rm_none:'No changes match the current rules.',
  rm_tbl_aria:'Rule matches table', rm_oldnew:'Old → New', rm_matched:'Matched rule(s)', rm_first:'Showing first {n} of {m} matches (still counted correctly above); narrow with the filters to see the rest.',
  rm_detail_mag:'{sign}{pct}% · rule: ≥±{th}%', rm_detail_state:'{fact} · rule: {label}', rm_op:'Operation: {op}', rm_status_to:'status → {s}',
  'rm.budget_magnitude':'Budget change', 'rm.target_cpa_magnitude':'Target CPA change', 'rm.target_roas_magnitude':'Target ROAS change', 'rm.bid_magnitude':'Bid/CPC change',
  'rm.campaign_paused':'Campaign paused', 'rm.campaign_removed':'Campaign removed', 'rm.ad_group_removed':'Ad group removed', 'rm.campaign_enabled':'Campaign enabled',
  thr_enter:'Enter a number', thr_nan:'Not a number', thr_gt0:'Must be greater than 0', thr_max:'At most {max}', thr_err:'{msg} (1–{max}). Still using {v}%.',
  exp_h:'Change Explorer', exp_cap:'Select any row (click, or Tab then Enter) for full before/after detail. Select a column header to sort by it.',
  exp_cap_split:'1 row = 1 change statement: a source cell that held several statements is shown as one row per statement (see the Statement line in the detail).',
  exp_tbl_aria:'Change Explorer table', exp_empty:'No changes match current filters.', pager:'Page {p} / {n} · {r} rows', prev:'Prev', next:'Next',
  ba_col:'Before/After column', ba_metric_l:'Before/After metric', ba_legend:'Column shows: before | after | difference | % difference. * = the after window includes days still maturing (conversion lag).',
  ba_cell_tip:'before | after | difference | % difference',
  c_date:'Date', c_user:'User', c_account:'Account', c_campaign:'Campaign', c_adgroup:'Ad Group', c_category:'Category', c_change:'Change', c_summary:'Summary',
  c_old:'Old Value', c_new:'New Value', c_flags:'Worth a look', c_matches:'Matches', c_ba:'Before/After', c_source:'Source',
  automation_lc:'automation', unknown_pill:'unknown', camp_inferred_tip:'Campaign name read from the summary text',
  raw:'(raw: {v})', parsed:'parsed', parsed_tip:'Read from the change summary text, not from a structured field',
  actor_auto_label:'{name} (automation)', actor_unknown_label:'{name} (actor type unknown)', actor_unknown_none:'Unknown (no identity in source)',
  account_n:'Account {id}', campaign_n:'Campaign {id}', no_account:'(no account)', no_campaign:'(no campaign)', no_file:'(no file)', no_data:'No data for current filters.',
  ba_h:'Before / after', ba_nomatch:'No metrics rows matched this change\'s account and campaign (or ad group).',
  lvl_campaign:'Campaign', lvl_adgroup:'Ad group', lvl_campaign_lc:'campaign', lvl_adgroup_lc:'ad group',
  ba_cap:'Before: the {n} full days before the change\'s day ({bd} with data). After: the change\'s day and the next {n1} days ({ad} with data){mat}. Sums from the metrics file, shown as plain numbers.',
  ba_mat:'; {m} of them still maturing (conversion lag), so their figures can still change', ba_tbl_aria:'Before and after metrics',
  ba_metric:'Metric', ba_before:'Before', ba_after:'After', ba_diff:'Difference', ba_pct:'% difference', ba_star:'* includes maturing days.',
  ba_overlap_one:'{n} other change on the same {lvl} inside these windows{first}:', ba_overlap:'{n} other changes on the same {lvl} inside these windows{first}:',
  ba_first:' (first {k} listed)', ba_notin:'{id} (not in this report)',
  mn_files:'Metrics: {f}', mn_file:'metrics file', mn_level:'level: {x}', mn_window:'window: {n} days', mn_lag:'conversion lag: {n} days', mn_ignored:'columns not used: {x}',
  m_impressions:'Impressions', m_clicks:'Clicks', m_cost:'Cost', m_conversions:'Conversions', m_conversion_value:'Conv. value',
  d_title:'Change Detail', close_aria:'Close detail', d_flags:'Worth a look',
  d_date:'Date', d_user:'User', d_account:'Account', d_campaign:'Campaign', d_adgroup:'Ad Group', d_category:'Category', d_field:'Field', d_previous:'Previous', d_new:'New',
  d_operation:'Operation', d_source:'Source', d_valuefrom:'Value read from', d_summary:'Summary', d_statement:'Statement', d_fullcell:'Full source cell',
  d_resource:'Resource', d_eventid:'Event ID', d_file:'Source file',
  d_tz_unknown:' (timezone unknown)', d_camp_inferred:' (read from the summary text)', d_no_rule:' (no rule matched)', d_op_inferred:'{op} (inferred, not stated by source)',
  d_val_none:'no value in the source', d_val_parsed:'summary text (parsed, not a structured field)', d_val_struct:'structured field',
  d_stmt:'{pos} of {n} change statements from one source cell', d_derived:'{id} (derived, not from source)', d_parsed_suffix:' (parsed from summary text)',
  'cov.editor_changes_absent':'The Google Ads API change history does not return changes made in Google Ads Editor, so they are not in the API data.',
  'cov.api_30_day_window':'The Google Ads API returns change history for the last 30 days only; older changes are not in the API data.',
  'cov.auto_applied_may_be_missing':'Recommendations auto-applied by Google and some automated changes may be missing or appear under a system user.',
  'cov.ui_export_may_differ_from_api':'The Google Ads UI export and the API data can list the same change differently, so their row counts may differ.',
  'cov.metrics_conversion_lag':'Conversions are counted on the click date and keep arriving for days, so the most recent days of the after window may still grow.',
  'rs.budget_jump':'Budget, one step', 'rs.budget_cumulative':'Budget, within hours', 'rs.reverted':'Reverted', 'rs.repeat_edit':'Edited again',
  'rs.burst_session':'Many edits at once', 'rs.bid_target_relearn':'Bid target again', 'rs.automation_source':'Automation', 'rs.unknown_user':'Not on the user list',
  'rs.stale_entity':'Untouched', 'rs.stale_ad_group':'Ad group not touched', 'rs.stale_budget':'Budget unchanged', 'rs.off_hours':'Off hours', 'rs.bulk_edit':'Bulk edit',
  'pk.pct':'{v}%', 'pk.hours':'{v} h', 'pk.minutes':'{v} min', 'pk.days':'{v} days', 'pk.min_count':'{v}+ edits', 'pk.min_events':'{v}+ events',
  'pk.min_changes':'{v}+ changes', 'pk.min_campaigns':'{v}+ campaigns', 'pk.add_remove_minutes':'Added and removed within {v} min',
  'pk.value_days':'Value back within {v} days', 'pk.status_hours':'Status back within {v} h', 'pk.hours_range':'Outside {a}–{b}', 'pk.weekends':'Weekends',
  'op.CREATE':'Create', 'op.UPDATE':'Update', 'op.REMOVE':'Remove',
  'cat.Account':'Account', 'cat.Ad':'Ad', 'cat.AdGroup':'Ad group', 'cat.Asset':'Asset', 'cat.Audience':'Audience', 'cat.Bidding':'Bidding', 'cat.Budget':'Budget',
  'cat.Campaign':'Campaign', 'cat.Conversion':'Conversion', 'cat.Feed':'Feed', 'cat.Keyword':'Keyword', 'cat.Other':'Other', 'cat.Status':'Status', 'cat.Targeting':'Targeting',
  'sub.Access changed':'Access changed', 'sub.Account settings changed':'Account settings changed', 'sub.Customer identity changed':'Customer identity changed', 'sub.Manager account changed':'Manager account changed',
  'sub.Notification settings changed':'Notification settings changed', 'sub.Security settings changed':'Security settings changed', 'sub.Ad changed':'Ad changed', 'sub.Ad created':'Ad created',
  'sub.Ad removed':'Ad removed', 'sub.Ad group created':'Ad group created', 'sub.Ad group name changed':'Ad group name changed', 'sub.Ad group removed':'Ad group removed',
  'sub.Ad group settings changed':'Ad group settings changed', 'sub.Asset added':'Asset added', 'sub.Asset changed':'Asset changed', 'sub.Asset removed':'Asset removed',
  'sub.Asset set changed':'Asset set changed', 'sub.Audience added':'Audience added', 'sub.Audience bid adjustment changed':'Audience bid adjustment changed', 'sub.Bid changed':'Bid changed',
  'sub.Bid modifier changed':'Bid modifier changed', 'sub.Strategy changed':'Strategy changed', 'sub.Target CPA':'Target CPA', 'sub.Target ROAS':'Target ROAS',
  'sub.Budget changed':'Budget changed', 'sub.Budget created':'Budget created', 'sub.Budget removed':'Budget removed', 'sub.Campaign created':'Campaign created',
  'sub.Campaign name changed':'Campaign name changed', 'sub.Campaign removed':'Campaign removed', 'sub.Campaign settings changed':'Campaign settings changed', 'sub.Conversion action changed':'Conversion action changed',
  'sub.Conversion action created':'Conversion action created', 'sub.Conversion tracking changed':'Conversion tracking changed', 'sub.Feed attached to ad group':'Feed attached to ad group', 'sub.Feed attached to campaign':'Feed attached to campaign',
  'sub.Feed changed':'Feed changed', 'sub.Feed created':'Feed created', 'sub.Feed item added':'Feed item added', 'sub.Feed item changed':'Feed item changed',
  'sub.Feed item removed':'Feed item removed', 'sub.Feed removed':'Feed removed', 'sub.Keyword added':'Keyword added', 'sub.Keyword removed':'Keyword removed',
  'sub.Negative keyword':'Negative keyword', 'sub.Other':'Other', 'sub.Enabled':'Enabled', 'sub.Paused':'Paused',
  'sub.Removed':'Removed', 'sub.Ad schedule changed':'Ad schedule changed', 'sub.Device targeting changed':'Device targeting changed', 'sub.Language targeting changed':'Language targeting changed',
  'sub.Location targeting changed':'Location targeting changed',
},
tr: {
  brand:'Değişiklik Geçmişi', skip:'İçeriğe geç', nav_aria:'Sayfalar', tab1:'Özet', tab2:'Detaylar', tab3:'Tüm değişiklikler',
  meta_aria:'Rapor bilgisi', stats_aria:'Ana rakamlar', side_aria:'Bölümler',
  headline_fb_one:'Bu dönemde {n} değişiklik yapıldı.', headline_fb:'Bu dönemde {n} değişiklik yapıldı.',
  lede_fb:'Değişen kampanya: {c}. Değişiklik yapan kişi: {p}. Otomasyon değişikliği: {a}.',
  m_period:'Dönem', m_source:'Kaynak', m_tz:'Saat dilimi', m_generated:'Oluşturuldu',
  n_files_one:'{n} dosya', n_files:'{n} dosya', n_events_one:'{n} olay', n_events:'{n} olay',
  n_accounts_one:'{n} hesap', n_accounts:'{n} hesap', tz_unknown:'Bilinmiyor',
  st_changes:'Değişiklik', st_campaigns:'Değişen kampanya', st_people:'Kişi · {a} otomasyon', st_attention:'Göz atılacak durum', st_adgroups:'Değişen reklam grubu',
  flags_h:'Göz atılacaklar', flags_adjust:'Eşikler ayarlanabilir', flags_sr:'Durum',
  flags_nodata:'Bu raporda kural sonuçları yok; daha eski bir sürümle oluşturulmuş olabilir. Kendi eşiğinle denemek için:',
  flags_nodata2:'Bu raporda kural sonuçları yok.', flags_zero:'Bu dönemde hiçbir kural eşiğini aşmadı.',
  more_n_one:'+{n} daha', more_n:'+{n} daha', info_h:'Bilgi amaçlı',
  who_h:'Kim değiştirdi', who_sub:'Değişikliklerin kaynağı', who_seg:'{label} yüzde {p}',
  other_people_one:'{n} kişi daha', other_people:'{n} kişi daha', auto_google:'Otomasyon ve Google',
  budget_h:'Bütçe hareketi', budget_sub:'Günlük bütçelerdeki toplam değişim', n_increases_one:'{n} artış', n_increases:'{n} artış',
  n_decreases_one:'{n} azalış', n_decreases:'{n} azalış', budget_none:'Bu dönemde günlük bütçe değişikliği yok.', budget_more:'Bütçe değişikliklerini gör',
  stale_h:'Uzun süredir dokunulmayanlar', stale_sub:'Son değişiklikten bu yana geçen gün', stale_sub_m:'Son değişiklikten bu yana geçen gün; bazıları metrik dosyasına göre',
  n_days_one:'{n} gün', n_days:'{n} gün', stale_none:'{d} gün ya da daha uzun süredir dokunulmayan kampanya yok.', n_more_one:'{n} tane daha', n_more:'{n} tane daha',
  p1_scope:'Bu rapor yalnız değişiklik geçmişindeki kayıtları gösterir; performans etkisini yorumlamaz.', p1_notes_link:'Veri notları ({n})',
  s1:'Kampanyalar', s1_sub:'Her kampanyanın değişiklik özeti', s1_cap:'Raporun tüm dönemi, filtrelerden etkilenmez. Gün sayıları verinin son gününe göre.',
  ct_campaign:'Kampanya', ct_account:'Hesap', ct_changes:'Değişiklik', ct_budget:'Bütçe değişimi', ct_last:'Son değişiklik', ct_flags:'Göz atılacak',
  days_ago_0:'Son gün', days_ago_one:'{n} gün önce', days_ago:'{n} gün önce', show_all:'Tümünü göster ({n})', show_more_one:'{n} tane daha göster', show_more:'{n} tane daha göster', grp_none:'Bu grupta değişiklik yok.', show_less:'Daha az göster', camp_tbl_aria:'Kampanyalar tablosu',
  rollup_h:'Hesap bazında özet', rollup_cap:'Şu anki filtrelerle her hesap için bir satır. Yalnız sayılar. Filtrelemek için hesap adını seç.',
  ro_aria:'Hesap bazında özet tablosu', ro_camps:'Değişen kampanya', ro_users:'İsimli kullanıcı', ro_auto:'Otomasyon değişikliği', ro_first:'İlk değişiklik', ro_files:'Dosyalar',
  acc_h:'Hesap ve kampanya hareketi', acc_cap:'Her hesap içindeki kampanyalara açılır, değişiklik sayısına göre sıralı. Bu sayfayı bir kampanyaya göre süzmek için kampanyayı seç.',
  untouched_h:'Kampanyaların son değişikliği',
  untouched_cap:'Yalnız bu dışa aktarımda en az bir değişikliği olan kampanyalar listelenir. Dışa aktarımda görünmeyen bir kampanya listede yoktur: o tarih aralığında değişmemiş ya da hiç var olmamış olabilir; bu rapor hangisi olduğunu bilemez. "Geçen gün" tarayıcının şu anki saatine ve şu anki filtrelere göre sayılır, dosyanın oluşturulduğu ana göre değil.',
  thresh_aria:'Son değişiklikten bu yana en az gün', thresh_all:'Tümü', thresh_7:'7+ gün', thresh_14:'14+ gün', thresh_30:'30+ gün', thresh_60:'60+ gün',
  ut_empty:'Bu eşiğe (ya da filtrelere) uyan kampanya yok.', ut_tbl_aria:'Kampanyaların son değişikliği tablosu', ut_days:'Bugüne kadar geçen gün',
  s2:'Göz atılacaklar', s2_sub:'Her durum, tetikleyen kurala göre',
  s2_cap:'Her durum, bu rapor için belirlenen bir eşikle ölçülmüş bir olgudur. Değişikliğe göz atmaya değer olduğunu söyler, ne anlama geldiğini söylemez. Değişikliklerini görmek için bir satır seç.',
  rule_fired:'Eşik: {p}', n_more_all_one:'{n} tane daha, Tüm değişiklikler sayfasında', n_more_all:'{n} tane daha, Tüm değişiklikler sayfasında',
  thr_h:'Kullanılan eşikler', thr_cap:'Yukarıdaki sonuçlar raporun oluşturulduğu ayarlarla hesaplandı. Rapor yeniden oluşturulurken bir kural dosyasıyla değiştirilebilir; hemen denemek için Tüm değişiklikler sayfasındaki "Kendi eşiğinle dene" bölümünü kullan.',
  thr_tbl_aria:'Eşikler tablosu', th_rule:'Kural', th_tier:'Düzey', th_status:'Durum', th_threshold:'Eşik', th_hits:'Sonuç',
  tier_attention:'Göz atılacak', tier_info:'Bilgi', st_on:'Açık', st_off:'Kapalı', thr_none:'Bu raporda kural ayarı yok.',
  s3:'Zaman çizelgesi', tl_cap:'Çubuk yüksekliği gösterilen en yoğun güne göredir. Tarih ve sayı için çubuğun üzerine gel, dokun ya da Tab ile git; yalnız o günü görmek için çubuğu seç.',
  tl_aria:'Günlük değişiklikler', tl_basis_utc:'UTC günleri', tl_basis_tz:'{tz} saatine göre günler', tl_basis_multi:'Her satırın kendi saat dilimine göre günler', tl_basis:'Günler',
  tl_bar_one:'{d}: {n} değişiklik', tl_bar:'{d}: {n} değişiklik', tl_excluded_one:'{n} satır saat dilimi bilinmediği için dışarıda.', tl_excluded:'{n} satır saat dilimi bilinmediği için dışarıda.',
  tl_empty:'Çizilecek, saat dilimi bilinen satır yok.', day_chip:'Gün: {d}', day_chip_aria:'Gün: {d}, bu filtreyi kaldır', metric_line:'Metrik çizgisi', off:'Kapalı',
  ml_none:'Metrik dosyasında bu günler için günlük {m} yok.', ml_aria:'Metrik dosyasından günlük {m}, tüm metrik satırları (filtresiz): en düşük {lo}, en yüksek {hi}. Noktalar en az bir değişiklik olan günleri gösterir.',
  ml_note:'Gri çizgi: günlük {m} (tüm metrik satırları, filtresiz), {lo} ile {hi} arası. Noktalar: değişiklik olan günler.',
  s4:'Kişiler', s4_sub:'Değişiklikleri kim yaptı', users_cap:'Kişi türüne göre: kişi (isimli kullanıcı), otomasyon (komut dosyası, kural, öneri) ya da bilinmiyor (kaynakta kimlik yok). Filtrelemek için bir çubuğu seç.',
  s5:'Kategoriler', s5_sub:'Ne tür değişiklik', cat_cap:'Kategoriler kaynak türü, alan, işlem ve özet metnine göre kural eşleşmesinden gelir. "Diğer", henüz hiçbir kuralın eşleşmediği anlamına gelir; coverage.txt dosyasına bak.',
  s6:'Önce / sonra', s6_sub:'Her değişikliğin çevresindeki metrikler', ba2_none:'Görünen değişikliklerin hiçbirinde önce/sonra penceresi yok.', ba2_aria:'Önce ve sonra tablosu', ba2_first:'Penceresi olan {m} değişiklikten en yeni {n} tanesi.',
  s7:'Kapsam ve veri', s7_sub:'Bu rapor neyi okudu, neyi gösterebilir', cov_h:'Kapsam notları', cov_cap:'Bu girdinin neyi gösterip neyi gösteremeyeceği, bu çalışma için.',
  limits_h:'Bu rapor neyi yapamaz',
  limits_1:'Yalnız girdi dosyasındakini bilir. Google Ads API\'yi canlı çağırmaz; dışa aktarımdan sonra ya da tarih aralığının dışında yapılan değişiklikleri bilmez.',
  limits_2:'Girdide görünmeyen bir kampanya "değişmedi" diye raporlanmaz: satır olmaması, var olup olmadığı dahil hiçbir yönde kanıt değildir.',
  limits_3:'Neyin ne zaman değiştiğini gösterir; bir değişikliğin yerinde olup olmadığını asla yorumlamaz. Göz atılacak durumlar yalnız bu rapor için belirlenen bir eşiğin aşıldığını söyler.',
  limits_4:'Girdide Google Ads ChangeEvent API verisi varsa: bu API yalnız 30 günlük geçmiş tutar, sorgu başına 10.000 satırla sınırlıdır, çok yeni bir değişikliğin yaklaşık 3 dakika gerisinden gelebilir ve Google Ads arayüzündeki değişiklik geçmişi sayfasının her kaydını içermeyebilir.',
  if_h:'Girdi dosyaları', if_aria:'Girdi dosyaları tablosu', if_file:'Dosya', if_rows:'Satır', if_accounts:'Hesaplar', if_first:'İlk tarih', if_last:'Son tarih',
  meta_line:'{n} değişiklik · {a} – {b} · Kaynaklar: {src}{files} · Diğer: %{o}{masked} · Oluşturuldu: {g}', meta_files:' ({n} girdi dosyası)', meta_masked:' · Kullanıcılar maskelendi',
  cav_api:'Google Ads ChangeEvent API verisi içeriyor. ChangeEvent, Google Ads arayüzündeki değişiklik geçmişinin her kaydını içermeyebilir; bu kaynaktaki sayıları alt sınır olarak oku.',
  cav_dates:'{n} satır (girdinin %{p} kadarı) tarih sütunu bu çalışmanın biçimiyle okunamadığı için dışarıda kaldı. Ayrıntı coverage.txt dosyasında.',
  cav_tz:'{n} satırın (%{p}) saat dilimi yok, bu yüzden zaman çizelgesinde gösterilmiyor; diğer tüm görünümlerde var. Bu satırlar tek bir saat diliminden geliyorsa --timezone <IANA adı> ile yeniden çalıştır.',
  dq_line:'Girdi: {in} satır · bu raporda: {rep}{split} · dışarıda kalan: {left}{flags}{ign}', dq_flags:' · gösterilen satırlardan: {x}', dq_ignored:' · kullanılmayan kaynak sütunları: {x}',
  'dq.category_other':'{n} satır "Diğer" kategorisinde', 'dq.timezone_unknown':'{n} satırda saat dilimi yok', 'dq.operation_inferred':'{n} satırda işlem türü tahmin edildi',
  'dq.value_parsed_from_summary':'{n} satırda değer özet metninden okundu', 'dq.multi_field_unsplit':'{n} satırda birden çok alan var',
  dq_split_stmt:'{n} satır, çok ifadeli hücreler bölünerek eklendi', dq_split_field:'{n} satır, çok alanlı satırlar bölünerek eklendi',
  dq_unreadable:'{n} okunamayan tarih', dq_duplicate:'{n} yinelenen', dq_dup_files:'{n} dosyalar arası yinelenen',
  dq_ba:'{a} satırda önce/sonra metrik penceresi var, {b} satırda eşleşen metrik yok', dq_ba_unmatched:', {c} kampanya metrik dosyalarında yok',
  dq_dup_kept:'{n} aynı satır tutuldu (yineleme olabilir)', dq_camp_inferred:'{n} satırda kampanya özet metninden okundu', dq_dst:'{n} satır yaz saati geçişine denk geliyor',
  filters_h:'Filtreler', search_aria:'Değişikliklerde ara', search_ph:'Kampanya, kişi, değişiklik metni ara…', more_filters:'Diğer filtreler',
  f_campaign:'Kampanya', f_user:'Kişi', f_category:'Kategori', f_flag:'Göz atılacak', f_account:'Hesap', f_actor:'Kişi türü', f_operation:'İşlem',
  f_source:'Kaynak', f_file:'Kaynak dosya', f_rule:'Eşik eşleşmesi', f_range:'Tarih aralığı', f_anchor:'Aralığın sonu',
  actor_all:'Tümü', actor_human:'Kişi', actor_auto:'Otomasyon', actor_unknown:'Bilinmiyor',
  rm_all:'Tüm değişiklikler', rm_any:'Bir kurala uyan', range_all:'Tümü', range_30:'30 gün', range_7:'7 gün', range_1:'Son 24 saat',
  anchor_data:'Verideki en yeni değişiklik', anchor_today:'Bugün', clear_filters:'Filtreleri temizle',
  all_accounts:'Tüm hesaplar', all_campaigns:'Tüm kampanyalar', all_users:'Tüm kişiler', all_categories:'Tüm kategoriler', all_types:'Tüm türler', all_sources:'Tüm kaynaklar', all_files:'Tüm dosyalar',
  n_selected:'{n} seçili', ms_clear:'Seçimi temizle', flag_all:'Tümü', flag_any:'Herhangi bir durum',
  flag_chip:'Göz atılacak durum: {x}', flag_chip_aria:'Göz atılacak durum: {x}, bu filtreyi kaldır',
  many_accounts:'{n} hesap', many_campaigns:'{n} kampanya', many_users:'{n} kişi', many_categories:'{n} kategori', many_operations:'{n} işlem türü', many_sources:'{n} kaynak', many_files:'{n} dosya',
  span_24h:'24 saat', span_days:'{n} gün', range_today:'Son {span}, bugüne kadar ({d})', range_data:'Verinin son {span} kısmı, {d} tarihine kadar',
  ctx_search:'Arama: "{q}"', showing:'Gösterilen: {x}', showing_all:'Gösterilen: tüm değişiklikler, filtre yok', open_all:'Tüm değişikliklerde aç ({n})',
  sum_h:'Özet', fs_changes:'Değişiklik', fs_accounts:'Hesap', fs_users:'Kişi', fs_campaigns:'Değişen kampanya', fs_adgroups:'Değişen reklam grubu',
  fs_last:'Son değişiklik', fs_top_user:'En çok değiştiren', fs_top_campaign:'En çok değişen kampanya',
  try_h:'Kendi eşiğinle dene', try_cap:'Eşleşme, değişikliğin burada belirlediğin değeri aştığını gösterir. Her eşleşmenin yanında tam sayı ve kural yazar. Varsayılan olarak kapalı; raporun kendi göz atılacak durumları bundan etkilenmez.',
  try_toggle:'Kurala göre vurgula', try_mag:'Büyüklük (±% değişim)', try_struct:'Yapısal',
  rm_off:'Kurala göre vurgulama kapalı.', rm_count:'Görünen {b} değişiklikten {a} tanesi bir kurala uyuyor', rm_none:'Şu anki kurallara uyan değişiklik yok.',
  rm_tbl_aria:'Kural eşleşmeleri tablosu', rm_oldnew:'Önce → sonra', rm_matched:'Uyan kural', rm_first:'{m} eşleşmeden ilk {n} tanesi gösteriliyor (sayı yukarıda doğru); geri kalanı için filtrelerle daralt.',
  rm_detail_mag:'{sign}%{pct} · kural: ≥±%{th}', rm_detail_state:'{fact} · kural: {label}', rm_op:'İşlem: {op}', rm_status_to:'Durum → {s}',
  'rm.budget_magnitude':'Bütçe değişimi', 'rm.target_cpa_magnitude':'Hedef EBM değişimi', 'rm.target_roas_magnitude':'Hedef ROAS değişimi', 'rm.bid_magnitude':'Teklif/TBM değişimi',
  'rm.campaign_paused':'Kampanya duraklatıldı', 'rm.campaign_removed':'Kampanya kaldırıldı', 'rm.ad_group_removed':'Reklam grubu kaldırıldı', 'rm.campaign_enabled':'Kampanya etkinleştirildi',
  thr_enter:'Bir sayı gir', thr_nan:'Sayı değil', thr_gt0:'0\'dan büyük olmalı', thr_max:'En çok {max}', thr_err:'{msg} (1–{max}). Şu an %{v} kullanılıyor.',
  exp_h:'Değişiklik listesi', exp_cap:'Ayrıntı için bir satır seç (tıkla ya da Tab ile gelip Enter). Sıralamak için sütun başlığını seç.',
  exp_cap_split:'1 satır = 1 değişiklik ifadesi: birden çok ifade içeren bir kaynak hücresi, ifade başına bir satır olarak gösterilir (ayrıntıdaki İfade satırına bak).',
  exp_tbl_aria:'Değişiklik tablosu', exp_empty:'Bu filtrelere uyan değişiklik yok.', pager:'Sayfa {p} / {n} · {r} satır', prev:'Önceki', next:'Sonraki',
  ba_col:'Önce/sonra sütunu', ba_metric_l:'Önce/sonra metriği', ba_legend:'Sütun: önce | sonra | fark | % fark. * = sonra penceresinde henüz olgunlaşan günler var (dönüşüm gecikmesi).',
  ba_cell_tip:'Önce | sonra | fark | % fark',
  c_date:'Tarih', c_user:'Kişi', c_account:'Hesap', c_campaign:'Kampanya', c_adgroup:'Reklam grubu', c_category:'Kategori', c_change:'Değişiklik', c_summary:'Özet',
  c_old:'Önce', c_new:'Sonra', c_flags:'Göz atılacak', c_matches:'Eşik eşleşmesi', c_ba:'Önce/sonra', c_source:'Kaynak',
  automation_lc:'Otomasyon', unknown_pill:'Bilinmiyor', camp_inferred_tip:'Kampanya adı özet metninden okundu',
  raw:'(ham: {v})', parsed:'Metinden', parsed_tip:'Değişiklik özet metninden okundu, yapısal bir alandan değil',
  actor_auto_label:'{name} (otomasyon)', actor_unknown_label:'{name} (kişi türü bilinmiyor)', actor_unknown_none:'Bilinmiyor (kaynakta kimlik yok)',
  account_n:'Hesap {id}', campaign_n:'Kampanya {id}', no_account:'(hesap yok)', no_campaign:'(kampanya yok)', no_file:'(dosya yok)', no_data:'Bu filtrelerle veri yok.',
  ba_h:'Önce / sonra', ba_nomatch:'Bu değişikliğin hesabı ve kampanyası (ya da reklam grubu) için metrik satırı bulunamadı.',
  lvl_campaign:'Kampanya', lvl_adgroup:'Reklam grubu', lvl_campaign_lc:'kampanya', lvl_adgroup_lc:'reklam grubu',
  ba_cap:'Önce: değişiklik gününden önceki {n} tam gün ({bd} gün veri var). Sonra: değişiklik günü ve sonraki {n1} gün ({ad} gün veri var){mat}. Toplamlar metrik dosyasından, düz sayı olarak.',
  ba_mat:'; bunların {m} günü henüz olgunlaşıyor (dönüşüm gecikmesi), rakamlar değişebilir', ba_tbl_aria:'Önce ve sonra metrikleri',
  ba_metric:'Metrik', ba_before:'Önce', ba_after:'Sonra', ba_diff:'Fark', ba_pct:'% fark', ba_star:'* Olgunlaşan günleri içerir.',
  ba_overlap_one:'Bu pencerelerde aynı {lvl} içinde {n} başka değişiklik var{first}:', ba_overlap:'Bu pencerelerde aynı {lvl} içinde {n} başka değişiklik var{first}:',
  ba_first:' (ilk {k} tanesi listelendi)', ba_notin:'{id} (bu raporda yok)',
  mn_files:'Metrikler: {f}', mn_file:'Metrik dosyası', mn_level:'Düzey: {x}', mn_window:'Pencere: {n} gün', mn_lag:'Dönüşüm gecikmesi: {n} gün', mn_ignored:'Kullanılmayan sütunlar: {x}',
  m_impressions:'Gösterim', m_clicks:'Tıklama', m_cost:'Maliyet', m_conversions:'Dönüşüm', m_conversion_value:'Dönüşüm değeri',
  d_title:'Değişiklik ayrıntısı', close_aria:'Ayrıntıyı kapat', d_flags:'Göz atılacak',
  d_date:'Tarih', d_user:'Kişi', d_account:'Hesap', d_campaign:'Kampanya', d_adgroup:'Reklam grubu', d_category:'Kategori', d_field:'Alan', d_previous:'Önceki', d_new:'Yeni',
  d_operation:'İşlem', d_source:'Kaynak', d_valuefrom:'Değerin kaynağı', d_summary:'Özet', d_statement:'İfade', d_fullcell:'Kaynak hücrenin tamamı',
  d_resource:'Kaynak öğe', d_eventid:'Olay kimliği', d_file:'Kaynak dosya',
  d_tz_unknown:' (saat dilimi bilinmiyor)', d_camp_inferred:' (özet metninden okundu)', d_no_rule:' (kural eşleşmedi)', d_op_inferred:'{op} (tahmin, kaynakta yazmıyor)',
  d_val_none:'Kaynakta değer yok', d_val_parsed:'Özet metni (ayrıştırıldı, yapısal alan değil)', d_val_struct:'Yapısal alan',
  d_stmt:'Tek kaynak hücresindeki {n} ifadeden {pos}. ifade', d_derived:'{id} (türetildi, kaynakta yok)', d_parsed_suffix:' (özet metninden ayrıştırıldı)',
  'cov.editor_changes_absent':'Google Ads API değişiklik geçmişi, Google Ads Editor ile yapılan değişiklikleri döndürmez; bu değişiklikler API verisinde yer almaz.',
  'cov.api_30_day_window':'API değişiklik geçmişi yalnız son 30 günü kapsar.',
  'cov.auto_applied_may_be_missing':'Otomatik uygulanan öneriler ve bazı otomatik değişiklikler bu veride eksik olabilir ya da bir sistem kullanıcısı adıyla görünebilir.',
  'cov.ui_export_may_differ_from_api':'Arayüz dışa aktarımı ile API verisi aynı değişikliği farklı satırlarla gösterebilir.',
  'cov.metrics_conversion_lag':'Dönüşümler gecikmeli raporlanır, değişiklik sonrasındaki son günlerin sayıları henüz tamamlanmamış olabilir.',
  'rs.budget_jump':'Bütçe tek seferde', 'rs.budget_cumulative':'Bütçe birkaç saatte', 'rs.reverted':'Geri alındı', 'rs.repeat_edit':'Tekrar değişti',
  'rs.burst_session':'Kısa sürede çok işlem', 'rs.bid_target_relearn':'Teklif hedefi yeniden', 'rs.automation_source':'Otomasyon', 'rs.unknown_user':'Listede olmayan kişi',
  'rs.stale_entity':'Dokunulmamış', 'rs.stale_ad_group':'Reklam grubu dokunulmamış', 'rs.stale_budget':'Bütçe uzun süredir aynı', 'rs.off_hours':'Mesai dışı', 'rs.bulk_edit':'Toplu düzenleme',
  'pk.pct':'%{v}', 'pk.hours':'{v} saat', 'pk.minutes':'{v} dk', 'pk.days':'{v} gün', 'pk.min_count':'{v}+ kez', 'pk.min_events':'{v}+ olay',
  'pk.min_changes':'{v}+ değişiklik', 'pk.min_campaigns':'{v}+ kampanya', 'pk.add_remove_minutes':'Ekleme ve kaldırma {v} dk içinde',
  'pk.value_days':'Değer {v} gün içinde geri döndü', 'pk.status_hours':'Durum {v} saat içinde geri döndü', 'pk.hours_range':'{a}–{b} dışı', 'pk.weekends':'Hafta sonu dahil',
  'op.CREATE':'Oluşturma', 'op.UPDATE':'Güncelleme', 'op.REMOVE':'Kaldırma',
  'cat.Account':'Hesap', 'cat.Ad':'Reklam', 'cat.AdGroup':'Reklam grubu', 'cat.Asset':'Öğe', 'cat.Audience':'Kitle', 'cat.Bidding':'Teklif', 'cat.Budget':'Bütçe',
  'cat.Campaign':'Kampanya', 'cat.Conversion':'Dönüşüm', 'cat.Feed':'Feed', 'cat.Keyword':'Anahtar kelime', 'cat.Other':'Diğer', 'cat.Status':'Durum', 'cat.Targeting':'Hedefleme',
  'sub.Access changed':'Erişim değişti', 'sub.Account settings changed':'Hesap ayarları değişti', 'sub.Customer identity changed':'Müşteri kimliği değişti', 'sub.Manager account changed':'Yönetici hesap değişti',
  'sub.Notification settings changed':'Bildirim ayarları değişti', 'sub.Security settings changed':'Güvenlik ayarları değişti', 'sub.Ad changed':'Reklam değişti', 'sub.Ad created':'Reklam oluşturuldu',
  'sub.Ad removed':'Reklam kaldırıldı', 'sub.Ad group created':'Reklam grubu oluşturuldu', 'sub.Ad group name changed':'Reklam grubu adı değişti', 'sub.Ad group removed':'Reklam grubu kaldırıldı',
  'sub.Ad group settings changed':'Reklam grubu ayarları değişti', 'sub.Asset added':'Öğe eklendi', 'sub.Asset changed':'Öğe değişti', 'sub.Asset removed':'Öğe kaldırıldı',
  'sub.Asset set changed':'Öğe grubu değişti', 'sub.Audience added':'Kitle eklendi', 'sub.Audience bid adjustment changed':'Kitle teklif ayarı değişti', 'sub.Bid changed':'Teklif değişti',
  'sub.Bid modifier changed':'Teklif ayarlayıcı değişti', 'sub.Strategy changed':'Teklif stratejisi değişti', 'sub.Target CPA':'Hedef EBM', 'sub.Target ROAS':'Hedef ROAS',
  'sub.Budget changed':'Bütçe değişti', 'sub.Budget created':'Bütçe oluşturuldu', 'sub.Budget removed':'Bütçe kaldırıldı', 'sub.Campaign created':'Kampanya oluşturuldu',
  'sub.Campaign name changed':'Kampanya adı değişti', 'sub.Campaign removed':'Kampanya kaldırıldı', 'sub.Campaign settings changed':'Kampanya ayarları değişti', 'sub.Conversion action changed':'Dönüşüm işlemi değişti',
  'sub.Conversion action created':'Dönüşüm işlemi oluşturuldu', 'sub.Conversion tracking changed':'Dönüşüm izleme değişti', 'sub.Feed attached to ad group':'Feed reklam grubuna bağlandı', 'sub.Feed attached to campaign':'Feed kampanyaya bağlandı',
  'sub.Feed changed':'Feed değişti', 'sub.Feed created':'Feed oluşturuldu', 'sub.Feed item added':'Feed öğesi eklendi', 'sub.Feed item changed':'Feed öğesi değişti',
  'sub.Feed item removed':'Feed öğesi kaldırıldı', 'sub.Feed removed':'Feed kaldırıldı', 'sub.Keyword added':'Anahtar kelime eklendi', 'sub.Keyword removed':'Anahtar kelime kaldırıldı',
  'sub.Negative keyword':'Negatif anahtar kelime', 'sub.Other':'Diğer', 'sub.Enabled':'Etkinleştirildi', 'sub.Paused':'Duraklatıldı',
  'sub.Removed':'Kaldırıldı', 'sub.Ad schedule changed':'Reklam zaman planı değişti', 'sub.Device targeting changed':'Cihaz hedeflemesi değişti', 'sub.Language targeting changed':'Dil hedeflemesi değişti',
  'sub.Location targeting changed':'Konum hedeflemesi değişti',
},
};
let UI_LANG = null;
function LANG(){ return UI_LANG || (DASH_DATA && DASH_DATA.ui_lang==='tr' ? 'tr' : 'en'); }
function t(key, vars){
  const L = T[LANG()] || T.en;
  const s = Object.prototype.hasOwnProperty.call(L, key) ? L[key] : T.en[key];
  if(s===undefined) return key;
  return vars ? String(s).replace(/\{(\w+)\}/g, (m, k)=> vars[k]===undefined || vars[k]===null ? m : String(vars[k])) : s;
}
// Counted strings: "<key>_one" for 1 when the table has it.
function tn(key, n, vars){ return t(n===1 && T.en[key+'_one']!==undefined ? key+'_one' : key, Object.assign({n: fmtInt(n)}, vars||{})); }
function catName(c){ return c==null ? '' : T.en['cat.'+c]!==undefined ? t('cat.'+c) : String(c); }
function subName(s){ return s==null ? '' : T.en['sub.'+s]!==undefined ? t('sub.'+s) : String(s); }
function opName(o){ return o==null ? '' : T.en['op.'+o]!==undefined ? t('op.'+o) : String(o); }
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
  // 2026-11-14: an unknown-type actor can still carry a name (e.g. a Google rep acting with permission).
  return 'u:'+(c.user_name||c.user_email||'');
}
function actorLabel(c){
  const k = actorKey(c);
  if(k.startsWith('h:')) return c.user_name||c.user_email;
  if(k.startsWith('a:')) return t('actor_auto_label', {name: k.slice(2)});
  return k.length>2 ? t('actor_unknown_label', {name: k.slice(2)}) : t('actor_unknown_none');
}
function accountName(c){ return c.account_name || (c.account_id ? t('account_n', {id: c.account_id}) : t('no_account')); }
function campaignName(c){ return c.campaign_name || (c.campaign_id ? t('campaign_n', {id: c.campaign_id}) : t('no_campaign')); }
// Turkish-aware, accent-insensitive fold (F-5/B-2): İ/I/ı -> i, ş -> s, ğ -> g, NFD marks stripped. Used on both sides.
function fold(s){
  if(s===null || s===undefined) return '';
  return String(s).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/ı/g,'i');
}
function rowHaystack(c){
  if(c._hay===undefined){
    const extra = LANG()==='tr' ? [catName(c.category), subName(c.subcategory)] : [];
    c._hay = fold([c.campaign_name,c.account_name,c.user_name,c.user_email,c.category,c.subcategory,
      c.field_name,c.old_value,c.new_value,c.ad_group_name,c.operation,c.source,c.resource_type,
      c.client_type,c.change_id,c.raw_summary,c.value_unit,c.currency,...extra].filter(v=>v!==null && v!==undefined).join(' '));
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
// is known), never re-zoned to the viewer. 2026-11-19: the report language
// sets the locale: dd.mm.yyyy HH:MM ('tr-TR') or day-first 24h English ('en-GB').
let UI_LOCALE;
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
// 2026-11-19: short forms for the summary pages (dd.mm HH:MM, "26 Oca – 11 Ağu 2026").
function isoParts(iso){
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2}))?/.exec(iso||'');
  return m ? {y:+m[1], mo:+m[2], d:+m[3], h:m[4]!==undefined ? +m[4] : null, mi:m[5]!==undefined ? +m[5] : null, date: new Date(Date.UTC(+m[1], +m[2]-1, +m[3], +(m[4]||0), +(m[5]||0)))} : null;
}
function fmtShort(iso){
  const p = isoParts(iso);
  if(!p) return iso ? String(iso) : '—';
  const two = n=> String(n).padStart(2, '0');
  if(LANG()==='tr') return `${two(p.d)}.${two(p.mo)}` + (p.h!=null ? ` ${two(p.h)}:${two(p.mi)}` : '');
  const opt = {timeZone:'UTC', month:'short', day:'numeric'};
  if(p.h!=null){ opt.hour = '2-digit'; opt.minute = '2-digit'; opt.hourCycle = 'h23'; }
  try { return new Intl.DateTimeFormat(UI_LOCALE, opt).format(p.date); } catch(e){ return String(iso); }
}
function fmtPeriod(a, b){
  const pa = isoParts(a), pb = isoParts(b);
  if(!pa || !pb) return '—';
  const f = (p, year)=>{ try { return new Intl.DateTimeFormat(UI_LOCALE, Object.assign({timeZone:'UTC', day:'numeric', month:'short'}, year ? {year:'numeric'} : {})).format(p.date); } catch(e){ return `${p.d}.${p.mo}.${p.y}`; } };
  if(pa.y===pb.y && pa.mo===pb.mo && pa.d===pb.d) return f(pb, true);
  return `${f(pa, pa.y!==pb.y)} – ${f(pb, true)}`;
}
function fmtInt(v){ const n = Number(v); return isFinite(n) ? n.toLocaleString(UI_LOCALE, {maximumFractionDigits: 0}) : '—'; }
function fmtMoney(v, cur){
  const n = Number(v);
  if(!isFinite(n)) return '—';
  if(cur && /^[A-Z]{3}$/.test(cur)){
    try { return new Intl.NumberFormat(UI_LOCALE, {style:'currency', currency:cur, currencyDisplay:'narrowSymbol', maximumFractionDigits:0}).format(n); } catch(e){}
  }
  return n.toLocaleString(UI_LOCALE, {maximumFractionDigits: 0}) + (cur ? ' '+cur : '');
}
// ---- values ----------------------------------------------------------
// Neutral display, no colour coding. Units: micros-converted amounts
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
function valueText(c, which){ const p = valueParts(c, which); return p ? p.main + (p.raw ? ' ' + t('raw', {v: p.raw}) : '') : ''; }
function valueCellHtml(c, which){
  const p = valueParts(c, which);
  if(!p) return '—';
  return escapeHtml(p.main) + (p.raw ? ` <span class="note" style="margin:0;">${escapeHtml(t('raw', {v: p.raw}))}</span>` : '')
    + (c.value_confidence==='parsed_from_summary' ? `<span class="parsed" title="${escapeHtml(t('parsed_tip'))}">${escapeHtml(t('parsed'))}</span>` : '');
}
function barFillStyle(color, pct){
  return `width:${Math.max(2,pct)}%;background:${color};`;
}
// ---- round 10 (2026-11-14): day filter, before/after, coverage notes, files, rollup.
// Every field read here is optional; absent fields render nothing (never 0).
const BA_METRICS = [['impressions','Impressions'],['clicks','Clicks'],['cost','Cost'],['conversions','Conversions'],['conversion_value','Conv. value']];
const view = { baCol:false, baMetric:'clicks', metric:'clicks' };
function dmeta(){ return (DASH_DATA && DASH_DATA.meta) || {}; }
function asObj(v){ return v && typeof v==='object' && !Array.isArray(v) ? v : null; }
function baData(){ return DASH_DATA ? asObj(DASH_DATA.before_after || dmeta().before_after) : null; }
function metricsMeta(){ return DASH_DATA ? asObj(DASH_DATA.metrics_meta || dmeta().metrics_meta) : null; }
function baFor(c){ const b = baData(); return b && c && c.change_id!=null ? asObj(b[c.change_id]) : null; }
function numOrNull(v){ return typeof v==='number' && isFinite(v) ? v : null; }
function fmtNum(v, digits){ return v==null ? '—' : Number(v).toLocaleString(UI_LOCALE, {maximumFractionDigits: digits==null ? 2 : digits}); }
function fmtSigned(v, digits, suffix){ return v==null ? '—' : (v>0 ? '+' : '') + fmtNum(v, digits) + (suffix||''); }
function metricLabel(k){ return t('m_'+k); }
function baMetricList(){
  const mm = metricsMeta(); const names = mm && Array.isArray(mm.metrics) ? mm.metrics : null;
  const l = names ? BA_METRICS.filter(([k])=>names.includes(k)) : [];
  return (l.length ? l : BA_METRICS).map(([k])=>[k, metricLabel(k)]);
}
function baMaturing(ba){ const m = ba && numOrNull((ba.after||{}).maturing_days); return m && m>0 ? m : 0; }
// Plain arithmetic: difference = after - before; % only when before is a non-zero number.
function baMetricRow(ba, key){
  const b = numOrNull(((ba&&ba.before)||{})[key]), a = numOrNull(((ba&&ba.after)||{})[key]);
  const diff = b!=null && a!=null ? a-b : null;
  return {key, before:b, after:a, diff, pct: diff!=null && b!==0 ? diff/Math.abs(b)*100 : null};
}
function baCellText(ba, key){
  if(!ba) return '—';
  const r = baMetricRow(ba, key);
  return `${fmtNum(r.before)} | ${fmtNum(r.after)}${baMaturing(ba) ? '*' : ''} | ${fmtSigned(r.diff)} | ${fmtSigned(r.pct, 1, '%')}`;
}
let changeById = null;
function changeByIdGet(id){
  if(!changeById || changeById._rows!==DASH_DATA.changes){ changeById = new Map(); changeById._rows = DASH_DATA.changes; DASH_DATA.changes.forEach(r=>{ if(r.change_id!=null && !changeById.has(r.change_id)) changeById.set(r.change_id, r); }); }
  return changeById.get(id) || null;
}
function baDetailHtml(c){
  const mm = metricsMeta(), ba = baFor(c);
  if(!ba) return mm ? `<div class="ba"><h4>${escapeHtml(t('ba_h'))}</h4><p class="note">${escapeHtml(t('ba_nomatch'))}</p></div>` : '';
  const n = numOrNull(ba.window_days) ?? numOrNull(mm && mm.window_days);
  const adg = ba.level==='ad_group';
  const lvl = t(adg ? 'lvl_adgroup' : 'lvl_campaign');
  const bd = numOrNull((ba.before||{}).days_with_data), ad = numOrNull((ba.after||{}).days_with_data), mat = baMaturing(ba);
  const cap = t('ba_cap', {n: n ?? '?', bd: bd ?? '—', n1: n!=null ? n-1 : '?', ad: ad ?? '—', mat: mat ? t('ba_mat', {m: mat}) : ''});
  const rows = baMetricList().map(([k,l])=>{ const r = baMetricRow(ba, k);
    return `<tr><th scope="row">${escapeHtml(l)}</th><td class="num">${fmtNum(r.before)}</td><td class="num">${fmtNum(r.after)}${mat ? '*' : ''}</td><td class="num">${fmtSigned(r.diff)}</td><td class="num">${fmtSigned(r.pct, 1, '%')}</td></tr>`; }).join('');
  let html = `<div class="ba"><h4>${escapeHtml(t('ba_h'))}</h4><p class="note">${escapeHtml(lvl)}: ${escapeHtml(ba.entity || '—')}. ${escapeHtml(cap)}</p>`
    + `<div class="tbl-wrap" tabindex="0" role="region" aria-label="${escapeHtml(t('ba_tbl_aria'))}"><table><thead><tr><th>${escapeHtml(t('ba_metric'))}</th><th class="num">${escapeHtml(t('ba_before'))}</th><th class="num">${escapeHtml(t('ba_after'))}</th><th class="num">${escapeHtml(t('ba_diff'))}</th><th class="num">${escapeHtml(t('ba_pct'))}</th></tr></thead><tbody>${rows}</tbody></table></div>`
    + (mat ? `<p class="note">${escapeHtml(t('ba_star'))}</p>` : '');
  const ov = asObj(ba.overlapping);
  const ids = ov && Array.isArray(ov.change_ids) ? ov.change_ids : [];
  const cnt = ov ? (numOrNull(ov.count) ?? ids.length) : 0;
  if(cnt > 0){
    html += `<p class="note">${escapeHtml(tn('ba_overlap', cnt, {lvl: t(adg ? 'lvl_adgroup_lc' : 'lvl_campaign_lc'), first: ids.length < cnt ? t('ba_first', {k: ids.length}) : ''}))}</p><ul>` + ids.map(id=>{
      const r = changeByIdGet(id);
      if(!r) return `<li>${escapeHtml(t('ba_notin', {id}))}</li>`;
      const x = `${fmtTs(r)} · ${subName(r.subcategory) || catName(r.category) || ''}${firstLine(r.raw_summary) ? ' · '+firstLine(r.raw_summary) : ''}`;
      return `<li><button type="button" class="link-btn" data-open="${escapeHtml(id)}">${escapeHtml(x.length>140 ? x.slice(0,139)+'…' : x)}</button></li>`;
    }).join('') + '</ul>';
  }
  return html + '</div>';
}
function coverageNotes(){
  const n = dmeta().coverage_notes;
  return Array.isArray(n) ? n.map(x=> typeof x==='string' ? {id:'', text:x} : x && x.text ? {id:String(x.id||''), text:String(x.text)} : null).filter(Boolean) : [];
}
// A known note id reads from T (so it follows the report language); an unknown one keeps its own text.
function coverageText(n){ return n.id && T.en['cov.'+n.id]!==undefined ? t('cov.'+n.id) : n.text; }
function metricsNoteText(){
  const mm = metricsMeta(); if(!mm) return '';
  const files = (Array.isArray(mm.files) ? mm.files : []).map(f=> typeof f==='string' ? f : f && (f.file || f.name)).filter(Boolean);
  const parts = [t('mn_files', {f: files.join(', ') || t('mn_file')})];
  if(mm.date_min || mm.date_max) parts.push(`${fmtWall(mm.date_min, false)} – ${fmtWall(mm.date_max, false)}`);
  if(Array.isArray(mm.levels) && mm.levels.length) parts.push(t('mn_level', {x: mm.levels.map(l=> l==='ad_group' ? t('lvl_adgroup_lc') : l==='campaign' ? t('lvl_campaign_lc') : String(l)).join(', ')}));
  if(numOrNull(mm.window_days)!=null) parts.push(t('mn_window', {n: mm.window_days}));
  if(numOrNull(mm.conversion_lag_days)!=null) parts.push(t('mn_lag', {n: mm.conversion_lag_days}));
  if(Array.isArray(mm.ignored_columns) && mm.ignored_columns.length) parts.push(t('mn_ignored', {x: mm.ignored_columns.join(', ')}));
  return parts.join(' · ');
}
// meta.source_labels (round 10), else meta.sources as plain labels (older runs) or per-file objects.
function sourceLabels(m){ const s = Array.isArray(m.source_labels) ? m.source_labels : Array.isArray(m.sources) ? m.sources : []; return [...new Set(s.map(x=> asObj(x) ? x.source_label : x).filter(Boolean))]; }
function sourceFilesMeta(m){ return (Array.isArray(m.sources) ? m.sources : []).filter(x=> asObj(x) && x.file); }
// Day bucket of a row: same basis as the timeline (account-local, or UTC when the run had no zone).
function dayKey(c){
  if(!c.timestamp_utc) return null;
  const utc = dmeta().timeline_reference_tz==='UTC';
  return String(utc ? c.timestamp_utc : (c.timestamp_iso || c.timestamp_utc)).slice(0,10);
}
function accountRollup(rows){
  const m = new Map();
  rows.forEach(c=>{
    const k = accountIdentity(c) || NONE;
    let a = m.get(k);
    if(!a){ a = {key:k, name: accountName(c), changes:0, camps:new Set(), humans:new Set(), automation:0, first:null, last:null, files:new Set()}; m.set(k, a); }
    a.changes++;
    const ck = campaignIdentity(c); if(ck) a.camps.add(ck);
    if(c.actor_type==='human'){ const u = userIdentity(c); if(u) a.humans.add(u); } else if(c.actor_type==='automation') a.automation++;
    const t = rowTime(c);
    if(t!=null){ if(!a.first || t<rowTime(a.first)) a.first = c; if(!a.last || t>rowTime(a.last)) a.last = c; }
    if(c.source_file) a.files.add(c.source_file);
  });
  return [...m.values()].map(a=>({key:a.key, name:a.name, changes:a.changes, campaigns:a.camps.size, users:a.humans.size, automation:a.automation, first:a.first, last:a.last, files:[...a.files].sort()}))
    .sort((x,y)=> y.changes-x.changes || String(x.name).localeCompare(String(y.name)));
}
// Daily metrics totals for the timeline line: optional `metrics_daily` (or metrics_meta.daily), [{date, clicks, cost, ...}].
function metricsDaily(){
  const d = DASH_DATA && (DASH_DATA.metrics_daily || (metricsMeta()||{}).daily);
  if(!Array.isArray(d)) return null;
  const map = {}; d.forEach(r=>{ if(r && r.date) map[String(r.date).slice(0,10)] = r; });
  return Object.keys(map).length ? map : null;
}
function lineMetrics(daily){ return daily ? ['clicks','cost'].filter(k=>Object.values(daily).some(r=>numOrNull(r[k])!=null)).map(k=>[k, metricLabel(k)]) : []; }
// Day axis: only days with changes, or (with a metrics line) every calendar day in range, at most 400.
function timelineAxis(counts, daily){
  const days = Object.keys(counts).sort();
  if(!daily) return days;
  let all = [...days, ...Object.keys(daily)].sort();
  if(!all.length) return [];
  let lo = all[0], hi = all[all.length-1];
  const span = (Date.parse(hi+'T00:00:00Z') - Date.parse(lo+'T00:00:00Z'))/86400000;
  if(span > 400 && days.length){ lo = days[0]; hi = days[days.length-1]; }
  const out = [];
  for(let t = Date.parse(lo+'T00:00:00Z'), end = Date.parse(hi+'T00:00:00Z'); t <= end && out.length < 400; t += 86400000) out.push(new Date(t).toISOString().slice(0,10));
  return out;
}
// =====================================================================
// RULE MATCHES (V1: Magnitude + State), deterministic, user-adjustable
// thresholds ("Try your own thresholds" on page 3). A match means "this row
// crossed a value you set", never a verdict; every match renders with the
// exact number and rule next to it. Matched on category/subcategory
// (resource_type/field_name are null for text-summary sources).
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
    fact:c=> statusFact(c, t('rm_status_to', {s:'PAUSED'})) },
  { id:'campaign_removed', label:'Campaign removed', defaultOn:true, match:c=> c.subcategory==='Campaign removed',
    fact:c=> t('rm_op', {op: c.operation||'REMOVE'}) },
  { id:'ad_group_removed', label:'Ad group removed', defaultOn:true, match:c=> c.subcategory==='Ad group removed',
    fact:c=> t('rm_op', {op: c.operation||'REMOVE'}) },
  { id:'campaign_enabled', label:'Campaign enabled', defaultOn:false, match:c=>{
      if(c.subcategory!=='Enabled') return false;
      if(c.resource_type) return isCampaignResource(c.resource_type);
      const s = c.raw_summary||'';
      return CAMPAIGN_ENABLED_TEXT.some(re=>re.test(s));
    },
    fact:c=> statusFact(c, t('rm_status_to', {s:'ENABLED'})) },
];
function ruleLabel(r){ return t('rm.'+r.id); }
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
    if(ov==null || nv==null || ov===0) continue; // CREATE/REMOVE has no "before": % change undefined, not zero
    const pctChange = ((nv-ov)/Math.abs(ov))*100;
    const threshold = ruleState.pct[r.id];
    if(Math.abs(pctChange) >= threshold){
      const sign = pctChange>=0 ? '+' : '';
      out.push({ id:r.id, label:ruleLabel(r), pct:pctChange, detail:t('rm_detail_mag', {sign, pct: pctChange.toFixed(0), th: threshold}) });
    }
  }
  for(const r of STATE_RULES){
    if(!ruleState.stateOn[r.id]) continue;
    if(r.match(c)) out.push({ id:r.id, label:ruleLabel(r), detail:t('rm_detail_state', {fact: r.fact(c), label: ruleLabel(r)}) });
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
  if(s==='') return {ok:false, msg:t('thr_enter')};
  const n = Number(s);
  if(!isFinite(n)) return {ok:false, msg:t('thr_nan')};
  if(n<=0) return {ok:false, msg:t('thr_gt0')};
  if(n>PCT_MAX) return {ok:false, msg:t('thr_max', {max: PCT_MAX})};
  return {ok:true, value:n};
}
// F-6: turning highlighting off also drops the rule filter, so no hidden filter stays active.
function setRuleHighlighting(on){
  ruleState.enabled = !!on;
  if(!ruleState.enabled) state.ruleMatch = '';
  state.page = 1;
}
function defaultState(){
  return { accounts:new Set(), campaigns:new Set(), users:new Set(), actor:'', categories:new Set(), operations:new Set(), sources:new Set(), sourceFiles:new Set(), day:'', ruleMatch:'', flagRule:'', flagIds:null, flagRef:'', range:'all', anchor:'data', search:'', page:1, pageSize:200, sortKey:'timestamp_iso', sortDir:-1, minDays:0 };
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
// ---- round 12 (2026-11-19): flags, rules config, executive summary, campaign table.
// All optional: an older report without them still renders page 3 and a shorter page 1.
function rowsAll(){ return (DASH_DATA && DASH_DATA.changes) || []; }
function loc(o){ if(o==null) return ''; if(typeof o==='string') return o; const v = o[LANG()] ?? o.en ?? o.tr; return v==null ? '' : String(v); }
function rulesConfig(){ const r = DASH_DATA && DASH_DATA.rules_config; return Array.isArray(r) ? r.filter(x=>x && typeof x==='object' && x.rule) : []; }
let _flags = {src:undefined, list:[], idx:new Map()};
function flagCache(){
  const src = DASH_DATA && DASH_DATA.flags;
  if(_flags.src!==src){
    const list = Array.isArray(src) ? src.filter(f=>f && typeof f==='object' && f.rule) : [];
    const idx = new Map();
    list.forEach(f=>{ (Array.isArray(f.change_ids) ? f.change_ids : []).forEach(id=>{ const a = idx.get(id); if(!a) idx.set(id, [f]); else if(!a.includes(f)) a.push(f); }); });
    _flags = {src, list, idx};
  }
  return _flags;
}
function flagList(){ return flagCache().list; }
function flagsForChange(c){ return (c && c.change_id!=null && flagCache().idx.get(c.change_id)) || []; }
function hasFlagData(){ return !!DASH_DATA && (Array.isArray(DASH_DATA.flags) || rulesConfig().length > 0); }
function flagById(id){ return flagList().find(f=>String(f.id)===String(id)) || null; }
function flagText(f){ return loc(f && f.text); }
function ruleCfg(rule){ return rulesConfig().find(r=>r.rule===rule) || null; }
function ruleTitle(rule){ const c = ruleCfg(rule); return (c && loc(c.title)) || (T.en['rs.'+rule]!==undefined ? t('rs.'+rule) : String(rule)); }
// 2026-11-19: a campaign row's stale tag must say whether the campaign itself or only an ad group in it is untouched.
function staleCampaignItself(r){ return flagList().some(f=>f.rule==='stale_entity' && f.campaign===r.campaign && (!f.account || !r.account || f.account===r.account) && !(f.fact && f.fact.level==='ad_group')); }
function ruleShort(rule){ return T.en['rs.'+rule]!==undefined ? t('rs.'+rule) : ruleTitle(rule); }
function ruleTierOf(rule){ const c = ruleCfg(rule); if(c && c.tier) return c.tier; const f = flagList().find(x=>x.rule===rule); return (f && f.tier) || 'attention'; }
// Rules with at least one flag, attention tier first, then info; config order inside each tier.
function ruleGroups(){
  const by = new Map();
  flagList().forEach(f=>{ if(!by.has(f.rule)) by.set(f.rule, []); by.get(f.rule).push(f); });
  const order = rulesConfig().map(c=>c.rule);
  [...by.keys()].forEach(r=>{ if(!order.includes(r)) order.push(r); });
  const out = order.map(rule=>{
    const c = ruleCfg(rule), fl = by.get(rule) || [];
    const hits = c && numOrNull(c.hits)!=null ? c.hits : fl.length;
    return {rule, cfg:c, tier: ruleTierOf(rule), title: ruleTitle(rule), description: c ? loc(c.description) : '', hits, flags: fl, enabled: !c || c.enabled!==false};
  }).filter(g=>g.enabled && g.hits>0);
  return [...out.filter(g=>g.tier!=='info'), ...out.filter(g=>g.tier==='info')];
}
// Params as short facts: {pct:50, hours:24} -> "50% · 24 h" ("%50 · 24 saat" in Turkish).
function fmtParams(p){
  p = asObj(p); if(!p) return '';
  const out = [];
  if(p.start!=null || p.end!=null) out.push(t('pk.hours_range', {a: p.start ?? '?', b: p.end ?? '?'}));
  Object.keys(p).forEach(k=>{
    if(k==='start' || k==='end') return;
    const v = p[k];
    if(k==='weekends'){ if(v) out.push(t('pk.weekends')); return; }
    if(T.en['pk.'+k]!==undefined) out.push(t('pk.'+k, {v: typeof v==='number' ? fmtNum(v) : v}));
    else out.push(`${k}: ${Array.isArray(v) ? v.join(', ') : v}`);
  });
  return out.join(' · ');
}
function execSummary(){ return DASH_DATA ? asObj(DASH_DATA.exec_summary) : null; }
// Same sums as the generator's exec_summary.budget, for reports built before it existed.
function computeBudget(rows){
  let inc = 0, ninc = 0, dec = 0, ndec = 0; const curs = new Set();
  rows.forEach(c=>{
    if(c.subcategory!=='Budget changed') return;
    const o = c.old_value_num, n = c.new_value_num;
    if(typeof o!=='number' || typeof n!=='number' || !isFinite(o) || !isFinite(n)) return;
    const d = n - o;
    if(d>0){ inc += d; ninc++; } else if(d<0){ dec += d; ndec++; }
    if(c.currency) curs.add(c.currency);
  });
  return {currency: curs.size===1 ? [...curs][0] : null, increase_total: inc, increase_count: ninc, decrease_total: dec, decrease_count: ndec, net: inc+dec};
}
function computePeople(rows){
  const m = new Map();
  rows.forEach(c=>{ const k = actorKey(c); let e = m.get(k);
    if(!e){ e = {label: c.actor_type==='human' ? (c.user_name || c.user_email || actorLabel(c)) : actorLabel(c), actor_type: c.actor_type || 'unknown', changes: 0}; m.set(k, e); }
    e.changes++; });
  return [...m.values()].sort((a,b)=> b.changes - a.changes);
}
function computeCampaignTable(rows){
  const end = dataEndTime(), m = new Map();
  rows.forEach(c=>{
    const cid = campaignIdentity(c); if(!cid) return;
    const k = (accountIdentity(c)||NONE) + '||' + cid;
    let e = m.get(k);
    if(!e){ e = {account: c.account_name || (c.account_id ? t('account_n', {id: c.account_id}) : ''), campaign: campaignName(c), changes: 0, budget_changes: 0, last_change: null, _t: null, flags_by_rule: {}}; m.set(k, e); }
    e.changes++;
    if(c.subcategory==='Budget changed') e.budget_changes++;
    const tt = rowTime(c);
    if(tt!=null && (e._t==null || tt>e._t)){ e._t = tt; e.last_change = c.timestamp_iso || c.timestamp_utc; }
  });
  return [...m.values()].map(e=>({account: e.account, campaign: e.campaign, changes: e.changes, budget_changes: e.budget_changes, last_change: e.last_change, flags_by_rule: e.flags_by_rule,
    days_since: e._t!=null && end!=null ? Math.max(0, Math.floor((end - e._t)/86400000)) : null}));
}
function campaignTableRows(){ const ct = DASH_DATA && DASH_DATA.campaign_table; return Array.isArray(ct) ? ct.filter(r=>r && typeof r==='object') : computeCampaignTable(rowsAll()); }
function staleDays(){ const c = ruleCfg('stale_entity'); const d = c && asObj(c.params) && numOrNull(c.params.days); return d!=null ? d : 30; }
function staleList(){
  const es = execSummary();
  if(es && Array.isArray(es.stale)) return es.stale.filter(s=>s && typeof s==='object');
  const d = staleDays();
  return campaignTableRows().filter(r=>numOrNull(r.days_since)!=null && r.days_since>=d)
    .map(r=>({account: r.account, campaign: r.campaign, days: r.days_since, basis: 'export'})).sort((a,b)=> b.days - a.days);
}
// Deep-link targets resolve names back to the identities the filters use.
function campaignKeys(account, campaign){
  const acc = new Set(), camp = new Set();
  rowsAll().forEach(c=>{
    if(!(campaignName(c)===campaign || campaignIdentity(c)===campaign)) return;
    if(account && !(accountName(c)===account || accountIdentity(c)===account)) return;
    acc.add(accountIdentity(c)||NONE); camp.add(campaignIdentity(c)||NONE);
  });
  return {acc, camp};
}
function userKeys(label){
  const s = new Set();
  rowsAll().forEach(c=>{ if(actorLabel(c)===label || c.user_name===label || c.user_email===label) s.add(actorKey(c)); });
  return s;
}
// opt.ignoreDay: the timeline itself is drawn without the day filter, so the other days stay selectable.
function getFiltered(opt){
  const q = fold((state.search||'').trim());
  const useDay = state.day && !(opt && opt.ignoreDay);
  return DASH_DATA.changes.filter(c=>{
    if(state.accounts.size && !state.accounts.has(accountIdentity(c)||NONE)) return false;
    if(state.campaigns.size && !state.campaigns.has(campaignIdentity(c)||NONE)) return false;
    if(state.users.size && !state.users.has(actorKey(c))) return false;
    if(state.actor && c.actor_type !== state.actor) return false;
    if(state.categories.size && !state.categories.has(c.category)) return false;
    if(state.operations.size && !state.operations.has(c.operation)) return false;
    if(state.sources.size && !state.sources.has(c.source)) return false;
    if(state.sourceFiles.size && !state.sourceFiles.has(c.source_file||NONE)) return false;
    if(useDay && dayKey(c)!==state.day) return false;
    if(state.flagIds && !state.flagIds.has(c.change_id)) return false;
    if(state.flagRule){
      const fl = flagsForChange(c);
      if(state.flagRule==='__any__' ? !fl.length : !fl.some(f=>f.rule===state.flagRule)) return false;
    }
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
// ---- pages and hash routing (2026-11-19) ----------------------------
const PAGES = [['p1','ozet','summary'], ['p2','detaylar','details'], ['p3','tum','all']];
const SECTIONS = [['s1','kampanyalar','campaigns'], ['s2','goz-atilacaklar','flags'], ['s3','zaman','timeline'], ['s4','kisiler','people'],
  ['s5','kategoriler','categories'], ['s6','once-sonra','before-after'], ['s7','kapsam','coverage']];
const PARAMS = {flag:['durum','flag'], rule:['kural','rule'], account:['hesap','account'], campaign:['kampanya','campaign'], user:['kisi','user'], category:['kategori','category']};
function li(){ return LANG()==='tr' ? 1 : 2; }
function parseHash(h){
  const s = String(h||'').replace(/^#/, '');
  const qi = s.indexOf('?');
  const path = qi<0 ? s : s.slice(0, qi), query = qi<0 ? '' : s.slice(qi+1);
  const [pg, sec] = path.split('/');
  const page = (PAGES.find(p=>p[1]===pg || p[2]===pg) || ['p1'])[0];
  const section = sec ? ((SECTIONS.find(x=>x[1]===sec || x[2]===sec) || [null])[0]) : null;
  const params = {};
  if(query) query.split('&').forEach(kv=>{
    const i = kv.indexOf('='); let k, v;
    try { k = decodeURIComponent(i<0 ? kv : kv.slice(0, i)); v = i<0 ? '' : decodeURIComponent(kv.slice(i+1)); } catch(e){ return; }
    const name = Object.keys(PARAMS).find(n=>PARAMS[n].includes(k));
    if(name && v!=='') params[name] = v;
  });
  return {page, section, params};
}
function buildHash(page, section, params){
  const p = PAGES.find(x=>x[0]===page) || PAGES[0];
  let h = '#' + p[li()];
  if(section){ const s = SECTIONS.find(x=>x[0]===section); if(s) h += '/' + s[li()]; }
  const q = Object.keys(PARAMS).filter(k=>params && params[k]!=null && params[k]!=='').map(k=> PARAMS[k][li()-1] + '=' + encodeURIComponent(params[k]));
  return q.length ? h + '?' + q.join('&') : h;
}
function hrefAll(params){ return buildHash('p3', null, params); }
function flagHref(f){
  if(Array.isArray(f.change_ids) && f.change_ids.length) return hrefAll({flag: f.id});
  if(f.campaign) return hrefAll({account: f.account, campaign: f.campaign});
  return hrefAll({rule: f.rule});
}
// A deep link replaces the filters (not the try-your-own thresholds) with exactly what it names.
function applyDeepLink(p){
  const keep = {sortKey: state.sortKey, sortDir: state.sortDir, minDays: state.minDays};
  Object.assign(state, defaultState(), keep);
  if(p.flag){
    const f = flagById(p.flag);
    if(f && Array.isArray(f.change_ids) && f.change_ids.length){ state.flagIds = new Set(f.change_ids); state.flagRef = String(f.id); }
    else if(f && f.campaign){ p = Object.assign({}, p, {account: f.account, campaign: f.campaign}); }
  }
  if(p.rule) state.flagRule = p.rule;
  if(p.campaign){ const k = campaignKeys(p.account, p.campaign); state.campaigns = k.camp.size ? k.camp : new Set([p.campaign]); if(p.account && k.acc.size) state.accounts = k.acc; }
  else if(p.account){ const s = new Set(); rowsAll().forEach(c=>{ if(accountName(c)===p.account || accountIdentity(c)===p.account) s.add(accountIdentity(c)||NONE); }); state.accounts = s.size ? s : new Set([p.account]); }
  if(p.user){ const s = userKeys(p.user); state.users = s.size ? s : new Set(['h:'+p.user]); }
  if(p.category) state.categories = new Set([p.category]);
}
// ---- filter controls -------------------------------------------------
const MS_IDS = ['fAccount','fCampaign','fUser','fCategory','fOperation','fSource','fSourceFile'];
const MS_ALL = {accounts:'all_accounts', campaigns:'all_campaigns', users:'all_users', categories:'all_categories', operations:'all_types', sources:'all_sources', sourceFiles:'all_files'};
const optionLabels = {};   // state key -> Map(value -> label)
function buildOptions(){
  const rows = DASH_DATA.changes;
  const add = (map, v, l)=>{ if(v!==null && v!==undefined && v!=='' && !map.has(v)) map.set(v, l); };
  const acc = new Map(), camp = new Map(), usr = new Map(), cat = new Map(), op = new Map(), src = new Map(), sf = new Map();
  rows.forEach(c=>{
    add(acc, accountIdentity(c)||NONE, accountName(c));
    add(camp, campaignIdentity(c)||NONE, campaignName(c));
    add(usr, actorKey(c), actorLabel(c));
    add(cat, c.category, catName(c.category));
    add(op, c.operation, (s=>s.length>80 ? s.slice(0,79)+'…' : s)(opName(firstLine(c.operation))));
    add(src, c.source, c.source);
    add(sf, c.source_file||NONE, c.source_file||t('no_file'));
  });
  Object.assign(optionLabels, {accounts:acc, campaigns:camp, users:usr, categories:cat, operations:op, sources:src, sourceFiles:sf});
}
function msFill(el){
  const key = el.dataset.state;
  const opts = [...optionLabels[key].entries()].sort((a,b)=> String(a[1]).localeCompare(String(b[1]), UI_LOCALE));
  el.querySelector('.ms-panel').innerHTML = `<button type="button" class="ms-clear">${escapeHtml(t('ms_clear'))}</button>` + opts.map(([v,l])=>
    `<label class="ms-opt"><input type="checkbox" value="${escapeHtml(v)}"> <span>${escapeHtml(l)}</span></label>`).join('');
}
function labelFor(key, v){ const m = optionLabels[key]; return (m && m.has(v)) ? m.get(v) : String(v); }
function advancedCount(){
  return state.accounts.size + (state.actor ? 1 : 0) + state.operations.size + state.sources.size + state.sourceFiles.size
    + (state.ruleMatch ? 1 : 0) + (state.range!=='all' ? 1 : 0) + (state.anchor!=='data' ? 1 : 0);
}
// F-10: every control is re-synced from state on each render.
function syncControls(){
  MS_IDS.forEach(id=>{
    const el = document.getElementById(id); const key = el.dataset.state; const sel = state[key];
    el.querySelectorAll('input[type=checkbox]').forEach(cb=>{ cb.checked = sel.has(cb.value); });
    const sum = el.querySelector('summary');
    sum.classList.toggle('has-sel', sel.size>0);
    const chip = el.classList.contains('chipms');
    document.getElementById(id+'Sum').textContent = !sel.size ? (chip ? '' : el.dataset.all) : sel.size===1 ? labelFor(key, [...sel][0]) : t('n_selected', {n: sel.size});
  });
  document.getElementById('fActor').value = state.actor;
  document.getElementById('fRuleMatch').value = state.ruleMatch;
  document.getElementById('fAnchor').value = state.anchor;
  const ff = document.getElementById('fFlag');
  if(state.flagRule && ![...ff.options].some(o=>o.value===state.flagRule)){ const o = document.createElement('option'); o.value = state.flagRule; o.textContent = ruleTitle(state.flagRule); ff.appendChild(o); }
  ff.value = state.flagRule;
  document.getElementById('fldFlag').classList.toggle('has-sel', !!state.flagRule);
  document.getElementById('ruleMatchesToggle').checked = ruleState.enabled;
  document.getElementById('baColToggle').checked = view.baCol;
  document.getElementById('fBaMetric').value = view.baMetric;
  document.getElementById('fBaMetric2').value = view.baMetric;
  document.getElementById('fMetricLine').value = view.metric;
  const inp = document.getElementById('fSearch'); if(document.activeElement!==inp) inp.value = state.search;
  document.querySelectorAll('#rangeBtns button').forEach(b=>{ const on = b.dataset.range===state.range; b.classList.toggle('active', on); b.setAttribute('aria-pressed', on); });
  document.querySelectorAll('#threshBtns button').forEach(b=>{ const on = parseInt(b.dataset.min)===state.minDays; b.classList.toggle('active', on); b.setAttribute('aria-pressed', on); });
  const n = advancedCount(), mb = document.getElementById('moreFilters');
  document.getElementById('moreCount').textContent = n ? ` (${n})` : '';
  mb.classList.toggle('has-sel', n>0);
}
function sourceFileCount(){ return optionLabels.sourceFiles ? [...optionLabels.sourceFiles.keys()].filter(k=>k!==NONE).length : 0; }
function closePopovers(except){ document.querySelectorAll('details.ms[open]').forEach(d=>{ if(d!==except) d.open = false; }); }
function setAdvanced(open){
  document.getElementById('advFilters').hidden = !open;
  document.getElementById('moreFilters').setAttribute('aria-expanded', open ? 'true' : 'false');
}
let searchTimer = null;
function initFilters(){
  buildOptions();
  MS_IDS.forEach(id=>{
    const el = document.getElementById(id); const key = el.dataset.state;
    el.dataset.all = t(MS_ALL[key]);
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
    const opt = document.createElement('option'); opt.value = r.id; opt.textContent = ruleLabel(r); fRuleMatch.appendChild(opt);
  });
  fRuleMatch.addEventListener('change', e=>{
    state.ruleMatch = e.target.value; state.page = 1;
    // choosing a rule is an explicit ask to see rule reasoning: turn highlighting on
    if(state.ruleMatch && !ruleState.enabled) ruleState.enabled = true;
    render();
  });
  // 2026-11-19: "Worth a look" filter, one option per rule that fired.
  const ff = document.getElementById('fFlag'), groups = ruleGroups();
  document.getElementById('fldFlag').hidden = !flagList().length;
  ff.innerHTML = `<option value="">${escapeHtml(t('flag_all'))}</option><option value="__any__">${escapeHtml(t('flag_any'))}</option>`
    + groups.map(g=>`<option value="${escapeHtml(g.rule)}">${escapeHtml(g.title)} (${fmtInt(g.flags.length)})</option>`).join('');
  ff.addEventListener('change', e=>{ state.flagRule = e.target.value; state.page = 1; render(); });
  document.getElementById('moreFilters').addEventListener('click', ()=>{ setAdvanced(document.getElementById('advFilters').hidden); });
  document.getElementById('fActor').addEventListener('change', e=>{ state.actor = e.target.value; state.page=1; render(); });
  document.getElementById('fAnchor').addEventListener('change', e=>{ state.anchor = e.target.value; state.page=1; render(); });
  document.getElementById('rangeBtns').addEventListener('click', e=>{
    const b = e.target.closest('button'); if(!b) return;
    state.range = b.dataset.range; state.page=1; render();
  });
  // C-5: debounced search (~150ms).
  document.getElementById('fSearch').addEventListener('input', e=>{
    state.search = e.target.value; state.page = 1;
    clearTimeout(searchTimer); searchTimer = setTimeout(render, 150);
  });
  // 2026-11-14: source-file filter only when the run read more than one file
  document.getElementById('fldSourceFile').hidden = sourceFileCount() < 2;
  const baOpts = baMetricList().map(([k,l])=>`<option value="${k}">${escapeHtml(l)}</option>`).join('');
  document.getElementById('fBaMetric').innerHTML = baOpts;
  document.getElementById('fBaMetric2').innerHTML = baOpts;
  if(!baMetricList().some(([k])=>k===view.baMetric)) view.baMetric = baMetricList()[0][0];
  document.getElementById('baCtl').hidden = !baData();
  document.getElementById('baColToggle').addEventListener('change', e=>{ view.baCol = e.target.checked; renderExplorer(getFiltered()); });
  document.getElementById('fBaMetric').addEventListener('change', e=>{ view.baMetric = e.target.value; render(); });
  document.getElementById('fBaMetric2').addEventListener('change', e=>{ view.baMetric = e.target.value; render(); });
  const lm = lineMetrics(metricsDaily());
  document.getElementById('tlMetricCtl').hidden = !lm.length;
  document.getElementById('fMetricLine').innerHTML = lm.map(([k,l])=>`<option value="${k}">${escapeHtml(l)}</option>`).join('') + `<option value="">${escapeHtml(t('off'))}</option>`;
  if(!lm.some(([k])=>k===view.metric)) view.metric = lm.length ? lm[0][0] : '';
  document.getElementById('fMetricLine').addEventListener('change', e=>{ view.metric = e.target.value; render(); });
  document.addEventListener('click', e=>{
    const b = e.target.closest && e.target.closest('.day-chip .chip'); if(!b) return;
    const d = state.day; state.day = ''; state.page = 1; render();
    const bar = document.querySelector(`#timeline .tl-bar[data-day="${d}"]`); if(bar && bar.offsetParent) bar.focus();
  });
  document.getElementById('flagChip').addEventListener('click', e=>{
    if(!e.target.closest('.chip')) return;
    state.flagIds = null; state.flagRef = ''; state.page = 1;
    replaceHash(buildHash('p3')); render(); document.getElementById('fSearch').focus();
  });
  const clear = ()=>{
    resetAll(); closePopovers(null);
    document.querySelectorAll('#ruleMatchesMagnitudeInputs input').forEach(inp=>{ inp.value = ruleState.pct[inp.dataset.ruleId]; inp.removeAttribute('aria-invalid'); });
    document.querySelectorAll('#ruleMatchesMagnitudeInputs .cfg-err').forEach(e=>{ e.textContent=''; });
    document.querySelectorAll('#ruleMatchesStateInputs input').forEach(cb=>{ cb.checked = ruleState.stateOn[cb.dataset.ruleId]; });
    if(/\?/.test(location.hash)) replaceHash(location.hash.replace(/\?.*$/, ''));
    render();
  };
  document.getElementById('clearFilters').addEventListener('click', clear);
  document.getElementById('p2Clear').addEventListener('click', ()=>{ clear(); document.getElementById('hS1').scrollIntoView({block:'nearest'}); });
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
    const name = campaignName(c);
    campDisplay[key] = accountSet.size <= 1 ? name : `${name} (${c.account_name||''})`;
  });
  const topCampKey = Object.entries(campCounts).sort((a,b)=>b[1]-a[1])[0];
  const stats = [
    [t('fs_changes'), fmtInt(filtered.length)], [t('fs_accounts'), fmtInt(accountSet.size)], [t('fs_users'), fmtInt(humanCount)],
    [t('fs_campaigns'), fmtInt(campaigns)], [t('fs_adgroups'), fmtInt(adgroups)],
    [t('fs_last'), last ? fmtTs(last) : '—'],
    [t('fs_top_user'), topUserKey ? (userDisplay[topUserKey[0]]||'—') : '—'], [t('fs_top_campaign'), topCampKey ? campDisplay[topCampKey[0]] : '—'],
  ];
  // Plain tiles: no colour should read as meaning something.
  document.getElementById('summaryGrid').innerHTML = stats.map(([l,n])=>
    `<div class="fstat"><div class="n">${escapeHtml(n)}</div><div class="l">${escapeHtml(l)}</div></div>`
  ).join('');
}
// Days are the source's own local days when its timezone is known (D-5);
// rows with no timezone can't be placed on a shared axis and are counted apart.
function renderTimeline(filtered){
  const counts = {}; let excluded = 0; const tzs = new Set();
  // same basis as the JSON (meta.timeline_reference_tz): UTC days only when the run had no zone
  const utcDays = dmeta().timeline_reference_tz==='UTC';
  filtered.forEach(c=>{
    const d = dayKey(c);
    if(d){ counts[d]=(counts[d]||0)+1; if(c.timezone) tzs.add(c.timezone); }
    else excluded++;
  });
  const basis = utcDays ? t('tl_basis_utc') : tzs.size===1 ? t('tl_basis_tz', {tz: [...tzs][0]}) : tzs.size>1 ? t('tl_basis_multi') : t('tl_basis');
  document.getElementById('timelineBasis').textContent = basis;
  const daily = view.metric ? metricsDaily() : null;
  const mLabel = daily ? (lineMetrics(daily).find(([k])=>k===view.metric)||[])[1] : null;
  const axis = timelineAxis(counts, mLabel ? daily : null);
  const max = Math.max(1,...Object.values(counts));
  const el = document.getElementById('timeline');
  document.getElementById('timelineReadout').textContent = '';
  document.getElementById('timelineNote').innerHTML = excluded ? `<div class="note">${escapeHtml(tn('tl_excluded', excluded))}</div>` : '';
  renderDayChips();
  if(!axis.length){ el.innerHTML = `<div class="empty">${escapeHtml(t('tl_empty'))}</div>`; document.getElementById('tlAxis').innerHTML = ''; drawMetricLine(null); return; }
  const firstDay = Object.keys(counts).sort()[0], lastDay = Object.keys(counts).sort().pop();
  el.innerHTML = axis.map(d=>{
    if(!counts[d]) return '<span class="tl-slot" aria-hidden="true"></span>';
    const edge = d===firstDay || d===lastDay;
    const h = Math.max(4, Math.round(counts[d]/max*100));
    const mv = mLabel ? numOrNull((daily[d]||{})[view.metric]) : null;
    const text = tn('tl_bar', counts[d], {d: fmtWall(d,false)}) + (mLabel ? ` · ${lc(mLabel)} ${fmtNum(mv)}` : '');
    // one colour, height only; first/last day lighter (may be partial)
    return `<button type="button" class="tl-bar${edge?' edge':''}" data-day="${d}" aria-pressed="${state.day===d}" aria-label="${escapeHtml(text)}" data-text="${escapeHtml(text)}" style="height:${h}%;"><span class="tip" aria-hidden="true">${escapeHtml(text)}</span></button>`;
  }).join('');
  drawMetricLine(mLabel ? {axis, daily, key:view.metric, label:mLabel, counts} : null);
  document.getElementById('tlAxis').innerHTML = `<span>${escapeHtml(fmtWall(axis[0], false))}</span><span>${escapeHtml(fmtWall(axis[axis.length-1], false))}</span>`;
  const show = e=>{ const b = e.target.closest('.tl-bar'); if(b) document.getElementById('timelineReadout').textContent = b.dataset.text; };
  if(!el._bound){
    el.addEventListener('focusin', show);
    el.addEventListener('click', e=>{
      const b = e.target.closest('.tl-bar'); if(!b) return;
      state.day = state.day===b.dataset.day ? '' : b.dataset.day; state.page = 1; render(); show(e);
    });
    el._bound = true;
  }
}
function lc(s){ try { return String(s).toLocaleLowerCase(UI_LOCALE || undefined); } catch(e){ return String(s).toLowerCase(); } }
// The day chips (timeline and Explorer) clear the day filter.
function renderDayChips(){
  const label = state.day ? fmtWall(state.day, false) : '';
  document.querySelectorAll('.day-chip').forEach(slot=>{
    slot.innerHTML = state.day ? `<button type="button" class="chip" aria-label="${escapeHtml(t('day_chip_aria', {d: label}))}">${escapeHtml(t('day_chip', {d: label}))} <span aria-hidden="true">×</span></button>` : '';
  });
}
// Neutral grey line of one daily metric under the bars, x = each day's bar centre; dots = days with changes.
let lastMetricLine = null;
function drawMetricLine(spec){
  lastMetricLine = spec;
  const svg = document.getElementById('tlMetric'), tl = document.getElementById('timeline');
  const note = document.getElementById('tlMetricNote');
  if(!spec){ svg.style.display = 'none'; svg.innerHTML = ''; if(note) note.textContent = ''; return; }
  const {axis, daily, key, label, counts} = spec;
  const kids = [...tl.children];
  const vals = axis.map(d=> numOrNull((daily[d]||{})[key]));
  const present = vals.filter(v=>v!=null);
  if(!present.length || kids.length!==axis.length){ svg.style.display = 'none'; svg.innerHTML = ''; note.textContent = t('ml_none', {m: lc(label)}); return; }
  const hi = Math.max(...present), lo = Math.min(0, ...present), H = 44, pad = 5, W = Math.max(tl.scrollWidth, 1);
  const x = i=> kids[i].offsetLeft + kids[i].offsetWidth/2;
  const y = v=> (H - pad - (v-lo)/((hi-lo)||1)*(H-2*pad)).toFixed(1);
  let d = '', pen = false;
  vals.forEach((v,i)=>{ if(v==null){ pen = false; return; } d += `${pen ? 'L' : 'M'}${x(i).toFixed(1)} ${y(v)} `; pen = true; });
  const dots = axis.map((day,i)=> counts[day] && vals[i]!=null ? `<circle class="mk" cx="${x(i).toFixed(1)}" cy="${y(vals[i])}" r="3"></circle>` : '').join('');
  svg.setAttribute('width', W); svg.setAttribute('height', H); svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.setAttribute('aria-label', t('ml_aria', {m: lc(label), lo: fmtNum(Math.min(...present)), hi: fmtNum(hi)}));
  svg.innerHTML = `<path class="ml" d="${d.trim()}"></path>${dots}`;
  svg.style.display = 'block';
  note.textContent = t('ml_note', {m: lc(label), lo: fmtNum(Math.min(...present)), hi: fmtNum(hi)});
}
// items: {key, name, count, color?}; each bar is a real button (keyboard and screen-reader operable).
function barList(container, items, onClick, color, selected){
  const max = Math.max(1, ...items.map(i=>i.count));
  container.innerHTML = items.length ? '<div class="bars">' + items.map(i=>{
    const col = i.color || color || 'var(--bar)';
    const key = i.key ?? i.name;
    return `<button type="button" class="bar-row" data-key="${escapeHtml(key)}" aria-pressed="${selected && selected.has(key) ? 'true' : 'false'}">
      <span class="bar-label" title="${escapeHtml(i.name)}">${escapeHtml(i.name)}</span>
      <span class="bar-track"><span class="bar-fill" style="${barFillStyle(col, i.count/max*100)}"></span></span>
      <span class="bar-n">${fmtInt(i.count)}</span>
    </button>`;
  }).join('') + '</div>' : `<div class="empty">${escapeHtml(t('no_data'))}</div>`;
  if(onClick) container.querySelectorAll('.bar-row').forEach(row=> row.addEventListener('click', ()=> onClick(row.dataset.key)));
}
function renderUsers(filtered){
  const groups = {h:{}, a:{}, u:{}}, disp = {};
  filtered.forEach(c=>{ const k = actorKey(c); const g = k[0]; groups[g][k] = (groups[g][k]||0)+1; disp[k] = actorLabel(c); });
  const list = g => Object.entries(groups[g]).sort((a,b)=>b[1]-a[1]).map(([key,count])=>({key, name: g==='a' ? key.slice(2) : disp[key], count}));
  const el = document.getElementById('userActivity');
  el.innerHTML = `<div class="grp-label">${escapeHtml(t('actor_human'))}</div><div id="humanBars"></div><div class="grp-label">${escapeHtml(t('actor_auto'))}</div><div id="autoBars"></div><div class="grp-label">${escapeHtml(t('actor_unknown'))}</div><div id="unknownBars"></div>`;
  const pick = key=>{ toggleOnly('users', key); render(); };
  [['humanBars','h','var(--bar)'], ['autoBars','a','var(--s3)'], ['unknownBars','u','var(--bar-soft)']].forEach(([id, g, col])=>{
    const l = list(g), box = document.getElementById(id);
    if(l.length) barList(box, l, pick, col, state.users); else box.innerHTML = `<p class="note grp-none">${escapeHtml(t('grp_none'))}</p>`;
  });
}
const openAccounts = new Set();
// F-9: the tree is keyed by account identity, not display name.
function renderAccounts(filtered){
  const accMap = {}, accName = {}, campName = {};
  filtered.forEach(c=>{
    const ak = accountIdentity(c) || NONE;
    accName[ak] = accountName(c);
    const ck = campaignIdentity(c) || NONE;
    accMap[ak] = accMap[ak] || {};
    accMap[ak][ck] = (accMap[ak][ck]||0)+1;
    campName[ck] = campaignName(c);
  });
  const accounts = Object.entries(accMap).map(([key,camps])=>({key, camps, total: Object.values(camps).reduce((a,b)=>a+b,0)})).sort((a,b)=>b.total-a.total);
  const el = document.getElementById('accountActivity');
  if(!accounts.length){ el.innerHTML=`<div class="empty">${escapeHtml(t('no_data'))}</div>`; return; }
  if(accounts.length===1) openAccounts.add(accounts[0].key);
  const maxAcc = Math.max(1, ...accounts.map(a=>a.total));
  const col = 'var(--bar)';
  el.innerHTML = accounts.map((a,idx)=>{
    const open = openAccounts.has(a.key);
    const cmax = Math.max(...Object.values(a.camps));
    return `
    <div class="acc-block">
      <button type="button" class="acc-head" data-acc="${escapeHtml(a.key)}" aria-expanded="${open}" aria-controls="camp-${idx}">
        <span class="chev" aria-hidden="true">▶</span>
        <span class="bar-label">${escapeHtml(accName[a.key])}</span>
        <span class="bar-track"><span class="bar-fill" style="${barFillStyle(col, a.total/maxAcc*100)}"></span></span>
        <span class="bar-n">${fmtInt(a.total)}</span>
      </button>
      <div class="camp-list" id="camp-${idx}"${open ? '' : ' hidden'}>
        ${Object.entries(a.camps).sort((x,y)=>y[1]-x[1]).map(([ck,n])=>{
          const on = state.campaigns.size===1 && state.campaigns.has(ck) && state.accounts.has(a.key);
          return `<button type="button" class="bar-row" data-acc="${escapeHtml(a.key)}" data-camp="${escapeHtml(ck)}" aria-pressed="${on}"><span class="bar-label" title="${escapeHtml(campName[ck])}">${escapeHtml(campName[ck])}</span><span class="bar-track"><span class="bar-fill" style="${barFillStyle(col, n/cmax*100)}opacity:.75;"></span></span><span class="bar-n">${fmtInt(n)}</span></button>`;
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
  const list = Object.entries(counts).sort((a,b)=>b[1]-a[1]).map(([key,count])=>({key, name: catName(key), count, color: key==='Other' ? 'var(--bar-soft)' : null}));
  barList(document.getElementById('categoryDist'), list, key=>{ toggleOnly('categories', key); render(); }, 'var(--bar)', state.categories);
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
    el.innerHTML = `<div class="empty">${escapeHtml(t('rm_off'))}</div>`;
    return;
  }
  const matched = filtered.map(c=>({c, matches:computeRuleMatches(c)})).filter(x=>x.matches.length);
  countEl.textContent = t('rm_count', {a: fmtInt(matched.length), b: fmtInt(filtered.length)});
  if(!matched.length){ el.innerHTML = `<div class="empty">${escapeHtml(t('rm_none'))}</div>`; return; }
  const SHOWN = 500;
  // C-7: the % and the old -> new values are visible text, not only a tooltip.
  el.innerHTML = `<div class="tbl-wrap" tabindex="0" role="region" aria-label="${escapeHtml(t('rm_tbl_aria'))}"><table><thead><tr><th>${escapeHtml(t('c_date'))}</th><th>${escapeHtml(t('c_campaign'))}</th><th>${escapeHtml(t('c_change'))}</th><th>${escapeHtml(t('rm_oldnew'))}</th><th>${escapeHtml(t('rm_matched'))}</th></tr></thead><tbody>${
    matched.slice(0, SHOWN).map(({c,matches})=>`<tr class="datarow" tabindex="0" data-idx="${rowIndex(c)}">
      <td>${escapeHtml(fmtTs(c))}</td>
      <td>${escapeHtml(c.campaign_name || (c.campaign_id ? t('campaign_n', {id: c.campaign_id}) : '—'))}</td>
      <td>${escapeHtml(c.field_name || subName(c.subcategory) || '—')}</td>
      <td>${escapeHtml(valueText(c,'old') || '—')} → ${escapeHtml(valueText(c,'new') || '—')}</td>
      <td style="white-space:normal;min-width:180px;">${matches.map(m=>`<span class="pill pill-rule">${escapeHtml(m.label)}</span><span class="rm-detail">${escapeHtml(m.detail)}</span>`).join('')}</td>
    </tr>`).join('')
  }</tbody></table></div>${matched.length > SHOWN ? `<div class="note">${escapeHtml(t('rm_first', {n: SHOWN, m: matched.length}))}</div>` : ''}`;
  bindRowOpen(el);
}
function initRuleMatchesConfig(){
  const magEl = document.getElementById('ruleMatchesMagnitudeInputs');
  magEl.innerHTML = MAGNITUDE_RULES.map(r=>`
    <label class="cfg-field">
      ${escapeHtml(ruleLabel(r))}
      <span style="display:flex;align-items:center;gap:6px;">±<input type="number" class="ctl" min="1" max="${PCT_MAX}" step="any" value="${r.defaultPct}" data-rule-id="${r.id}" aria-describedby="err-${r.id}" style="width:80px;">%</span>
      <span class="cfg-err" id="err-${r.id}" aria-live="polite"></span>
    </label>`).join('');
  magEl.querySelectorAll('input[type=number]').forEach(inp=>{
    inp.addEventListener('input', e=>{
      const id = e.target.dataset.ruleId; const res = parseThreshold(e.target.value);
      const err = document.getElementById('err-'+id);
      if(res.ok){ ruleState.pct[id] = res.value; e.target.removeAttribute('aria-invalid'); err.textContent = ''; render(); }
      else { e.target.setAttribute('aria-invalid', 'true'); err.textContent = t('thr_err', {msg: res.msg, max: PCT_MAX, v: ruleState.pct[id]}); }
    });
  });
  const stateEl = document.getElementById('ruleMatchesStateInputs');
  stateEl.innerHTML = STATE_RULES.map(r=>`
    <label class="cfg-check">
      <input type="checkbox" ${r.defaultOn ? 'checked' : ''} data-rule-id="${r.id}"> ${escapeHtml(ruleLabel(r))}
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
      campaignLast[key] = { campaign_name: campaignName(c), account_name: c.account_name, row: c, _t: t };
    }
  });
  const now = Date.now();
  const rows = Object.values(campaignLast).map(u=>({...u, days_since: Math.floor((now - u._t) / 86400000)}))
    .sort((a,b)=> b.days_since - a.days_since).filter(u=>u.days_since >= state.minDays);
  const el = document.getElementById('untouched');
  if(!rows.length){ el.innerHTML=`<div class="tcard"><div class="empty">${escapeHtml(t('ut_empty'))}</div></div>`; return; }
  el.innerHTML = `<div class="tcard"><div class="tbl-wrap" tabindex="0" role="region" aria-label="${escapeHtml(t('ut_tbl_aria'))}"><table><thead><tr><th>${escapeHtml(t('c_campaign'))}</th><th>${escapeHtml(t('c_account'))}</th><th>${escapeHtml(t('ct_last'))}</th><th class="num">${escapeHtml(t('ut_days'))}</th></tr></thead><tbody>${
    rows.map(u=>`<tr><td>${escapeHtml(u.campaign_name)}</td><td>${escapeHtml(u.account_name||'')}</td><td>${escapeHtml(fmtTs(u.row))}</td><td class="num">${fmtInt(u.days_since)}</td></tr>`).join('')
  }</tbody></table></div></div>`;
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
// Columns that would hold one repeated value (one account, one source) stay hidden; the detail still shows them.
function explorerCols(){
  return { account: !!(optionLabels.accounts && optionLabels.accounts.size > 1), source: !!(optionLabels.sources && optionLabels.sources.size > 1),
    flags: hasFlagData(), matches: !!ruleState.enabled, ba: !!(view.baCol && baData()) };
}
function flagTagsHtml(c){
  const seen = new Set();
  return flagsForChange(c).filter(f=>{ if(seen.has(f.rule)) return false; seen.add(f.rule); return true; })
    .map(f=>`<span class="tag${f.tier==='info' ? ' info' : ''}" title="${escapeHtml(flagText(f))}">${escapeHtml(ruleShort(f.rule))}</span>`).join('');
}
function renderExplorer(filtered){
  const sorted = [...filtered].sort((a,b)=>{ const k = state.sortKey; const cmp = compareExplorerSortValues(explorerSortValue(a,k), explorerSortValue(b,k)); return cmp<0 ? -state.sortDir : cmp>0 ? state.sortDir : 0; });
  const totalPages = Math.max(1, Math.ceil(sorted.length / state.pageSize));
  state.page = Math.max(1, Math.min(state.page, totalPages));
  const pageRows = sorted.slice((state.page-1)*state.pageSize, state.page*state.pageSize);
  const body = document.getElementById('explorerBody');
  const cols = explorerCols();
  document.getElementById('baTh').hidden = !cols.ba;
  document.querySelectorAll('#explorerTable th[data-col]').forEach(th=>{ th.hidden = !cols[th.dataset.col]; });
  const ncol = 9 + ['account','source','flags','matches','ba'].filter(k=>cols[k]).length;
  body.innerHTML = pageRows.length ? pageRows.map(c=>{
    const summary = firstLine(c.raw_summary);
    return `
    <tr class="datarow" tabindex="0" data-idx="${rowIndex(c)}">
      <td>${escapeHtml(fmtTs(c))}</td>
      <td>${c.actor_type==='human' ? escapeHtml(c.user_name||c.user_email||'—') : c.actor_type==='automation' ? `<span class="pill pill-auto">${escapeHtml(c.client_type||c.user_name||t('automation_lc'))}</span>` : (c.user_name||c.user_email) ? `<span class="pill">${escapeHtml(actorLabel(c))}</span>` : `<span class="pill">${escapeHtml(t('unknown_pill'))}</span>`}</td>
      ${cols.account ? `<td>${escapeHtml(c.account_name||'')}</td>` : ''}<td${c.campaign_confidence==='inferred_from_summary' ? ` title="${escapeHtml(t('camp_inferred_tip'))}"` : ''}>${escapeHtml(c.campaign_name||'—')}</td><td>${escapeHtml(c.ad_group_name||'—')}</td>
      <td>${escapeHtml(catName(c.category))}</td><td>${escapeHtml(c.field_name || subName(c.subcategory) || '—')}</td>
      <td title="${escapeHtml(c.raw_summary||'')}">${escapeHtml(summary||'—')}</td>
      <td>${valueCellHtml(c,'old')}</td>
      <td>${valueCellHtml(c,'new')}</td>
      ${cols.flags ? `<td>${flagTagsHtml(c) || '—'}</td>` : ''}
      ${cols.matches ? `<td>${ruleMatchPillsHtml(computeRuleMatches(c)) || '—'}</td>` : ''}
      ${cols.ba ? `<td class="num" title="${escapeHtml(t('ba_cell_tip'))}">${escapeHtml(baCellText(baFor(c), view.baMetric))}</td>` : ''}
      ${cols.source ? `<td>${escapeHtml(c.source)}</td>` : ''}
    </tr>`; }).join('') : `<tr><td colspan="${ncol}" class="empty">${escapeHtml(t('exp_empty'))}</td></tr>`;
  bindRowOpen(body);
  document.getElementById('pagerInfo').textContent = t('pager', {p: fmtInt(state.page), n: fmtInt(totalPages), r: fmtInt(sorted.length)});
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
  const vText = which=>{ const v = valueText(c, which); return v ? v + (c.value_confidence==='parsed_from_summary' ? t('d_parsed_suffix') : '') : '—'; };
  const hasValue = valueParts(c,'old') || valueParts(c,'new');
  const grp = eventGroup(c);
  let stmt = null;
  let fullCell = null;
  if(grp){
    const ordered = grp.every(r=>typeof r.statement_index==='number') ? [...grp].sort((a,b)=>a.statement_index-b.statement_index) : grp;
    const pos = typeof c.statement_index==='number' ? c.statement_index+1 : ordered.indexOf(c)+1;
    stmt = t('d_stmt', {pos, n: c.statement_count || grp.length});
    fullCell = ordered.map(r=>r.raw_summary||'').join('\n');
  }
  const user = c.actor_type==='human' ? (c.user_name||c.user_email||'—') : c.actor_type==='automation' ? t('actor_auto_label', {name: c.client_type||c.user_name||t('actor_auto')}) : ((c.user_name||c.user_email) ? t('actor_unknown_label', {name: c.user_name||c.user_email}) : t('actor_unknown_none'));
  const rows = [
    ['date', fmtTs(c) + (c.timezone ? ` (${c.timezone})` : (c.timestamp_utc ? '' : t('d_tz_unknown')))],
    ['user', user],
    ['account', c.account_name||'—'],
    ['campaign', (c.campaign_name||'—') + (c.campaign_confidence==='inferred_from_summary' ? t('d_camp_inferred') : '')],
    ['adgroup', c.ad_group_name||'—'],
    ['category', `${catName(c.category)} · ${subName(c.subcategory)}${c.category_confidence==='fallback_other' ? t('d_no_rule') : ''}`], ['field', c.field_name || '—'],
    ['previous', vText('old')], ['new', vText('new')],
    ['operation', c.operation_confidence==='inferred' ? t('d_op_inferred', {op: opName(c.operation)}) : opName(c.operation)],
    ['source', c.source],
    ['valuefrom', !hasValue ? t('d_val_none') : c.value_confidence==='parsed_from_summary' ? t('d_val_parsed') : t('d_val_struct')],
    ['summary', c.raw_summary || '—'],
  ];
  if(stmt) rows.push(['statement', stmt], ['fullcell', fullCell]);
  rows.push(['resource', c.resource_path || c.resource_id || '—'], ['eventid', c.source_event_id || t('d_derived', {id: c.change_id})]);
  if(c.source_file) rows.push(['file', c.source_file]);
  document.getElementById('detailBody').innerHTML = rows.map(([k,v])=>`<dt>${escapeHtml(t('d_'+k))}</dt><dd${k==='summary' || k==='operation' || k==='fullcell' ? ' class="pre"' : ''}>${escapeHtml(v)}</dd>`).join('');
  const fl = flagsForChange(c);
  document.getElementById('detailFlags').innerHTML = fl.length ? `<div class="ba"><h4>${escapeHtml(t('d_flags'))}</h4><ul class="dflags">${fl.map(f=>{
    const p = fmtParams((ruleCfg(f.rule)||{}).params);
    return `<li><span class="tag${f.tier==='info' ? ' info' : ''}">${escapeHtml(ruleTitle(f.rule))}</span> ${escapeHtml(flagText(f))}${p ? ` <span class="muted">(${escapeHtml(t('rule_fired', {p}))})</span>` : ''}</li>`; }).join('')}</ul></div>` : '';
  document.getElementById('detailBA').innerHTML = baDetailHtml(c);
  const ov = document.getElementById('detailOverlay');
  // an overlapping-change link opens inside the open dialog: keep the original opener for focus return
  if(!ov.classList.contains('open')) lastFocus = document.activeElement;
  ov.classList.add('open');
  document.getElementById('detailCard').scrollTop = 0;
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
  const span = state.range==='1' ? t('span_24h') : t('span_days', {n: state.range});
  return state.anchor==='today' ? t('range_today', {span, d: fmtEpochDate(Date.now())}) : t('range_data', {span, d: fmtEpochDate(dataEndTime())});
}
function filterParts(){
  const parts = [];
  const r = rangeLabel(); if(r) parts.push(r);
  const named = (key, one, many)=>{ const s = state[key]; if(s.size===1) parts.push(`${t(one)}: ${labelFor(key, [...s][0])}`); else if(s.size) parts.push(t(many, {n: s.size})); };
  named('accounts','f_account','many_accounts'); named('campaigns','f_campaign','many_campaigns'); named('users','f_user','many_users');
  if(state.actor) parts.push(`${t('f_actor')}: ${t({human:'actor_human', automation:'actor_auto', unknown:'actor_unknown'}[state.actor] || 'actor_unknown')}`);
  named('categories','f_category','many_categories'); named('operations','f_operation','many_operations'); named('sources','f_source','many_sources'); named('sourceFiles','f_file','many_files');
  if(state.day) parts.push(t('day_chip', {d: fmtWall(state.day, false)}));
  if(state.flagRule) parts.push(`${t('f_flag')}: ${state.flagRule==='__any__' ? t('flag_any') : ruleTitle(state.flagRule)}`);
  if(state.flagIds) parts.push(t('flag_chip', {x: flagRefLabel()}));
  if(state.ruleMatch){ const rr = [...MAGNITUDE_RULES, ...STATE_RULES].find(x=>x.id===state.ruleMatch); parts.push(`${t('f_rule')}: ${rr ? ruleLabel(rr) : t('rm_any')}`); }
  if(state.search) parts.push(t('ctx_search', {q: state.search}));
  return parts;
}
function flagRefLabel(){ const f = flagById(state.flagRef); return f ? `${ruleTitle(f.rule)}${f.campaign ? ' · '+f.campaign : ''}` : String(state.flagRef || ''); }
function renderFilterContext(n){
  const parts = filterParts();
  document.getElementById('filterContext').textContent = parts.length ? t('showing', {x: parts.join(' · ')}) : t('showing_all');
  document.getElementById('flagChip').innerHTML = state.flagIds ? `<button type="button" class="chip" aria-label="${escapeHtml(t('flag_chip_aria', {x: flagRefLabel()}))}">${escapeHtml(t('flag_chip', {x: flagRefLabel()}))} <span aria-hidden="true">×</span></button>` : '';
  const p2 = document.getElementById('p2Filter');
  p2.hidden = !parts.length;
  if(parts.length){
    document.getElementById('p2FilterText').textContent = t('showing', {x: parts.join(' · ')});
    const a = document.getElementById('p2ToAll'); a.textContent = t('open_all', {n: fmtInt(n)}); a.href = buildHash('p3');
  }
}
// Re-rendering replaces the bar/row buttons; put keyboard focus back on the same item.
function focusMemo(){
  const ae = document.activeElement;
  if(!ae || !ae.dataset || !ae.closest) return null;
  const host = ae.closest('#userActivity,#accountActivity,#categoryDist,#explorerBody,#ruleMatches,#timeline,#accountRollup,#ba2Table');
  return host ? {host: host.id, tag: ae.tagName, cls: ae.className, ds: {...ae.dataset}} : null;
}
function focusRestore(f){
  if(!f) return;
  const host = document.getElementById(f.host); if(!host) return;
  const hit = [...host.querySelectorAll(f.tag)].find(el=> el.className===f.cls && Object.keys(f.ds).every(k=>el.dataset[k]===f.ds[k]));
  if(hit) hit.focus();
}
// Per-account rollup (round 10): shown when the data holds more than one account or input file.
function renderRollup(filtered){
  const sec = document.getElementById('rollupSection');
  const files = sourceFilesMeta(dmeta());
  const multi = (optionLabels.accounts && optionLabels.accounts.size > 1) || files.length > 1 || sourceFileCount() > 1;
  sec.hidden = !multi;
  const fe = document.getElementById('inputFiles');
  fe.innerHTML = files.length ? `<h3 class="h3">${escapeHtml(t('if_h'))}</h3><div class="tbl-wrap" tabindex="0" role="region" aria-label="${escapeHtml(t('if_aria'))}"><table><thead><tr><th>${escapeHtml(t('if_file'))}</th><th class="num">${escapeHtml(t('if_rows'))}</th><th>${escapeHtml(t('if_accounts'))}</th><th>${escapeHtml(t('c_source'))}</th><th>${escapeHtml(t('if_first'))}</th><th>${escapeHtml(t('if_last'))}</th></tr></thead><tbody>${
    files.map(f=>`<tr><td>${escapeHtml(f.file)}</td><td class="num">${numOrNull(f.rows)!=null ? fmtInt(f.rows) : '—'}</td><td>${escapeHtml(Array.isArray(f.accounts) ? f.accounts.join(', ') : (f.accounts ?? '—'))}</td><td>${escapeHtml(f.source_label || '—')}</td><td>${escapeHtml(f.date_min ? fmtWall(f.date_min, false) : '—')}</td><td>${escapeHtml(f.date_max ? fmtWall(f.date_max, false) : '—')}</td></tr>`).join('')
  }</tbody></table></div>` : '';
  if(!multi) return;
  const list = accountRollup(filtered), anyFiles = list.some(a=>a.files.length);
  const el = document.getElementById('accountRollup');
  el.innerHTML = list.length ? `<div class="tbl-wrap" tabindex="0" role="region" aria-label="${escapeHtml(t('ro_aria'))}"><table><thead><tr><th>${escapeHtml(t('c_account'))}</th><th class="num">${escapeHtml(t('ct_changes'))}</th><th class="num">${escapeHtml(t('ro_camps'))}</th><th class="num">${escapeHtml(t('ro_users'))}</th><th class="num">${escapeHtml(t('ro_auto'))}</th><th>${escapeHtml(t('ro_first'))}</th><th>${escapeHtml(t('ct_last'))}</th>${anyFiles ? `<th>${escapeHtml(t('ro_files'))}</th>` : ''}</tr></thead><tbody>${
    list.map(a=>`<tr><td><button type="button" class="link-btn" data-acc="${escapeHtml(a.key)}" aria-pressed="${state.accounts.size===1 && state.accounts.has(a.key)}">${escapeHtml(a.name)}</button></td><td class="num">${fmtInt(a.changes)}</td><td class="num">${fmtInt(a.campaigns)}</td><td class="num">${fmtInt(a.users)}</td><td class="num">${fmtInt(a.automation)}</td><td>${escapeHtml(a.first ? fmtTs(a.first) : '—')}</td><td>${escapeHtml(a.last ? fmtTs(a.last) : '—')}</td>${anyFiles ? `<td>${escapeHtml(a.files.join(', ') || '—')}</td>` : ''}</tr>`).join('')
  }</tbody></table></div>` : `<div class="empty">${escapeHtml(t('no_data'))}</div>`;
  el.querySelectorAll('button[data-acc]').forEach(b=> b.addEventListener('click', ()=>{ toggleOnly('accounts', b.dataset.acc); render(); }));
}
function renderCoverage(){
  const notes = coverageNotes(), mtxt = metricsNoteText();
  document.getElementById('coverageSection').hidden = !notes.length && !mtxt;
  document.getElementById('coverageList').innerHTML = notes.map(n=>`<li${n.id ? ` data-id="${escapeHtml(n.id)}"` : ''}>${escapeHtml(coverageText(n))}</li>`).join('');
  document.getElementById('metricsNote').textContent = mtxt;
}
// Page 2, section 6: changes with a before/after window, newest first (follows the filters).
function renderBA2(filtered){
  const ba = baData();
  document.getElementById('s6').hidden = !ba;
  const li6 = document.querySelector('#sideList a[data-sec="s6"]'); if(li6) li6.parentElement.hidden = !ba;
  if(!ba) return;
  document.getElementById('ba2Note').textContent = metricsNoteText();
  const rows = filtered.filter(c=>baFor(c)).sort((a,b)=> (rowTime(b)||0) - (rowTime(a)||0));
  const el = document.getElementById('ba2Table');
  if(!rows.length){ el.innerHTML = `<div class="empty">${escapeHtml(t('ba2_none'))}</div>`; return; }
  const SHOWN = 100, k = view.baMetric;
  el.innerHTML = `<div class="tbl-wrap" tabindex="0" role="region" aria-label="${escapeHtml(t('ba2_aria'))}"><table><thead><tr><th>${escapeHtml(t('c_date'))}</th><th>${escapeHtml(t('c_campaign'))}</th><th>${escapeHtml(t('c_change'))}</th><th class="num">${escapeHtml(t('ba_before'))}</th><th class="num">${escapeHtml(t('ba_after'))}</th><th class="num">${escapeHtml(t('ba_diff'))}</th><th class="num">${escapeHtml(t('ba_pct'))}</th></tr></thead><tbody>${
    rows.slice(0, SHOWN).map(c=>{ const b = baFor(c), r = baMetricRow(b, k), mat = baMaturing(b);
      return `<tr class="datarow" tabindex="0" data-idx="${rowIndex(c)}"><td>${escapeHtml(fmtTs(c))}</td><td>${escapeHtml(campaignName(c))}</td><td>${escapeHtml(c.field_name || subName(c.subcategory) || '—')}</td><td class="num">${fmtNum(r.before)}</td><td class="num">${fmtNum(r.after)}${mat ? '*' : ''}</td><td class="num">${fmtSigned(r.diff)}</td><td class="num">${fmtSigned(r.pct, 1, '%')}</td></tr>`; }).join('')
  }</tbody></table></div>` + (rows.length > SHOWN ? `<p class="note" style="padding:0 12px 10px;">${escapeHtml(t('ba2_first', {n: SHOWN, m: fmtInt(rows.length)}))}</p>` : '');
  bindRowOpen(el);
}
function render(){
  clearTimeout(searchTimer);
  const memo = focusMemo();
  const allDays = getFiltered({ignoreDay:true});
  const filtered = state.day ? allDays.filter(c=>dayKey(c)===state.day) : allDays;
  syncControls();
  renderFilterContext(filtered.length);
  renderSummary(filtered); renderTimeline(allDays); renderUsers(filtered);
  renderAccounts(filtered); renderRollup(filtered); renderCategories(filtered); renderRuleMatches(filtered);
  renderUntouched(filtered); renderBA2(filtered); renderExplorer(filtered);
  numberSections();
  focusRestore(memo);
}
// ---- page 1: summary (2026-11-19) -------------------------------------
function emFirstNumber(s){
  const m = /\d(?:[\d.,]*\d)?/.exec(s);
  if(!m) return escapeHtml(s);
  return escapeHtml(s.slice(0, m.index)) + '<em>' + escapeHtml(m[0]) + '</em>' + escapeHtml(s.slice(m.index + m[0].length));
}
// Numbers in the lede are bold with the word that follows them ("3 kampanyada", "₺1.240 arttı").
function boldNumbers(s){
  const re = /[+−-]?%?[₺$€£]?\d(?:[\d.,]*\d)?%?(?:\s+[A-Za-zÇĞİÖŞÜçğıöşüÂâÎîÛû']+)?/g, glue = /\s+(of|in|to|by|for|and|or|on|at|the|a|an|from|with|ve|ile|ya|da|de)$/i;
  let out = '', i = 0, m;
  while((m = re.exec(s))){ const w = m[0].replace(glue, ''); out += escapeHtml(s.slice(i, m.index)) + '<b>' + escapeHtml(w) + '</b>'; i = m.index + w.length; re.lastIndex = i; }
  return out + escapeHtml(s.slice(i));
}
function accountNames(){ const s = new Set(); rowsAll().forEach(c=>{ const a = c.account_name || (c.account_id ? t('account_n', {id: c.account_id}) : ''); if(a) s.add(a); }); return [...s]; }
function tzLabel(tz){
  if(!tz) return t('tz_unknown');
  if(tz==='UTC') return 'UTC';
  const city = String(tz).split('/').pop().replace(/_/g, ' ');
  return LANG()==='tr' && city==='Istanbul' ? 'İstanbul' : city;
}
function rowSpan(){
  let first = null, last = null;
  rowsAll().forEach(c=>{ const tt = rowTime(c); if(tt==null) return; if(!first || tt<rowTime(first)) first = c; if(!last || tt>rowTime(last)) last = c; });
  return {first, last};
}
function renderHero(){
  const es = execSummary(), m = dmeta(), rows = rowsAll();
  const accs = accountNames();
  const acct = accs.length===1 ? accs[0] : accs.length>1 ? tn('n_accounts', accs.length) : '';
  document.getElementById('eyebrow').textContent = acct ? `${acct} · Google Ads` : 'Google Ads';
  const total = numOrNull(m.row_count) ?? rows.length;
  const head = (es && loc(es.headline)) || tn('headline_fb', total);
  document.getElementById('headline').innerHTML = emFirstNumber(head);
  const sent = es && es.sentences ? (Array.isArray(es.sentences[LANG()]) ? es.sentences[LANG()] : Array.isArray(es.sentences.en) ? es.sentences.en : []) : [];
  let lede = sent.filter(x=>typeof x==='string' && x).join(' ');
  if(!lede){
    const sm = asObj(DASH_DATA.summary) || {};
    const camps = numOrNull(sm.changed_campaigns) ?? new Set(rows.filter(c=>campaignIdentity(c)).map(c=>accountIdentity(c)+'||'+campaignIdentity(c))).size;
    const people = numOrNull(sm.active_users) ?? new Set(rows.filter(c=>c.actor_type==='human').map(userIdentity)).size;
    lede = t('lede_fb', {c: fmtInt(camps), p: fmtInt(people), a: fmtInt(rows.filter(c=>c.actor_type==='automation').length)});
  }
  document.getElementById('lede').innerHTML = boldNumbers(lede);
  const sp = rowSpan(), per = es && asObj(es.period);
  const from = (per && per.from) || (sp.first && (sp.first.timestamp_iso || sp.first.timestamp_utc));
  const to = (per && per.to) || (sp.last && (sp.last.timestamp_iso || sp.last.timestamp_utc));
  const files = sourceFilesMeta(m).length || (rows.length ? 1 : 0);
  const events = numOrNull(m.event_count) ?? new Set(rows.map(c=>c.event_id)).size;
  const items = [[t('m_period'), from && to ? fmtPeriod(from, to) : '—'], [t('m_source'), `${tn('n_files', files)} · ${tn('n_events', events)}`],
    [t('m_tz'), tzLabel(m.timeline_reference_tz)], [t('m_generated'), m.generated_at ? fmtWall(m.generated_at, true) : '—']];
  document.getElementById('metaList').innerHTML = items.map(([k,v])=>`<div><dt>${escapeHtml(k)}</dt><dd>${escapeHtml(v)}</dd></div>`).join('');
}
function renderStats(){
  const es = execSummary(), tot = (es && asObj(es.totals)) || {}, sm = asObj(DASH_DATA.summary) || {}, rows = rowsAll(), m = dmeta();
  const changes = numOrNull(tot.changes) ?? numOrNull(m.row_count) ?? rows.length;
  const camps = numOrNull(tot.campaigns_changed) ?? numOrNull(sm.changed_campaigns) ?? new Set(rows.filter(c=>campaignIdentity(c)).map(c=>accountIdentity(c)+'||'+campaignIdentity(c))).size;
  const people = numOrNull(tot.people) ?? numOrNull(sm.active_users) ?? new Set(rows.filter(c=>c.actor_type==='human').map(userIdentity)).size;
  const auto = numOrNull(tot.automation) ?? rows.filter(c=>c.actor_type==='automation').length;
  const attn = numOrNull(tot.attention_flags) ?? (Array.isArray(DASH_DATA.flags) ? flagList().filter(f=>f.tier!=='info').length : null);
  const cards = [[changes, t('st_changes')], [camps, t('st_campaigns')], [people, t('st_people', {a: fmtInt(auto)})]];
  if(attn!=null) cards.push([attn, t('st_attention'), 'focus']);
  else cards.push([numOrNull(tot.ad_groups_changed) ?? numOrNull(sm.changed_ad_groups) ?? new Set(rows.filter(c=>adGroupIdentity(c)).map(c=>campaignIdentity(c)+'||'+adGroupIdentity(c))).size, t('st_adgroups')]);
  document.getElementById('statCards').innerHTML = cards.map(([v,l,cls])=>`<div class="stat${cls ? ' '+cls : ''}"><div class="v">${escapeHtml(fmtInt(v))}</div><div class="l">${escapeHtml(l)}</div></div>`).join('');
}
function flagWhere(f){ return f.campaign || f.ad_group || f.user || f.account || ''; }
function flagWhereLong(f){ return [f.account && accountNames().length>1 ? f.account : null, f.campaign, f.ad_group, f.user].filter(Boolean).join(' · '); }
function countHtml(g){ return `<span class="count ${g.tier==='info' ? 'info' : 'attn'}">${escapeHtml(fmtInt(g.hits))}<span class="sr"> ${escapeHtml(t('flags_sr'))}</span></span>`; }
function renderFlagCards(){
  const el = document.getElementById('flagCards');
  document.getElementById('threshLink').hidden = !hasFlagData();
  if(!hasFlagData()){ el.innerHTML = `<div class="flag-empty">${escapeHtml(t('flags_nodata'))} <a href="${escapeHtml(buildHash('p3'))}">${escapeHtml(t('tab3'))}</a></div>`; return; }
  const groups = ruleGroups();
  if(!groups.length){ el.innerHTML = `<div class="flag-empty">${escapeHtml(t('flags_zero'))} <a href="${escapeHtml(buildHash('p2', 's2'))}">${escapeHtml(t('thr_h'))}</a></div>`; return; }
  // 2026-11-19: page 1 is a short executive summary. Attention rules: title,
  // one example and "+N more"; info rules: one compact line. Full lists on page 2.
  const attn = groups.filter(g=>g.tier!=='info'), info = groups.filter(g=>g.tier==='info');
  let html = attn.map((g,i)=>{
    const f = g.flags[0], more = Math.max(0, (numOrNull(g.hits) ?? g.flags.length) - 1);
    const ex = f ? `<li><a href="${escapeHtml(flagHref(f))}"><time>${escapeHtml(fmtShort(f.ts_first))}</time><span>${escapeHtml([flagWhere(f), flagText(f)].filter(Boolean).join(' · '))}</span></a></li>` : '';
    const moreLink = more ? `<li class="more"><a href="${escapeHtml(hrefAll({rule: g.rule}))}">${escapeHtml(tn('more_n', more))}</a></li>` : '';
    return `<article class="flag"><span class="no" aria-hidden="true">${String(i+1).padStart(2, '0')}</span><div><h3><a href="${escapeHtml(hrefAll({rule: g.rule}))}" title="${escapeHtml(g.description || '')}">${escapeHtml(g.title)}</a></h3>${ex || moreLink ? `<ul class="ex">${ex}${moreLink}</ul>` : ''}</div>${countHtml(g)}</article>`;
  }).join('');
  if(info.length) html += `<div class="flag-info"><span class="fi-h">${escapeHtml(t('info_h'))}</span>${info.map(g=>`<a href="${escapeHtml(hrefAll({rule: g.rule}))}">${escapeHtml(g.title)} <span class="count info">${escapeHtml(fmtInt(g.hits))}</span></a>`).join('')}</div>`;
  el.innerHTML = html;
}
function renderWho(){
  const es = execSummary();
  const people = (es && Array.isArray(es.people) ? es.people.filter(p=>p && typeof p==='object') : computePeople(rowsAll())).map(p=>({label: String(p.label ?? ''), actor_type: p.actor_type || 'unknown', n: numOrNull(p.changes) || 0}));
  const humans = people.filter(p=>p.actor_type==='human').sort((a,b)=>b.n-a.n), other = people.filter(p=>p.actor_type!=='human');
  const sum = l=> l.reduce((a,p)=>a+p.n, 0);
  const segs = humans.slice(0, 3).map((p,i)=>({label: p.label, n: p.n, color: `var(--s${i+1})`, user: p.label}));
  if(humans.length > 3) segs.push({label: tn('other_people', humans.length-3), n: sum(humans.slice(3)), color: 'var(--s4)'});
  segs.push({label: t('auto_google'), n: sum(other), color: 'var(--s5)'});
  const total = Math.max(1, sum(segs));
  const pct = n=> Math.round(n/total*100);
  const aria = segs.map(s=>t('who_seg', {label: s.label, p: pct(s.n)})).join(', ');
  document.getElementById('whoBody').innerHTML = `<div class="wbar" role="img" aria-label="${escapeHtml(aria)}">${segs.filter(s=>s.n>0).map(s=>`<i style="width:${(s.n/total*100).toFixed(2)}%;background:${s.color}"></i>`).join('')}</div>`
    + `<ul class="legend">${segs.map(s=>`<li><span class="sw" style="background:${s.color}" aria-hidden="true"></span>${s.user ? `<a href="${escapeHtml(hrefAll({user: s.user}))}">${escapeHtml(s.label)}</a>` : `<span class="lt">${escapeHtml(s.label)}</span>`}<b>${escapeHtml(fmtInt(s.n))}</b></li>`).join('')}</ul>`;
}
function renderBudget(){
  const es = execSummary(); const b = (es && asObj(es.budget)) || computeBudget(rowsAll());
  const inc = Math.abs(numOrNull(b.increase_total) || 0), dec = Math.abs(numOrNull(b.decrease_total) || 0);
  const ni = numOrNull(b.increase_count) || 0, nd = numOrNull(b.decrease_count) || 0;
  const el = document.getElementById('budgetBody');
  if(!ni && !nd){ el.innerHTML = `<p class="sub" style="margin:0">${escapeHtml(t('budget_none'))}</p>`; return; }
  el.innerHTML = `<div class="budget"><div class="b"><div class="v"><span class="sgn">+</span>${escapeHtml(fmtMoney(inc, b.currency))}</div><div class="l">${escapeHtml(tn('n_increases', ni))}</div></div>`
    + `<div class="b alt"><div class="v"><span class="sgn">−</span>${escapeHtml(fmtMoney(dec, b.currency))}</div><div class="l">${escapeHtml(tn('n_decreases', nd))}</div></div></div>`
    + `<a class="more" href="${escapeHtml(hrefAll({category: 'Budget'}))}">${escapeHtml(t('budget_more'))}</a>`;
}
function renderStale(){
  const list = staleList(), d = staleDays(), multi = accountNames().length > 1;
  document.getElementById('staleSub').textContent = t(list.some(s=>s.basis==='metrics') ? 'stale_sub_m' : 'stale_sub');
  const el = document.getElementById('staleBody');
  if(!list.length){ el.innerHTML = `<p class="sub" style="margin:0">${escapeHtml(t('stale_none', {d: fmtInt(d)}))}</p>`; return; }
  el.innerHTML = `<ul class="stale">${list.slice(0, 5).map(s=>`<li><a href="${escapeHtml(hrefAll({account: s.account, campaign: s.campaign}))}">${escapeHtml(s.campaign || '—')}${multi && s.account ? `<small>${escapeHtml(s.account)}</small>` : ''}</a><span class="dpill">${escapeHtml(tn('n_days', numOrNull(s.days) ?? 0))}</span></li>`).join('')}</ul>`
    + (list.length > 5 ? `<a class="more" href="${escapeHtml(buildHash('p2', 's1'))}">${escapeHtml(tn('n_more', list.length-5))}</a>` : '');
}
let CAVEATS = [];
function renderP1Notes(){
  const notes = coverageNotes().map(coverageText);
  const parts = [t('p1_scope'), ...notes].map(x=>`<span>${escapeHtml(x)}</span>`);
  if(CAVEATS.length) parts.push(`<span><a href="${escapeHtml(buildHash('p2', 's7'))}">${escapeHtml(t('p1_notes_link', {n: CAVEATS.length}))}</a></span>`);
  document.getElementById('p1Notes').innerHTML = parts.join('');
}
// ---- page 2: campaign table, flags by rule, thresholds ----------------
const campSort = {key: 'changes', dir: -1};
let campShowAll = false;
function campFlagCount(r){ const f = asObj(r.flags_by_rule) || {}; return Object.values(f).reduce((a,n)=>a+(numOrNull(n)||0), 0); }
function renderCampaignTable(){
  const rows = campaignTableRows(), multi = new Set(rows.map(r=>r.account||'')).size > 1;
  const key = campSort.key, dir = campSort.dir;
  const val = r=> key==='flags' ? campFlagCount(r) : key==='last_change' ? String(r.last_change||'') : r[key];
  rows.sort((a,b)=>{ const x = val(a), y = val(b); const c = typeof x==='number' && typeof y==='number' ? x-y : String(x??'').localeCompare(String(y??''), UI_LOCALE); return c*dir || String(a.campaign).localeCompare(String(b.campaign), UI_LOCALE); });
  const order = rulesConfig().map(c=>c.rule);
  const cols = [['campaign', t('ct_campaign')], ...(multi ? [['account', t('ct_account')]] : []), ['changes', t('ct_changes'), 'num'], ['budget_changes', t('ct_budget'), 'num'], ['last_change', t('ct_last'), 'num'], ['flags', t('ct_flags')]];
  const LIMIT = 25, shown = campShowAll ? rows : rows.slice(0, LIMIT);
  const tags = r=>{ const f = asObj(r.flags_by_rule) || {};
    return Object.keys(f).filter(k=>numOrNull(f[k])>0).sort((a,b)=> (order.indexOf(a)+1||99) - (order.indexOf(b)+1||99))
      .map(k=>`<span class="tag${ruleTierOf(k)==='info' ? ' info' : ''}">${escapeHtml(k==='stale_entity' && !staleCampaignItself(r) ? t('rs.stale_ad_group') : ruleShort(k))}${f[k]>1 ? ' ×'+escapeHtml(fmtInt(f[k])) : ''}</span>`).join(''); };
  const ago = r=>{ const d = numOrNull(r.days_since); return d==null ? '—' : d===0 ? t('days_ago_0') : tn('days_ago', d); };
  const el = document.getElementById('campTableWrap');
  el.innerHTML = rows.length ? `<table><thead><tr>${cols.map(([k,l,cl])=>`<th${cl ? ` class="${cl}"` : ''}${k===key ? ` aria-sort="${dir<0 ? 'descending' : 'ascending'}"` : ''}><button type="button" class="th-btn" data-ck="${k}">${escapeHtml(l)}</button></th>`).join('')}</tr></thead><tbody>${
    shown.map(r=>{ const href = hrefAll({account: multi ? r.account : null, campaign: r.campaign});
      return `<tr class="crow" data-href="${escapeHtml(href)}"><td class="wrapc"><a href="${escapeHtml(href)}">${escapeHtml(r.campaign || '—')}</a></td>${multi ? `<td>${escapeHtml(r.account || '')}</td>` : ''}<td class="num">${escapeHtml(fmtInt(r.changes))}</td><td class="num">${escapeHtml(fmtInt(r.budget_changes ?? 0))}</td><td class="num" title="${escapeHtml(r.last_change ? fmtWall(r.last_change, true) : '')}">${escapeHtml(ago(r))}</td><td class="wrapc">${tags(r) || '<span class="muted">—</span>'}</td></tr>`; }).join('')
  }</tbody></table>${rows.length > LIMIT ? `<button type="button" class="plainbtn morebtn" id="campMore">${escapeHtml(campShowAll ? t('show_less') : t('show_all', {n: fmtInt(rows.length)}))}</button>` : ''}` : `<div class="empty">${escapeHtml(t('no_data'))}</div>`;
  el.querySelectorAll('.th-btn').forEach(b=> b.addEventListener('click', ()=>{ const k = b.dataset.ck; if(campSort.key===k) campSort.dir *= -1; else { campSort.key = k; campSort.dir = k==='campaign' || k==='account' ? 1 : -1; } renderCampaignTable(); const nb = el.querySelector(`.th-btn[data-ck="${k}"]`); if(nb) nb.focus(); }));
  el.querySelectorAll('tr.crow').forEach(tr=> tr.addEventListener('click', e=>{ if(e.target.closest('a')) return; location.hash = tr.dataset.href; }));
  const mb = document.getElementById('campMore'); if(mb) mb.addEventListener('click', ()=>{ campShowAll = !campShowAll; renderCampaignTable(); const nb = document.getElementById('campMore'); if(nb) nb.focus(); });
}
function renderFlagGroups(){
  const el = document.getElementById('flagGroups');
  if(!hasFlagData()){ el.innerHTML = `<div class="flag-empty">${escapeHtml(t('flags_nodata2'))}</div>`; return; }
  const groups = ruleGroups();
  if(!groups.length){ el.innerHTML = `<div class="flag-empty">${escapeHtml(t('flags_zero'))}</div>`; return; }
  const SHOWN = 8, LIMIT = 200;
  el.innerHTML = groups.map(g=>{
    const p = g.cfg ? fmtParams(g.cfg.params) : '';
    const items = g.flags.slice(0, LIMIT).map((f,i)=>`<li${i>=SHOWN ? ' hidden' : ''}><a href="${escapeHtml(flagHref(f))}"><time>${escapeHtml(fmtShort(f.ts_first))}</time><span class="fw">${escapeHtml(flagText(f) || ruleShort(f.rule))}<small>${escapeHtml(flagWhereLong(f))}</small></span></a></li>`).join('');
    const hiddenN = Math.min(g.flags.length, LIMIT) - SHOWN;
    const more = (hiddenN > 0 ? `<button type="button" class="linkish fmore" aria-expanded="false">${escapeHtml(tn('show_more', hiddenN))}</button>` : '')
      + (g.flags.length > LIMIT ? ` <a class="note" href="${escapeHtml(hrefAll({rule: g.rule}))}">${escapeHtml(tn('n_more_all', g.flags.length - LIMIT))}</a>` : '');
    return `<article class="fgroup"><div class="fgroup-h"><div><h3>${escapeHtml(g.title)}</h3>${g.description ? `<p>${escapeHtml(g.description)}</p>` : ''}${p ? `<p class="rule">${escapeHtml(t('rule_fired', {p}))}</p>` : ''}</div>${countHtml(g)}</div>${items ? `<ul class="flist">${items}</ul>` : ''}${more ? `<div class="fgroup-more">${more}</div>` : ''}</article>`;
  }).join('');
  el.querySelectorAll('.fmore').forEach(b=> b.addEventListener('click', ()=>{
    const open = b.getAttribute('aria-expanded')!=='true', items = [...b.closest('.fgroup').querySelectorAll('.flist li')];
    items.forEach((li, i)=>{ if(i>=SHOWN) li.hidden = !open; });
    b.setAttribute('aria-expanded', open ? 'true' : 'false');
    b.textContent = open ? t('show_less') : tn('show_more', items.length - SHOWN);
  }));
}
function renderThresholds(){
  const el = document.getElementById('threshTable'), cfg = rulesConfig();
  if(!cfg.length){ el.innerHTML = `<div class="empty">${escapeHtml(t('thr_none'))}</div>`; return; }
  el.innerHTML = `<table><thead><tr><th>${escapeHtml(t('th_rule'))}</th><th>${escapeHtml(t('th_tier'))}</th><th>${escapeHtml(t('th_status'))}</th><th>${escapeHtml(t('th_threshold'))}</th><th class="num">${escapeHtml(t('th_hits'))}</th></tr></thead><tbody>${
    cfg.map(c=>{ const on = c.enabled!==false, why = on ? '' : loc(c.disabled_reason);
      return `<tr><td class="wrapc">${escapeHtml(loc(c.title) || ruleShort(c.rule))}</td><td>${escapeHtml(t(c.tier==='info' ? 'tier_info' : 'tier_attention'))}</td><td class="wrapc">${escapeHtml(t(on ? 'st_on' : 'st_off'))}${why ? `<span class="rm-detail">${escapeHtml(why)}</span>` : ''}</td><td class="wrapc">${escapeHtml(fmtParams(c.params) || '—')}</td><td class="num">${on && numOrNull(c.hits)!=null ? escapeHtml(fmtInt(c.hits)) : '—'}</td></tr>`; }).join('')
  }</tbody></table>`;
}
// Visible sections are numbered 01.. in the side list (section 6 only exists with metrics).
function numberSections(){
  let n = 0;
  document.querySelectorAll('#sideList a').forEach(a=>{ const k = a.querySelector('.k'); if(a.parentElement.hidden) return; n++; if(k) k.textContent = String(n).padStart(2, '0'); });
}
// Side list follows the scroll position.
function initSideSpy(){
  if(typeof IntersectionObserver==='undefined') return;
  const links = [...document.querySelectorAll('#sideList a')];
  const mark = id=> links.forEach(a=>{ const on = a.dataset.sec===id; a.classList.toggle('on', on); if(on) a.setAttribute('aria-current', 'true'); else a.removeAttribute('aria-current'); });
  const seen = new Map();
  const io = new IntersectionObserver(entries=>{
    entries.forEach(e=> seen.set(e.target.id, e.isIntersecting));
    const first = SECTIONS.map(s=>s[0]).find(id=> seen.get(id));
    if(first) mark(first);
  }, {rootMargin: '-90px 0px -55% 0px'});
  SECTIONS.forEach(s=>{ const el = document.getElementById(s[0]); if(el) io.observe(el); });
  mark('s1');
}
// ---- routing -----------------------------------------------------------
let currentPage = null, lastHash = null;
function replaceHash(h){ try { history.replaceState(null, '', h); } catch(e){ location.hash = h; } lastHash = location.hash; }
function showPage(pid){
  ['p1','p2','p3'].forEach(id=>{ document.getElementById(id).hidden = id!==pid; });
  document.querySelectorAll('.tab').forEach(a=>{ if(a.dataset.page===pid) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current'); });
  const name = t({p1:'tab1', p2:'tab2', p3:'tab3'}[pid]), accs = accountNames();
  document.title = `${name} · ${t('brand')}${accs.length===1 ? ' · '+accs[0] : ''}`;
  if(pid==='p2' && lastMetricLine) drawMetricLine(lastMetricLine);
  currentPage = pid;
}
function route(){
  const r = parseHash(location.hash);
  const fresh = location.hash !== lastHash;
  if(fresh && Object.keys(r.params).length){ applyDeepLink(r.params); render(); }
  lastHash = location.hash;
  const changed = currentPage !== r.page;
  const ae = document.activeElement;
  showPage(r.page);
  if(r.section){ const s = document.getElementById(r.section); if(s && !s.hidden) s.scrollIntoView({block: 'start'}); }
  else if(changed) window.scrollTo(0, 0);
  // focus left inside a page that just hid: move it to the new page's heading
  if(changed && ae && ae!==document.body && !ae.getClientRects().length){
    const h = document.querySelector(`#${r.page} h1`); if(h){ h.setAttribute('tabindex', '-1'); h.focus({preventScroll: true}); }
  }
}
function initRouting(){
  document.getElementById('tab1').setAttribute('href', buildHash('p1'));
  document.getElementById('tab2').setAttribute('href', buildHash('p2'));
  document.getElementById('tab3').setAttribute('href', buildHash('p3'));
  document.querySelectorAll('#sideList a').forEach(a=>{
    a.setAttribute('href', buildHash('p2', a.dataset.sec));
    // the same section twice: no hashchange, scroll anyway
    a.addEventListener('click', ()=>{ if(a.getAttribute('href')===location.hash){ const s = document.getElementById(a.dataset.sec); if(s) s.scrollIntoView({block: 'start'}); } });
  });
  document.getElementById('threshLink').setAttribute('href', buildHash('p2', 's2'));
  window.addEventListener('hashchange', route);
  route();
}
// ---- i18n of the static markup: [data-t] text and [data-ta] attributes ----
function applyI18n(){
  document.querySelectorAll('[data-t]').forEach(el=>{ el.textContent = t(el.dataset.t); });
  document.querySelectorAll('[data-ta]').forEach(el=>{ el.dataset.ta.split(';').forEach(p=>{ const i = p.indexOf(':'); if(i>0) el.setAttribute(p.slice(0, i), t(p.slice(i+1))); }); });
  const uh = document.querySelector('#untouchedBlock h2'); if(uh) uh.textContent = t('untouched_h');
}
function renderMetaLines(m){
  const sp = rowSpan();
  const winStart = sp.first ? fmtWall(sp.first.timestamp_iso || sp.first.timestamp_utc, false) : '—';
  const winEnd = sp.last ? fmtWall(sp.last.timestamp_iso || sp.last.timestamp_utc, false) : '—';
  const nf = sourceFilesMeta(m).length;
  document.getElementById('metaLine').textContent = t('meta_line', {n: fmtInt(m.row_count||0), a: winStart, b: winEnd, src: sourceLabels(m).join(', '),
    files: nf > 1 ? t('meta_files', {n: nf}) : '', o: m.other_pct ?? 0, masked: m.masked_users ? t('meta_masked') : '', g: fmtWall(m.generated_at, true)});
  // source_event_id is only ever populated by the API's own event-identity field.
  const caveats = [];
  if(DASH_DATA.changes.some(c=>c.source_event_id)) caveats.push(t('cav_api'));
  if(m.rows_skipped_unparseable_date_pct > 5) caveats.push(t('cav_dates', {n: fmtInt(m.rows_skipped_unparseable_date), p: m.rows_skipped_unparseable_date_pct}));
  // What the run read vs. what is shown here: counts only, no verdict. Every key optional.
  const dq = m.data_quality;
  if(dq){
    const n = x => fmtInt(x||0);
    const lim = dq.in_report_with_limits || {}, lo = dq.left_out || {};
    const flags = ['category_other','timezone_unknown','operation_inferred','value_parsed_from_summary','multi_field_unsplit']
      .filter(k=>lim[k]>0).map(k=>t('dq.'+k, {n: n(lim[k])}));
    const split = [];
    if(dq.rows_added_by_statement_split>0) split.push(t('dq_split_stmt', {n: n(dq.rows_added_by_statement_split)}));
    if(dq.rows_added_by_field_split>0) split.push(t('dq_split_field', {n: n(dq.rows_added_by_field_split)}));
    const left = [t('dq_unreadable', {n: n(lo.unreadable_date)}), t('dq_duplicate', {n: n(lo.duplicate)})];
    if(lo.duplicate_across_files!==undefined) left.push(t('dq_dup_files', {n: n(lo.duplicate_across_files)}));
    const bq = dq.before_after;
    if(bq) flags.push(t('dq_ba', {a: n(bq.rows_with_window), b: n(bq.rows_without_metrics)}) + (bq.entities_unmatched ? t('dq_ba_unmatched', {c: n(bq.entities_unmatched)}) : ''));
    const dup = dq.possible_duplicate_rows_kept ?? dq.possible_duplicates;
    if(dup>0) flags.push(t('dq_dup_kept', {n: n(dup)}));
    if(dq.campaign_inferred_from_summary>0) flags.push(t('dq_camp_inferred', {n: n(dq.campaign_inferred_from_summary)}));
    const dst = (dq.dst_nonexistent_local_time||0) + (dq.dst_ambiguous_local_time||0);
    if(dst>0) flags.push(t('dq_dst', {n: n(dst)}));
    document.getElementById('dataQuality').textContent = t('dq_line', {in: n(dq.input_rows), rep: n(dq.rows_in_report), split: split.length ? ` (${split.join(', ')})` : '', left: left.join(', '),
      flags: flags.length ? t('dq_flags', {x: flags.join(', ')}) : '', ign: (dq.ignored_headers||[]).length ? t('dq_ignored', {x: dq.ignored_headers.join(', ')}) : ''});
    if(Array.isArray(dq.warnings)) dq.warnings.forEach(w=>{ if(w) caveats.push(typeof w==='string' ? w : (w.message || JSON.stringify(w))); });
    if(dq.rows_added_by_statement_split>0){
      document.getElementById('explorerCaption').textContent = t('exp_cap_split') + ' ' + t('exp_cap');
    }
  }
  const tzUnknown = m.rows_excluded_from_timeline_unknown_tz || 0;
  if(m.row_count && tzUnknown / m.row_count > 0.05) caveats.push(t('cav_tz', {n: fmtInt(tzUnknown), p: (100*tzUnknown/m.row_count).toFixed(1)}));
  CAVEATS = caveats;
  document.getElementById('apiCaveat').innerHTML = caveats.map(c=>`<div class="warn-box">${escapeHtml(c)}</div>`).join('');
}
function pickLang(){
  const u = DASH_DATA && DASH_DATA.ui_lang;
  if(u==='tr' || u==='en') return u;
  if(sourceLabels(dmeta()).includes('legacy_summary_tr')) return 'tr';
  const rows = rowsAll().slice(0, 500);
  const tr = rows.filter(c=>/[ğışĞŞİ]|değiştirildi|eklendi|kaldırıldı/.test((c.raw_summary||'')+(c.operation||''))).length;
  return rows.length && tr/rows.length > 0.2 ? 'tr' : 'en';
}
document.addEventListener('DOMContentLoaded', ()=>{
  const m = DASH_DATA.meta || {};
  DASH_DATA.changes.forEach((c,i)=>{ c._i = i; });
  UI_LANG = pickLang();
  UI_LOCALE = UI_LANG==='tr' ? 'tr-TR' : 'en-GB';
  document.documentElement.lang = UI_LANG;
  applyI18n();
  renderMetaLines(m);
  renderCoverage();
  initFilters();
  initRuleMatchesConfig();
  renderHero(); renderStats(); renderFlagCards(); renderWho(); renderBudget(); renderStale(); renderP1Notes();
  renderCampaignTable(); renderFlagGroups(); renderThresholds();
  document.getElementById('detailBA').addEventListener('click', e=>{ const b = e.target.closest('[data-open]'); if(b) openDetail(b.dataset.open); });
  window.addEventListener('resize', ()=>{ if(lastMetricLine) drawMetricLine(lastMetricLine); });
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
  // C-2: the detail panel is a modal dialog: focus moves in, Esc closes, focus returns to the opener.
  document.getElementById('detailClose').addEventListener('click', closeDetail);
  document.getElementById('detailOverlay').addEventListener('click', e=>{ if(e.target.id==='detailOverlay') closeDetail(); });
  document.addEventListener('keydown', e=>{
    if(!document.getElementById('detailOverlay').classList.contains('open')) return;
    if(e.key==='Escape'){ e.preventDefault(); closeDetail(); }
    else if(e.key==='Tab'){
      e.preventDefault();
      const f = [...document.getElementById('detailCard').querySelectorAll('button, [href], input, select, [tabindex="0"]')].filter(x=>!x.disabled && x.getClientRects().length);
      const i = f.indexOf(document.activeElement);
      const next = e.shiftKey ? (i<=0 ? f.length-1 : i-1) : (i<0 || i===f.length-1 ? 0 : i+1);
      if(f[next]) f[next].focus();
    }
  });
  render();
  initRouting();
  initSideSpy();
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


def build_dashboard(rows, out_path, mask_users=False, generated_at=None, json_out=None, rows_skipped_unparseable_date=0, data_quality=None,
                    meta_extra=None, extra=None):
    sources = {r.get("source", "unknown") for r in rows}
    other_pct = round(100 * sum(1 for r in rows if r["category"] == "Other") / len(rows), 1) if rows else 0.0
    generated_at = generated_at or datetime.now().isoformat()
    if len(rows) > SCALE_WARN_THRESHOLD:
        print(f"WARNING: {len(rows)} rows exceeds the {SCALE_WARN_THRESHOLD} single-file guideline. Consider a date-range split.")
    data = build_aggregation(rows, sources, other_pct, mask_users, generated_at, date_parse_stats={"rows_skipped_unparseable_date": rows_skipped_unparseable_date})
    if data_quality is not None:
        data["meta"]["data_quality"] = data_quality
    # 2026-11-14: meta.sources becomes per-file objects; the old label list moves to meta.source_labels.
    if meta_extra:
        data["meta"]["source_labels"] = data["meta"]["sources"]
        data["meta"].update(meta_extra)
    data.update(extra or {})
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


# =====================================================================
# MULTI-INPUT, COVERAGE NOTES, BEFORE/AFTER METRICS (2026-11-14)
# =====================================================================
class InputAccountError(ValueError):
    """2026-11-15 R-7: an --input-account FILE that matches several inputs; carries the candidates."""
    def __init__(self, message, candidates=None):
        super().__init__(message)
        self.candidates = candidates or []


def _resolved(p):
    try:
        return str(Path(p).resolve())
    except OSError:
        return str(p)


def _dedupe_inputs(inputs):
    """2026-11-15 R-6: the same file given twice (by any path spelling) is read once."""
    seen, out, dups = set(), [], []
    for p in inputs:
        k = _resolved(p)
        if k in seen:
            dups.append(str(p))
            continue
        seen.add(k)
        out.append(str(p))
    return out, dups


def parse_input_accounts(entries, inputs):
    """--input-account FILE=NAME -> {input path: name}. FILE is matched against
    the given path first, then the basename; a FILE that names no input is an error
    (2026-11-14), one that names several different files too (2026-11-15 R-7)."""
    out = {}
    uniq, _ = _dedupe_inputs(inputs)
    for e in entries or []:
        hits = []
        for refs in ((lambda p: str(p)), (lambda p: Path(p).name)):
            hits = [p for p in uniq if e.startswith(refs(p) + "=") and e[len(refs(p)) + 1:].strip()]
            if hits:
                break
        if not hits:
            raise ValueError(f"--input-account {e!r}: expected FILE=NAME where FILE is one of the inputs: {[Path(p).name for p in uniq]}")
        if len(hits) > 1:
            raise InputAccountError(f"--input-account {e!r}: {Path(hits[0]).name!r} matches {len(hits)} inputs; give the path as passed to tell them apart: {hits}", hits)
        p = hits[0]
        ref = str(p) if e.startswith(str(p) + "=") else Path(p).name
        out[str(p)] = e[len(ref) + 1:].strip()
    return out


def _source_file_names(inputs):
    """Basename per input; when two inputs share a basename, the shortest unique
    "<parent>/<basename>" tail (2026-11-15 R-7: never a full local path)."""
    names = Counter(Path(p).name for p in inputs)
    out = {}
    for p in inputs:
        base = Path(p).name
        if names[base] == 1:
            out[str(p)] = base
            continue
        mine = Path(_resolved(p)).parts
        others = [Path(_resolved(q)).parts for q in inputs if str(q) != str(p) and Path(q).name == base]
        label = None
        for k in range(2, len(mine)):
            tail = mine[-k:]
            if all(o[-k:] != tail for o in others):
                label = "/".join(tail)
                break
        out[str(p)] = label or f"{base}#{list(inputs).index(p) + 1}"
    return out


def _is_api_source(source_label, path):
    return str(source_label or "").startswith("google_ads_api") or Path(path).suffix.lower() == ".json"


def dedupe_across_files(rows):
    """A change_id seen in an earlier file is an overlap duplicate: kept once,
    counted. Within one file nothing changes (rows arrive per-file deduped)."""
    first_file, out, dropped = {}, [], 0
    for r in rows:
        f = first_file.setdefault(r["change_id"], r.get("source_file"))
        if f != r.get("source_file"):
            dropped += 1
            continue
        out.append(r)
    return out, dropped


def build_sources_meta(rows, file_infos):
    """meta.sources: one entry per input file, in input order."""
    per = defaultdict(list)
    for r in rows:
        per[r.get("source_file")].append(r)
    out = []
    for fi in file_infos:
        rs = per.get(fi["file"], [])
        days = sorted(str(r.get("timestamp_iso") or "")[:10] for r in rs if r.get("timestamp_iso"))
        out.append({"file": fi["file"], "rows": len(rs),
                    "accounts": sorted({str(r.get("account_name") or r.get("account_id")) for r in rs if r.get("account_name") or r.get("account_id")}),
                    "source_label": fi["source_label"], "date_min": days[0] if days else None, "date_max": days[-1] if days else None})
    return out


COVERAGE_NOTE_TEXT = {
    "editor_changes_absent": "The Google Ads API change history does not return changes made in Google Ads Editor, so they are not in the API data.",  # 2026-11-15 R-5: as Google documents it
    "api_30_day_window": "The Google Ads API returns change history for the last 30 days only; older changes are not in the API data.",
    "auto_applied_may_be_missing": "Recommendations auto-applied by Google and some automated changes may be missing or appear under a system user.",
    "ui_export_may_differ_from_api": "The Google Ads UI export and the API data can list the same change differently, so their row counts may differ.",
    "metrics_conversion_lag": "Conversions are counted on the click date and keep arriving for days, so the most recent days of the after window may still grow.",
}


def build_coverage_notes(file_infos, metrics_given):
    api = any(fi["is_api"] for fi in file_infos)
    ui = any(not fi["is_api"] for fi in file_infos)
    ids = (["editor_changes_absent", "api_30_day_window"] if api else []) + ["auto_applied_may_be_missing"]
    ids += (["ui_export_may_differ_from_api"] if api and ui else []) + (["metrics_conversion_lag"] if metrics_given else [])
    return [{"id": i, "text": COVERAGE_NOTE_TEXT[i]} for i in ids]


METRIC_FIELDS = ("impressions", "clicks", "cost", "conversions", "conversion_value")
METRICS_HEADER_ALIASES = {
    "day": ["day", "gün", "date", "tarih", "segments.date"],
    "campaign": ["campaign", "kampanya", "campaign.name", "campaign name", "kampanya adı"],
    "ad_group": ["ad group", "reklam grubu", "ad_group.name", "ad group name", "reklam grubu adı"],
    "account": ["account", "hesap", "account name", "hesap adı", "customer.descriptive_name"],
    "impressions": ["impr.", "impressions", "gösterim", "gösterimler", "gösterim sayısı", "metrics.impressions"],
    "clicks": ["clicks", "tıklama", "tıklamalar", "tıklama sayısı", "metrics.clicks"],
    "cost": ["cost", "maliyet", "metrics.cost", "metrics.cost_micros"],
    "conversions": ["conversions", "dönüşüm", "dönüşümler", "conv.", "metrics.conversions"],
    "conversion_value": ["conv. value", "conversion value", "dönüşüm değeri", "dönş. değeri", "metrics.conversions_value"],
}
_METRICS_TOTAL_RE = re.compile(r"^\s*(total|toplam|genel toplam)\s*(:|$)", re.IGNORECASE)


def _entity_key(text):
    """Case/whitespace-insensitive, TR-folded name key (2026-11-14)."""
    return " ".join(_fold(text).split())


def _acct_keys(text):
    k = _entity_key(text)
    digits = re.sub(r"\D", "", k)
    return {k} | ({digits} if len(digits) >= 6 and digits == re.sub(r"[\s-]", "", k) else set())


def load_metrics_file(path, account=None, date_format=None, decimal_style=None):
    """One daily performance CSV -> {"status": "ok", "rows": [...]} or a status
    dict naming the file. Reuses read_rows / detect_date_format / normalize_number."""
    name = Path(path).name
    try:
        headers, raw_rows = read_rows(path, encoding=INPUT_ENCODING)
    except ValueError as e:
        return {"status": "error", "file": name, "message": f"--metrics: {e}"}
    col, ignored = {}, []
    for h in headers:
        key = normalize_header(str(h or "")).replace("﻿", "")
        field = next((f for f, al in METRICS_HEADER_ALIASES.items() if key in al), None)
        if field and field not in col:
            col[field] = h
        elif h:
            ignored.append(h)
    if "day" not in col or "campaign" not in col:
        return {"status": "needs_metrics_mapping", "file": name, "headers": headers,
                "message": f"--metrics {name}: needs a day column (Day/Gün/Date/Tarih) and a campaign column (Campaign/Kampanya). Headers found: {headers}"}
    rows = [r for r in raw_rows if not any(_METRICS_TOTAL_RE.match(str(r.get(col[f]) or "")) for f in ("day", "campaign", "ad_group", "account") if f in col)]
    rows = [r for r in rows if str(r.get(col["day"]) or "").strip() and str(r.get(col["campaign"]) or "").strip()]
    fmt = date_format or detect_date_format([r.get(col["day"]) for r in rows])
    if fmt is None:
        q = date_format_question([r.get(col["day"]) for r in rows])
        q["file"] = name
        return q
    tr_headers = any(normalize_header(str(col[f])) in ("gün", "tarih", "kampanya", "gösterim", "maliyet", "tıklama", "tıklamalar", "gösterimler") for f in col)
    style = decimal_style or ("TR" if tr_headers else "US")
    micros = normalize_header(str(col.get("cost", ""))) == "metrics.cost_micros"
    out, bad_dates = [], 0
    for r in rows:
        try:
            day = parse_timestamp(r.get(col["day"]), fmt).date()
        except (ValueError, TypeError, OverflowError):
            bad_dates += 1
            continue
        vals = {}
        for f in METRIC_FIELDS:
            if f not in col:
                continue
            raw = str(r.get(col[f]) or "").strip()
            try:
                v = normalize_number(raw, style) if raw not in ("", "--", "-", "—") else None
            except ValueError:
                v = None
            if v is not None and f == "cost" and micros:
                v = v / 1_000_000
            vals[f] = v
        out.append({"day": day, "campaign": str(r.get(col["campaign"])).strip(),
                    "ad_group": (str(r.get(col["ad_group"]) or "").strip() or None) if "ad_group" in col else None,
                    "account": (str(r.get(col["account"]) or "").strip() or None) if "account" in col else account, "values": vals})
    return {"status": "ok", "file": name, "rows": out, "metrics": [f for f in METRIC_FIELDS if f in col], "ignored_columns": ignored,
            "has_ad_group": "ad_group" in col, "date_format_used": fmt, "decimal_style_used": style, "rows_skipped_unparseable_date": bad_dates}


def compute_before_after(rows, metrics_files, window_days=7, conversion_lag_days=7):
    """before = N full days before the change's local date; after = that date
    + N-1 days. Sums per metric, null when no value exists (never 0). Entity
    match: account (when both sides name one) + campaign, + ad group when
    both sides have it; names case/space-insensitive and TR-folded (2026-11-14)."""
    N = window_days
    series = {"campaign": defaultdict(dict), "ad_group": defaultdict(dict)}  # key -> day -> values
    owner = {}  # (level, key, day) -> file: an entity-day from two files is counted once
    accounts_by_campaign = defaultdict(set)
    days_all = []
    for mf in metrics_files:
        level = "ad_group" if mf["has_ad_group"] else "campaign"
        for m in mf["rows"]:
            if level == "ad_group" and not m["ad_group"]:
                continue
            ck = _entity_key(m["campaign"])
            key = (_entity_key(m["account"]) if m["account"] else None, ck) + ((_entity_key(m["ad_group"]),) if level == "ad_group" else ())
            if owner.setdefault((level, key, m["day"]), mf["file"]) != mf["file"]:
                continue
            accounts_by_campaign[ck].add(key[0])
            slot = series[level][key].setdefault(m["day"], {})
            for f, v in m["values"].items():
                if v is not None:
                    slot[f] = slot.get(f, 0.0) + v
                else:
                    slot.setdefault(f, None)
            days_all.append(m["day"])
    date_max = max(days_all) if days_all else None
    maturing_from = date_max - timedelta(days=conversion_lag_days - 1) if date_max and conversion_lag_days > 0 else None

    def find(level, r):
        ck = _entity_key(r.get("campaign_name"))
        tail = ((_entity_key(r.get("ad_group_name")),) if level == "ad_group" else ())
        row_accts = set()
        for a in (r.get("account_name"), r.get("account_id")):
            if a:
                row_accts |= _acct_keys(a)
        for acct in accounts_by_campaign.get(ck, ()):
            if acct is None or acct in row_accts or not row_accts:
                k = (acct, ck) + tail
                if k in series[level]:
                    return k
        return None

    def window(sk, start, end):
        days_data, sums = 0, {f: None for f in METRIC_FIELDS}
        d = start
        while d <= end:
            slot = sk.get(d)
            if slot is not None:
                days_data += 1
                for f, v in slot.items():
                    if v is not None:
                        sums[f] = (sums[f] or 0.0) + v
            d += timedelta(days=1)
        return {"days_with_data": days_data, **{f: (round(v, 6) if v is not None else None) for f, v in sums.items()}}

    # rows per campaign, by local date, for the overlap list
    by_campaign = defaultdict(list)
    for r in rows:
        if r.get("campaign_name") and r.get("timestamp_iso"):
            by_campaign[(account_identity(r), _entity_key(r.get("campaign_name")))].append((str(r["timestamp_iso"])[:10], r))
    for v in by_campaign.values():
        v.sort(key=lambda t: t[0])
    keys_sorted = {k: [t[0] for t in v] for k, v in by_campaign.items()}

    import bisect
    out, unmatched, cache = {}, set(), {}
    for r in rows:
        if not r.get("campaign_name") or not r.get("timestamp_iso"):
            continue
        level, key = None, None
        if r.get("ad_group_name"):
            key = find("ad_group", r)
            level = "ad_group" if key else None
        if not key:
            key = find("campaign", r)
            level = "campaign" if key else None
        if not key:
            unmatched.add((account_identity(r), _entity_key(r.get("campaign_name"))))
            continue
        d = date.fromisoformat(str(r["timestamp_iso"])[:10])
        ck = (level, key, d)
        if ck not in cache:
            sk = series[level][key]
            before = window(sk, d - timedelta(days=N), d - timedelta(days=1))
            after = window(sk, d, d + timedelta(days=N - 1))
            after["maturing_days"] = sum(1 for i in range(N) if maturing_from and maturing_from <= d + timedelta(days=i) <= date_max and (d + timedelta(days=i)) in sk)
            cache[ck] = (before, after)
        before, after = cache[ck]
        camp = (account_identity(r), _entity_key(r.get("campaign_name")))
        lst, ks = by_campaign[camp], keys_sorted[camp]
        lo = bisect.bisect_left(ks, (d - timedelta(days=N)).isoformat())
        hi = bisect.bisect_right(ks, (d + timedelta(days=N - 1)).isoformat())
        ag = _entity_key(r.get("ad_group_name")) if level == "ad_group" else None
        ids, count = [], 0
        for _, o in lst[lo:hi]:
            if o is r or (ag is not None and o.get("ad_group_name") and _entity_key(o.get("ad_group_name")) != ag):
                continue
            count += 1
            if len(ids) < 20:
                ids.append(o["change_id"])
        entity = r.get("campaign_name") + (f" / {r.get('ad_group_name')}" if level == "ad_group" else "")
        out[r["change_id"]] = {"level": level, "entity": entity, "window_days": N, "before": dict(before), "after": dict(after),
                               "overlapping": {"count": count, "change_ids": ids}}
    dq = {"rows_with_window": len(out), "rows_without_metrics": len(rows) - len(out), "entities_unmatched": len(unmatched)}
    return out, dq


def compute_metrics_daily(metrics_files):
    """2026-11-14: per-day sums over all metric files, null when no row had the metric.
    A campaign-day in a campaign-level file wins over ad-group rows of that campaign-day."""
    per = {"campaign": {}, "ad_group": {}}  # (acct, campaign[, ad_group], day) -> values; one file per key
    for mf in metrics_files:
        level = "ad_group" if mf["has_ad_group"] else "campaign"
        for m in mf["rows"]:
            if level == "ad_group" and not m["ad_group"]:
                continue
            acct = _entity_key(m["account"]) if m["account"] else None
            key = (acct, _entity_key(m["campaign"])) + ((_entity_key(m["ad_group"]),) if level == "ad_group" else ()) + (m["day"],)
            prev = per[level].setdefault(key, (mf["file"], []))
            if prev[0] == mf["file"]:
                prev[1].append(m["values"])
    camp_days = {(k[0], k[1], k[-1]) for k in per["campaign"]}
    camp_any_acct = {(k[1], k[-1]) for k in per["campaign"]}
    days = defaultdict(lambda: {f: None for f in METRIC_FIELDS})
    for level, table in per.items():
        for key, (_, vals_list) in table.items():
            acct, ck, day = key[0], key[1], key[-1]
            if level == "ad_group" and ((acct, ck, day) in camp_days or (None, ck, day) in camp_days or (acct is None and (ck, day) in camp_any_acct)):
                continue
            slot = days[day]
            for vals in vals_list:
                for f, v in vals.items():
                    if v is not None:
                        slot[f] = (slot[f] or 0.0) + v
    return [{"date": d.isoformat(), **{f: (round(v, 6) if v is not None else None) for f, v in days[d].items()}} for d in sorted(days)]


def build_metrics_meta(metrics_files, window_days, conversion_lag_days):
    days = [m["day"] for mf in metrics_files for m in mf["rows"]]
    levels = sorted({"ad_group" if mf["has_ad_group"] else "campaign" for mf in metrics_files}, key=["campaign", "ad_group"].index)
    return {"files": [mf["file"] for mf in metrics_files], "levels": levels,
            "date_min": min(days).isoformat() if days else None, "date_max": max(days).isoformat() if days else None,
            "metrics": [f for f in METRIC_FIELDS if any(f in mf["metrics"] for mf in metrics_files)],
            "window_days": window_days, "conversion_lag_days": conversion_lag_days,
            "ignored_columns": list(dict.fromkeys(c for mf in metrics_files for c in mf["ignored_columns"]))}


# (write_digest is added here by another change, 2026-11-14)


def _mapping_fits(path, mapping_file=None, column_map=None):
    """Multi-input only: a --map/--mapping-file applies to a file whose headers it names (2026-11-14)."""
    try:
        headers, _ = read_rows(path, encoding=INPUT_ENCODING)
    except ValueError:
        return True  # let validate_source report the read error
    wanted = set((column_map or {}).values())
    if mapping_file:
        try:
            with open(mapping_file, encoding="utf-8") as f:
                conf = json.load(f)
            m = conf.get("mapping", conf) if isinstance(conf, dict) else {}
            wanted |= {v for v in m.values() if isinstance(v, str)} if isinstance(m, dict) else set()
        except (OSError, ValueError):
            return True  # validate_source reports it
    return wanted <= set(headers)


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
        "worth": "Worth a look", "worth_none": "No rule threshold was crossed.", "worth_more": "{n} more in dashboard.html.",
        "worth_info": "For information", "worth_off": "off", "worth_off_rules": "Rules turned off",
        "worth_note": "Each line is a fact measured against a threshold you can change (--rules-config).",
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
        "worth": "Göz atılacaklar", "worth_none": "Hiçbir kural eşiği aşılmadı.", "worth_more": "{n} tane daha dashboard.html içinde.",
        "worth_info": "Bilgi için", "worth_off": "kapalı", "worth_off_rules": "Kapalı kurallar",
        "worth_note": "Her satır, değiştirilebilir bir eşiğe göre ölçülmüş bir olgudur (--rules-config).",
    },
}
# Turkish wording for the pipeline's coverage note ids; an unknown id keeps its English text.
_COVERAGE_NOTES_TR = {
    "editor_changes_absent": "Google Ads API değişiklik geçmişi, Google Ads Editor ile yapılan değişiklikleri döndürmez; bu değişiklikler API verisinde yer almaz.",
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


# 2026-11-19 (round 12): rule flags, exec summary and campaign table.
# Flags are facts against user-set thresholds ("worth a look"), never a verdict.
RULE_DEFS = (  # display order; params are defaults, overridable by --rules-config
    ("budget_jump", "attention", {"pct": 50}),
    ("budget_cumulative", "attention", {"pct": 50, "hours": 24}),
    ("reverted", "attention", {"add_remove_minutes": 60, "value_days": 7, "status_hours": 24}),
    ("repeat_edit", "attention", {"minutes": 10, "min_count": 2}),
    ("burst_session", "attention", {"minutes": 3, "min_events": 3}),
    ("bid_target_relearn", "attention", {"days": 7}),
    ("automation_source", "attention", {}),
    ("unknown_user", "attention", {}),
    ("stale_entity", "info", {"days": 30}),
    ("stale_budget", "info", {"days": 60}),
    ("off_hours", "info", {"start": "20:00", "end": "08:00", "weekends": True}),
    ("bulk_edit", "info", {"minutes": 10, "min_changes": 20, "min_campaigns": 5}),
)
_RULE_COUNT_PARAMS = {"min_count", "min_events", "min_changes", "min_campaigns"}
_HHMM_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")
RULE_TEXT = {  # {name} placeholders are filled from the rule's params
    "budget_jump": ({"tr": "Bütçe tek seferde %{pct} ya da daha fazla değişti", "en": "Budget changed by {pct}% or more in one step"},
                    {"tr": "Günlük bütçe tek değişiklikte %{pct} ya da daha fazla arttı veya azaldı.", "en": "A daily budget went up or down by {pct}% or more in a single change."}),
    "budget_cumulative": ({"tr": "Bütçe {hours} saat içinde toplamda %{pct} ya da daha fazla değişti", "en": "Budget moved {pct}% or more within {hours} hours"},
                          {"tr": "Aynı kampanyada {hours} saat içindeki birden çok bütçe değişikliği toplamda %{pct} ya da daha fazla fark yarattı. Tek adımı zaten listelenenler hariç.",
                           "en": "Several budget changes on one campaign within {hours} hours add up to {pct}% or more. Windows with a step already listed on its own are left out."}),
    "reverted": ({"tr": "Eklenip hemen geri alınan değişiklik", "en": "Changes undone soon after"},
                 {"tr": "Aynı öğe {add_remove_minutes} dakika içinde eklenip kaldırıldı, bir ayar {value_days} gün içinde önceki değerine döndü ya da durum {status_hours} saat içinde geri çevrildi.",
                  "en": "The same item was added and removed within {add_remove_minutes} minutes, a setting went back to an earlier value within {value_days} days, or a status was switched back within {status_hours} hours."}),
    "repeat_edit": ({"tr": "Aynı ayar birkaç dakika içinde tekrar değişti", "en": "Same setting edited again within minutes"},
                    {"tr": "Aynı kişi aynı kampanyada aynı öğenin aynı ayarını {minutes} dakika içinde {min_count} ya da daha fazla kez art arda değiştirdi; her değişiklik bir öncekinin bıraktığı değerden başladı. Yeni kampanya kurulumları hariç.",
                     "en": "One person edited the same setting of the same item on one campaign {min_count} or more times in a row within {minutes} minutes, each edit starting from the value the previous one left. New campaign setups are left out."}),
    "burst_session": ({"tr": "Kısa sürede çok sayıda değişiklik", "en": "Many changes in a few minutes"},
                      {"tr": "Aynı kişi aynı kampanyada {minutes} dakika içinde {min_events} ya da daha fazla değişiklik olayı oluşturdu. Yeni kampanya kurulumları hariç.",
                       "en": "One person created {min_events} or more change events on one campaign within {minutes} minutes. New campaign setups are left out."}),
    "bid_target_relearn": ({"tr": "Teklif hedefi {days} gün içinde yeniden değişti", "en": "Bid target changed again within {days} days"},
                           {"tr": "Hedef EBM, hedef ROAS ya da teklif stratejisi önceki değişiklikten sonraki {days} gün içinde yeniden değiştirildi.",
                            "en": "Target CPA, target ROAS or the bid strategy was changed again within {days} days of the previous change."}),
    "automation_source": ({"tr": "Otomasyon ya da Google kaynaklı değişiklik", "en": "Changes by automation or Google"},
                          {"tr": "Değişikliği bir kişi değil otomatik bir kaynak (kural, komut dosyası, öneri) ya da türü belirlenemeyen adlı bir kullanıcı yaptı, örneğin kullanıcı izniyle çalışan bir Google temsilcisi.",
                           "en": "The change came from an automated source (rule, script, recommendation) or from a named user of unknown type, such as a Google representative acting with user permission."}),
    "unknown_user": ({"tr": "Listede olmayan kullanıcı", "en": "User not on the known list"},
                     {"tr": "Değişikliği yapan kişi verilen bilinen kullanıcılar listesinde yok.", "en": "The person who made the change is not on the known-users list you gave."}),
    "stale_entity": ({"tr": "Uzun süredir dokunulmayan kampanya", "en": "Campaigns left untouched for a long time"},
                     {"tr": "Son değişikliği verideki en yeni günden {days} gün ya da daha önce olan kampanya ve reklam grupları.",
                      "en": "Campaigns and ad groups whose last change is {days} or more days before the newest day in the data."}),
    "stale_budget": ({"tr": "Bütçesi uzun süredir değişmeyen kampanya", "en": "Budget unchanged for a long time"},
                     {"tr": "Son bütçe değişikliği verideki en yeni günden {days} gün ya da daha önce olan kampanyalar. Dışa aktarımda hiç bütçe değişikliği olmayan kampanya, ilk ve son kayıt arasında en az {days} gün varsa listelenir.",
                      "en": "Campaigns whose last budget change is {days} or more days before the newest day in the data. A campaign with no budget change in the export is listed when the export's first and last records are at least {days} days apart."}),
    "off_hours": ({"tr": "Mesai dışı yapılan değişiklik", "en": "Changes outside working hours"},
                  {"tr": "Yerel saatle {start}-{end} arası{weekend_tr} yapılan değişiklikler.", "en": "Changes made between {start} and {end} local time{weekend_en}."}),
    "bulk_edit": ({"tr": "Toplu düzenleme", "en": "Bulk edit"},
                  {"tr": "Bir kişi {minutes} dakika içinde en az {min_campaigns} kampanyada {min_changes} ya da daha fazla değişiklik yaptı.",
                   "en": "One person made {min_changes} or more changes across at least {min_campaigns} campaigns within {minutes} minutes."}),
}
_RULE_OFF_REASON = {
    "config": {"tr": "Kural ayarlarında kapatıldı.", "en": "Turned off in the rules config."},
    "no_known_users": {"tr": "Bilinen kullanıcılar listesi verilmedi (--known-users).", "en": "No known-users list was given (--known-users)."},
    "no_timezone": {"tr": "Saat dilimi bilinmiyor (--timezone verilmedi), yerel saat hesaplanamıyor.", "en": "The time zone is unknown (no --timezone), so local time can't be worked out."},
}
_SUB_NAMES = {  # subcategory -> (tr, en) for flag sentences; the data keeps the English value
    "Budget changed": ("Bütçe", "Budget"), "Bid changed": ("Teklif", "Bid"), "Strategy changed": ("Teklif stratejisi", "Bid strategy"),
    "Target CPA": ("Hedef EBM", "Target CPA"), "Target ROAS": ("Hedef ROAS", "Target ROAS"), "Bid modifier changed": ("Teklif ayarlaması", "Bid adjustment"),
    "Negative keyword": ("Negatif anahtar kelime", "Negative keyword"), "Ad changed": ("Reklam", "Ad"), "Asset changed": ("Öğe", "Asset"),
    "Asset set changed": ("Öğe grubu", "Asset set"), "Location targeting changed": ("Konum hedeflemesi", "Location targeting"),
    "Device targeting changed": ("Cihaz hedeflemesi", "Device targeting"), "Ad schedule changed": ("Reklam zaman planlaması", "Ad schedule"),
    "Language targeting changed": ("Dil hedeflemesi", "Language targeting"), "Audience added": ("Kitle", "Audience"),
    "Audience bid adjustment changed": ("Kitle teklif ayarlaması", "Audience bid adjustment"), "Enabled": ("Durum", "Status"), "Paused": ("Durum", "Status"),
    "Campaign name changed": ("Kampanya adı", "Campaign name"), "Campaign settings changed": ("Kampanya ayarları", "Campaign settings"),
    "Ad group name changed": ("Reklam grubu adı", "Ad group name"), "Ad group settings changed": ("Reklam grubu ayarları", "Ad group settings"),
    "Conversion action changed": ("Dönüşüm işlemi", "Conversion action"), "Conversion tracking changed": ("Dönüşüm izleme", "Conversion tracking"),
    "Feed changed": ("Feed", "Feed"), "Feed item changed": ("Feed öğesi", "Feed item"), "Account settings changed": ("Hesap ayarları", "Account settings"),
    "Keyword added": ("Anahtar kelime", "Keyword"), "Asset added": ("Öğe", "Asset"),
}
_FAMILY_NAMES = {"keyword": ("Anahtar kelime", "Keyword"), "negative_keyword": ("Negatif anahtar kelime", "Negative keyword"), "asset": ("Öğe", "Asset"),
                 "ad": ("Reklam", "Ad"), "audience": ("Kitle", "Audience"), "feed_item": ("Feed öğesi", "Feed item"), None: ("Öğe", "Item")}
_ADD_REMOVE_SUBS = {"Keyword added": ("keyword", "add"), "Keyword removed": ("keyword", "remove"), "Asset added": ("asset", "add"),
                    "Asset removed": ("asset", "remove"), "Ad created": ("ad", "add"), "Ad removed": ("ad", "remove"),
                    "Audience added": ("audience", "add"), "Audience removed": ("audience", "remove"), "Feed item added": ("feed_item", "add"),
                    "Feed item removed": ("feed_item", "remove"), "Negative keyword": ("negative_keyword", None), "Removed": (None, "remove")}
_SETUP_SUBS = {"Campaign created", "Ad group created", "Budget created"}
_BID_SUBS = {"Target CPA", "Target ROAS", "Strategy changed"}
_BID_LIMIT_RE = re.compile(r"(teklif sınırı|bid limit)", re.IGNORECASE)
_ITEM_ID_RE = re.compile(r"^\s*(?:öğe kimliği|asset id|item id|reklam kimliği|ad id|feed item id)\s*:\s*(\d+)\s*$", re.IGNORECASE)
_ITEM_TYPE_RE = re.compile(r"^\s*(?:uzantı türü|öğe türü|extension type|asset type)\s*:\s*(.+?)\s*$", re.IGNORECASE)
_ITEM_LINK_RE = re.compile(r"^\s*(?:bağlantı metni|site bağlantısı metni|link text|sitelink text)\s*:\s*(.+?)\s*$", re.IGNORECASE)
_ITEM_STATUS_RE = re.compile(r"^\s*(.+?):\s*(?:\S+\s+olan\s+durum\b|status changed\b|\S+\s*->\s*\S+\s*$)", re.IGNORECASE)
_NOT_ITEM_LINE_RE = re.compile(r"(\bolan\b.*\bolarak\b|\bchanged from\b|\bidi\s*$|\bwas\b|->)", re.IGNORECASE)
_INLINE_ASSET_ID_RE = re.compile(r"\((?:Asset ID|Öğe kimliği):\s*(\d+)\)", re.IGNORECASE)
_REMOVED_WORD_RE = re.compile(r"^\s*(kaldırıldı|removed|silindi|deleted)\s*$", re.IGNORECASE)
_WEEKDAYS = {"tr": ("Pazartesi", "Salı", "Çarşamba", "Perşembe", "Cuma", "Cumartesi", "Pazar"),
             "en": ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")}
_CUR_SYMBOL = {"TRY": "₺", "USD": "$", "EUR": "€", "GBP": "£"}


def _parse_work_hours(text):
    """--work-hours 08-20 (or 08:30-19:00) -> off_hours start/end ("20:00", "08:00")."""
    m = re.match(r"^\s*(\d{1,2})(?::(\d{2}))?\s*-\s*(\d{1,2})(?::(\d{2}))?\s*$", str(text or ""))
    if not m:
        raise ValueError(f"--work-hours must look like 08-20 or 08:30-19:00, got {text!r}.")
    a, b = f"{int(m.group(1)):02d}:{m.group(2) or '00'}", f"{int(m.group(3)):02d}:{m.group(4) or '00'}"
    if not (_HHMM_RE.match(a) and _HHMM_RE.match(b)) or a == b:
        raise ValueError(f"--work-hours must name two different times of day, got {text!r}.")
    return {"start": b, "end": a}


def resolve_rules_config(user_cfg=None, work_hours=None):
    """(cfg, error): cfg is rule -> {tier, enabled, params, off}; user_cfg is the
    --rules-config JSON {rule: {enabled, ...params}} (2026-11-19)."""
    cfg = {name: {"tier": tier, "enabled": True, "params": dict(params), "off": None} for name, tier, params in RULE_DEFS}
    if user_cfg is not None and not isinstance(user_cfg, dict):
        return None, "--rules-config must be a JSON object {rule: {enabled, ...params}}."
    for name, over in (user_cfg or {}).items():
        if name not in cfg:
            return None, f"Unknown rule in --rules-config: {name!r}. Known rules: {', '.join(n for n, _, _ in RULE_DEFS)}."
        if not isinstance(over, dict):
            return None, f"--rules-config {name}: expected an object like {{\"enabled\": true, ...}}."
        for k, v in over.items():
            defaults = cfg[name]["params"]
            if k == "enabled":
                if not isinstance(v, bool):
                    return None, f"--rules-config {name}.enabled must be true or false."
                cfg[name]["enabled"] = v
            elif k not in defaults:
                return None, f"--rules-config {name}: unknown setting {k!r}. Settings: {', '.join(['enabled'] + list(defaults)) }."
            elif k in ("start", "end"):
                if not isinstance(v, str) or not _HHMM_RE.match(v):
                    return None, f"--rules-config {name}.{k} must be a time like \"20:00\"."
                defaults[k] = f"{int(v.split(':')[0]):02d}:{v.split(':')[1]}"
            elif k == "weekends":
                if not isinstance(v, bool):
                    return None, f"--rules-config {name}.weekends must be true or false."
                defaults[k] = v
            else:
                if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0 or (k in _RULE_COUNT_PARAMS and int(v) != v):
                    return None, f"--rules-config {name}.{k} must be a positive {'whole ' if k in _RULE_COUNT_PARAMS else ''}number."
                defaults[k] = int(v) if int(v) == v else v
    if work_hours:
        try:
            cfg["off_hours"]["params"].update(_parse_work_hours(work_hours))
        except ValueError as e:
            return None, str(e)
    if cfg["off_hours"]["params"]["start"] == cfg["off_hours"]["params"]["end"]:
        return None, "--rules-config off_hours.start and .end must differ."
    for c in cfg.values():
        if not c["enabled"]:
            c["off"] = "config"
    return cfg, None


def _r12_num(v, lang, dec=0):
    s = f"{v:,.{dec}f}"
    return s.replace(",", "\x00").replace(".", ",").replace("\x00", ".") if lang == "tr" else s


def _r12_param(v, lang):
    return _r12_num(v, lang, 0 if float(v).is_integer() else 1) if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)


def _r12_money(v, cur, lang, dec=2):
    body = _r12_num(abs(v), lang, dec)
    sign = "-" if v < 0 else ""
    if cur == "%":
        return f"{sign}%{body}" if lang == "tr" else f"{sign}{body}%"
    sym = _CUR_SYMBOL.get(cur)
    return f"{sign}{sym}{body}" if sym else (f"{sign}{body} {cur}" if cur else sign + body)


def _r12_pct(p, lang):
    s = "+" if p > 0 else ("-" if p < 0 else "")
    body = _r12_num(abs(p), lang, 0)
    return f"{s}%{body}" if lang == "tr" else f"{s}{body}%"


def _r12_span(sec, lang, after=False):
    """Duration words: 'within N' rounds up, 'N later' rounds to nearest."""
    import math
    sec = max(0.0, sec or 0.0)
    if after and sec < 60:
        return "1 dakikadan kısa süre sonra" if lang == "tr" else "less than a minute later"
    rnd = round if after else math.ceil
    for size, tr_w, en_w in ((60, "dakika", "minute"), (3600, "saat", "hour"), (86400, "gün", "day")):
        n = max(1, int(rnd(sec / size)))
        if size == 86400 or (size == 60 and sec < 3600) or (size == 3600 and sec < 2 * 86400):
            words = f"{n} {tr_w}" if lang == "tr" else f"{n} {en_w}{'' if n == 1 else 's'}"
            return (words + (" sonra" if lang == "tr" else " later")) if after else words
    return ""


def _r12_cur(r):
    c = r.get("currency") or r.get("value_unit")
    return c if c and (c == "%" or CURRENCY_CODE_RE.match(str(c))) else None


def _r12_epoch(r):
    v = r.get("timestamp_utc")
    if v:
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    v = r.get("timestamp_iso")
    if v:
        try:
            return datetime.fromisoformat(str(v)[:19]).replace(tzinfo=dt_timezone.utc).timestamp()
        except ValueError:
            pass
    return None


def _r12_actor(r):
    if r.get("actor_type") == "automation":
        return r.get("client_type") or r.get("user_name") or "Automation"
    return r.get("user_name") or r.get("user_email") or None


def _r12_item_keys(r, family=None):
    """Identity keys of the item(s) one statement names: API resource id, else an
    asset/ad id line, a sitelink text, or plain item lines (keyword text)."""
    if r.get("resource_id") or r.get("resource_name"):
        return {("res", str(r.get("resource_type") or ""), str(r.get("resource_id") or r.get("resource_name")))}, None
    _, details = split_statement(r.get("raw_summary") or "")
    ext = next((m.group(1).strip('"') for m in map(_ITEM_TYPE_RE.match, details) if m), None)
    ids = {m.group(1) for m in map(_ITEM_ID_RE.match, details) if m}
    if ids:
        return {("id", x) for x in ids}, ext
    if family == "ad":
        inline = sorted(set(_INLINE_ASSET_ID_RE.findall("\n".join(details))))
        return ({("ad", ",".join(inline))} if inline else set()), ext
    links = {("text", _entity_key(m.group(1)), m.group(1).strip()) for m in map(_ITEM_LINK_RE.match, details) if m}
    if links:
        return links, ext
    keys = set()
    for line in details:
        m = _ITEM_STATUS_RE.match(line)
        if m and not _NOT_ITEM_LINE_RE.search(m.group(1)):
            keys.add(("text", _entity_key(m.group(1)), m.group(1).strip()))
        elif ":" not in line and not _NOT_ITEM_LINE_RE.search(line):
            keys.add(("text", _entity_key(line), line.strip()))
    return keys, ext


_REPEAT_SINGLE_SUBS = {"Budget changed", "Target CPA", "Target ROAS", "Strategy changed"}
_ITEM_TARGET_RE = re.compile(r"^\s*([^\n]{1,200}?):\s+(?=.*(?:->|\b(?:olarak ayarlandı|olarak değiştirildi|değerine|olan|artırıldı|düşürüldü|set to|changed|increased|decreased)\b))", re.IGNORECASE)


def _r12_repeat_keys(r):
    """Items one UPDATE statement edits, for repeat_edit: the campaign's budget or bid
    target, an API resource, an id line or a 'target: change' line; empty when the
    statement doesn't say which item (two such statements are not a repeat)."""
    if r.get("resource_id") or r.get("resource_name"):
        return {("res", str(r.get("resource_type") or ""), str(r.get("resource_id") or r.get("resource_name")))}
    if r.get("subcategory") in _REPEAT_SINGLE_SUBS:
        return {("setting",)}
    _, details = split_statement(r.get("raw_summary") or "")
    ids = {("id", m.group(1)) for m in map(_ITEM_ID_RE.match, details) if m}
    if ids:
        return ids
    return {("text", _entity_key(m.group(1)), m.group(1).strip()) for m in map(_ITEM_TARGET_RE.match, details)
            if m and not _NOT_ITEM_LINE_RE.search(m.group(1))}


def _r12_val(r, side):
    """Comparable old/new value of a row, or None when the export doesn't give it."""
    v = r.get(side + "_value_num")
    if isinstance(v, (int, float)):
        return ("n", round(float(v), 6))
    v = r.get(side + "_value")
    return ("s", _entity_key(v)) if v not in (None, "") else None


def _r12_key(k):
    """Matching part of an item key (text keys carry their display text third)."""
    return k[:2] if k[0] == "text" else k


def _r12_item_label(key):
    return key[2] if key[0] == "res" else (key[2].lstrip("-").strip('"[]') if key[0] == "text" else None)


def compute_rule_flags(rows, cfg, known=None, metrics_files=None):
    """(flags, extra): flags sorted attention-first then newest-first, ids F1..;
    rows are the final (masked) rows; known[i] says whether row i's person is on --known-users."""
    import bisect
    n = len(rows)
    ep = [_r12_epoch(r) for r in rows]
    acct = [account_identity(r) for r in rows]
    camp = [campaign_identity(r) for r in rows]
    adg = [ad_group_identity(r) for r in rows]
    who = [user_identity(r) if r.get("actor_type") == "human" else None for r in rows]
    sub = [r.get("subcategory") for r in rows]
    op = [r.get("operation") for r in rows]
    evid = [r.get("event_id") for r in rows]
    raw = []  # (rule, row indices, fact, text, overrides)

    def on(rule):
        return cfg[rule]["enabled"] and not cfg[rule]["off"]

    def P(rule):
        return cfg[rule]["params"]

    def by_time(idx):
        return sorted(idx, key=lambda i: (ep[i], i))

    def events_of(idx):
        """Distinct events (event_id) of row indices -> [(t, [rows])] by time."""
        ev = {}
        for i in idx:
            ev.setdefault(evid[i] or ("row", i), []).append(i)
        return sorted(((min(ep[i] for i in v), v) for v in ev.values()), key=lambda e: (e[0], e[1][0]))

    timed = [i for i in range(n) if ep[i] is not None]
    setup = defaultdict(list)
    for i in timed:
        if sub[i] in _SETUP_SUBS and camp[i]:
            setup[(who[i], acct[i], camp[i])].append(ep[i])
    for v in setup.values():
        v.sort()

    def is_setup(key, t0, t1, minutes):
        times = setup.get(key) or []
        k = bisect.bisect_left(times, t0 - minutes * 60)
        return k < len(times) and times[k] <= t1 + minutes * 60

    # 1 budget_jump
    budget_num = [i for i in range(n) if sub[i] == "Budget changed"
                  and isinstance(rows[i].get("old_value_num"), (int, float)) and isinstance(rows[i].get("new_value_num"), (int, float))
                  and rows[i]["old_value_num"] > 0]
    jumped = set()
    if on("budget_jump"):
        for i in budget_num:
            o, nw = rows[i]["old_value_num"], rows[i]["new_value_num"]
            pct = (nw - o) / o * 100
            if abs(pct) >= P("budget_jump")["pct"] - 1e-9:
                jumped.add(i)
                cur = _r12_cur(rows[i])
                raw.append(("budget_jump", [i], {"old": o, "new": nw, "pct": round(pct, 1), "currency": cur},
                            {lg: f"{'Bütçe' if lg == 'tr' else 'Budget'} {_r12_money(o, cur, lg)} → {_r12_money(nw, cur, lg)} ({_r12_pct(pct, lg)})" for lg in ("tr", "en")}, {}))
    # 2 budget_cumulative
    if on("budget_cumulative"):
        pc, hrs = P("budget_cumulative")["pct"], P("budget_cumulative")["hours"]
        groups = defaultdict(list)
        for i in budget_num:
            if ep[i] is not None and camp[i]:
                groups[(acct[i], camp[i])].append(i)
        for seq in groups.values():
            seq = by_time(seq)
            a = 0
            while a < len(seq):
                i = seq[a]
                if i in jumped:
                    a += 1
                    continue
                best, b = None, a + 1
                while b < len(seq) and ep[seq[b]] - ep[i] <= hrs * 3600:
                    if seq[b] in jumped:
                        break
                    cum = (rows[seq[b]]["new_value_num"] - rows[i]["old_value_num"]) / rows[i]["old_value_num"] * 100
                    if abs(cum) >= pc - 1e-9 and (best is None or abs(cum) > abs(best[1])):
                        best = (b, cum)
                    b += 1
                if not best:
                    a += 1
                    continue
                win = seq[a:best[0] + 1]
                o, nw, cur = rows[i]["old_value_num"], rows[seq[best[0]]]["new_value_num"], _r12_cur(rows[i])
                span = ep[win[-1]] - ep[win[0]]
                raw.append(("budget_cumulative", win, {"old": o, "new": nw, "pct": round(best[1], 1), "steps": len(win), "hours": round(span / 3600, 1), "currency": cur},
                            {"tr": f"Bütçe {_r12_span(span, 'tr')} içinde {len(win)} adımda {_r12_money(o, cur, 'tr')} → {_r12_money(nw, cur, 'tr')} ({_r12_pct(best[1], 'tr')})",
                             "en": f"Budget {_r12_money(o, cur, 'en')} → {_r12_money(nw, cur, 'en')} in {len(win)} steps within {_r12_span(span, 'en')} ({_r12_pct(best[1], 'en')})"}, {}))
                a = best[0] + 1
    # 3 reverted
    if on("reverted"):
        prm = P("reverted")
        adds, removes = [], defaultdict(list)
        for i in timed:
            fam = _ADD_REMOVE_SUBS.get(sub[i])
            if not fam:
                continue
            kind = fam[1] or {"CREATE": "add", "REMOVE": "remove"}.get(op[i])
            if (kind == "add" and op[i] != "CREATE") or (kind == "remove" and op[i] not in ("REMOVE", "UPDATE")) or not kind:
                continue
            keys, ext = _r12_item_keys(rows[i], fam[0])
            for k in keys:
                if kind == "add":
                    adds.append((i, fam[0], k, ext))
                else:
                    removes[(acct[i], camp[i], adg[i], _r12_key(k))].append((ep[i], i, fam[0]))
        pairs = {}  # one flag per (add event, remove event, family), 2026-11-19
        for i, fam, k, ext in adds:
            cands = [(t, j) for t, j, f in removes.get((acct[i], camp[i], adg[i], _r12_key(k)), ())
                     if (f is None or f == fam) and j != i and evid[j] != evid[i] and 0 <= t - ep[i] <= prm["add_remove_minutes"] * 60]
            if cands:
                j = min(cands)[1]
                p_ = pairs.setdefault((evid[i] or i, evid[j] or j, fam), {"rows": [], "keys": {}, "exts": set()})
                p_["rows"] += [i, j]
                p_["keys"].setdefault(_r12_key(k), k)
                p_["exts"].add(ext)
        for (_, _, fam), p_ in pairs.items():
            keys, exts = list(p_["keys"].values()), {e for e in p_["exts"] if e}
            names = _FAMILY_NAMES.get(fam, _FAMILY_NAMES[None])
            label = _r12_item_label(keys[0]) if len(keys) == 1 else None
            ext = next(iter(exts)) if len(exts) == 1 else None
            span = max(ep[x] for x in p_["rows"]) - min(ep[x] for x in p_["rows"])
            txt = {}
            for lg, nm in (("tr", names[0]), ("en", names[1])):
                what = (f"{len(keys)} {nm.lower() if lg == 'tr' else nm.lower() + 's'}" + (f" ({ext})" if ext else "")) if len(keys) > 1 else (
                    f'{nm} "{label}"' if label else (f"{nm} ({ext})" if ext else nm))
                txt[lg] = f"{what} eklendi, {_r12_span(span, 'tr', True)} kaldırıldı" if lg == "tr" else f"{what} added, removed {_r12_span(span, 'en', True)}"
            raw.append(("reverted", p_["rows"], {"kind": "add_remove", "items": len(keys), "minutes": round(span / 60, 1)}, txt, {}))
        # (b) numeric setting back to an earlier value
        vals = defaultdict(list)
        for i in timed:
            r = rows[i]
            o, nw = r.get("old_value_num"), r.get("new_value_num")
            if not isinstance(o, (int, float)) or not isinstance(nw, (int, float)) or o == nw or not camp[i]:
                continue
            kind = {"Budget changed": "budget", "Target CPA": "target_cpa", "Target ROAS": "target_roas"}.get(sub[i]) or (
                "bid_limit" if _BID_LIMIT_RE.search(str(r.get("raw_summary") or r.get("field_name") or "")) else None)
            if kind:
                vals[(acct[i], camp[i], adg[i], kind)].append(i)
        kind_names = {"budget": ("Bütçe", "Budget"), "target_cpa": ("Hedef EBM", "Target CPA"), "target_roas": ("Hedef ROAS", "Target ROAS"), "bid_limit": ("Teklif sınırı", "Bid limit")}
        for (_, _, _, kind), seq in vals.items():
            seq = by_time(seq)
            for b in range(1, len(seq)):
                k = seq[b]
                back = rows[k]["new_value_num"]
                for a in range(b - 1, -1, -1):
                    j = seq[a]
                    if ep[k] - ep[j] > prm["value_days"] * 86400:
                        break
                    if abs(rows[j]["old_value_num"] - back) <= 1e-6 * max(1.0, abs(back)):
                        cur, span = _r12_cur(rows[k]), ep[k] - ep[j]
                        o, nw = rows[j]["old_value_num"], rows[j]["new_value_num"]
                        nm = kind_names[kind]
                        raw.append(("reverted", seq[a:b + 1], {"kind": "value", "setting": kind, "old": o, "new": nw, "back": back, "days": round(span / 86400, 1), "currency": cur},
                                    {"tr": f"{nm[0]} {_r12_money(o, cur, 'tr')} iken {_r12_money(nw, cur, 'tr')} yapıldı, {_r12_span(span, 'tr', True)} yeniden {_r12_money(back, cur, 'tr')} oldu",
                                     "en": f"{nm[1]} went from {_r12_money(o, cur, 'en')} to {_r12_money(nw, cur, 'en')} and was back at {_r12_money(back, cur, 'en')} {_r12_span(span, 'en', True)}"}, {}))
                        break
        # (c) status paused <-> enabled and back
        status = defaultdict(list)
        for i in timed:
            if sub[i] not in ("Paused", "Enabled") or (sub[i] == "Enabled" and _REMOVED_WORD_RE.match(str(rows[i].get("old_value") or ""))):
                continue
            keys, _ = _r12_item_keys(rows[i])
            for k in (keys or {("self",)}):
                status[(acct[i], camp[i], adg[i], _r12_key(k))].append((i, k))
        spairs = defaultdict(list)
        for key, seq in status.items():
            full = dict(seq)
            seq = by_time(full)
            for a in range(1, len(seq)):
                j, k = seq[a - 1], seq[a]
                if sub[j] != sub[k] and ep[k] - ep[j] <= prm["status_hours"] * 3600:
                    spairs[(j, k)].append(full[j])
        for (j, k), keys in spairs.items():
            span, first = ep[k] - ep[j], sub[j]
            label = _r12_item_label(keys[0]) if len(keys) == 1 and keys[0] != ("self",) else None
            subj_tr = f'"{label}"' if label else (f"{len(keys)} öğe" if len(keys) > 1 else ("Reklam grubu" if adg[j] else "Kampanya"))
            subj_en = f'"{label}"' if label else (f"{len(keys)} items" if len(keys) > 1 else ("Ad group" if adg[j] else "Campaign"))
            v1, v2 = ("duraklatıldı", "etkinleştirildi") if first == "Paused" else ("etkinleştirildi", "duraklatıldı")
            e1, e2 = ("paused", "enabled") if first == "Paused" else ("enabled", "paused")
            raw.append(("reverted", [j, k], {"kind": "status", "first": e1, "then": e2, "items": len(keys), "hours": round(span / 3600, 1)},
                        {"tr": f"{subj_tr} {v1}, {_r12_span(span, 'tr', True)} yeniden {v2}", "en": f"{subj_en} {e1}, {e2} again {_r12_span(span, 'en', True)}"}, {}))
    # 4 repeat_edit: the same item edited again, each edit starting where the last one ended (2026-11-19)
    if on("repeat_edit"):
        mins, mc = P("repeat_edit")["minutes"], P("repeat_edit")["min_count"]
        groups = defaultdict(list)
        for i in timed:
            if who[i] and camp[i] and op[i] == "UPDATE":
                for k in _r12_repeat_keys(rows[i]):
                    groups[(who[i], acct[i], camp[i], adg[i], sub[i], _r12_key(k))].append((i, k))
        seen_win = set()
        for (u, ac, cp, _, sc, _), pairs_ in groups.items():
            label = {i: k for i, k in pairs_}
            seq, last_ev = [], None
            for i in by_time(label):
                if evid[i] is None or evid[i] != last_ev:  # one save = one edit
                    seq.append(i)
                last_ev = evid[i]
            segs, cur_seg = [], seq[:1]
            for prev, i in zip(seq, seq[1:]):
                a_, b_ = _r12_val(rows[prev], "new"), _r12_val(rows[i], "old")
                if a_ is None or b_ is None or a_ == b_:
                    cur_seg.append(i)
                else:
                    segs.append(cur_seg)
                    cur_seg = [i]
            segs.append(cur_seg)
            for sg in segs:
                a = b = 0
                while a < len(sg):
                    b = max(b, a)
                    while b + 1 < len(sg) and ep[sg[b + 1]] - ep[sg[a]] <= mins * 60:
                        b += 1
                    if b - a + 1 >= mc:
                        win = sg[a:b + 1]
                        if not is_setup((u, ac, cp), ep[win[0]], ep[win[-1]], mins) and tuple(win) not in seen_win:
                            seen_win.add(tuple(win))
                            span, nm, k = ep[win[-1]] - ep[win[0]], _SUB_NAMES.get(sc, (sc, sc)), label[win[0]]
                            item = f' "{_r12_item_label(k)}"' if k[0] == "text" else ""
                            nums = [rows[win[0]].get("old_value_num")] + [rows[i].get("new_value_num") for i in win]
                            chain = {lg: "" for lg in ("tr", "en")}
                            if all(isinstance(v, (int, float)) for v in nums) and len(nums) <= 6:
                                cur = _r12_cur(rows[win[0]])
                                chain = {lg: " (" + " → ".join(_r12_money(v, cur, lg) for v in nums) + ")" for lg in ("tr", "en")}
                            raw.append(("repeat_edit", win, {"subcategory": sc, "count": len(win), "minutes": round(span / 60, 1)},
                                        {"tr": f"{nm[0]}{item} {_r12_span(span, 'tr')} içinde {len(win)} kez art arda değişti{chain['tr']}",
                                         "en": f"{nm[1]}{item} changed {len(win)} times in a row within {_r12_span(span, 'en')}{chain['en']}"}, {}))
                        a = b + 1
                    else:
                        a += 1
    # 5 burst_session
    if on("burst_session"):
        mins, me = P("burst_session")["minutes"], P("burst_session")["min_events"]
        groups = defaultdict(list)
        for i in timed:
            if who[i] and camp[i]:
                groups[(who[i], acct[i], camp[i])].append(i)
        for key, idx in groups.items():
            evs = events_of(idx)
            a = b = 0
            while a < len(evs):
                b = max(b, a)
                while b + 1 < len(evs) and evs[b + 1][0] - evs[a][0] <= mins * 60:
                    b += 1
                if b - a + 1 >= me:
                    if not is_setup(key, evs[a][0], evs[b][0], mins):
                        win = [i for e in evs[a:b + 1] for i in e[1]]
                        span, ne = evs[b][0] - evs[a][0], b - a + 1
                        raw.append(("burst_session", win, {"events": ne, "changes": len(win), "minutes": round(span / 60, 1)},
                                    {"tr": f"{_r12_span(span, 'tr')} içinde {ne} değişiklik olayı ({len(win)} değişiklik)",
                                     "en": f"{ne} change events ({len(win)} changes) within {_r12_span(span, 'en')}"}, {}))
                    a = b + 1
                else:
                    a += 1
    # 6 bid_target_relearn
    if on("bid_target_relearn"):
        days = P("bid_target_relearn")["days"]
        groups = defaultdict(list)
        for i in timed:
            if sub[i] in _BID_SUBS and camp[i]:
                groups[(acct[i], camp[i])].append(i)
        for idx in groups.values():
            evs = events_of(idx)
            for a in range(1, len(evs)):
                gap = evs[a][0] - evs[a - 1][0]
                if gap > days * 86400:
                    continue
                cur_rows = evs[a][1]

                def describe(ev_rows, mid):
                    """'Hedef EBM ₺1 → ₺2' (or 'teklif stratejisi değişti' mid-sentence) per language."""
                    pick = next((i for i in ev_rows if sub[i] != "Strategy changed" and isinstance(rows[i].get("new_value_num"), (int, float))), None)
                    if pick is None:
                        return ({"tr": "teklif stratejisi değişti", "en": "bid strategy changed"} if mid else {"tr": "Teklif stratejisi değişti", "en": "Bid strategy changed"}), None
                    r, cur = rows[pick], _r12_cur(rows[pick])
                    nm, o = _SUB_NAMES[r["subcategory"]], r.get("old_value_num")
                    val = {lg: (f"{_r12_money(o, cur, lg)} → {_r12_money(r['new_value_num'], cur, lg)}" if isinstance(o, (int, float))
                                else (f"{_r12_money(r['new_value_num'], cur, lg)} olarak ayarlandı" if lg == "tr" else f"set to {_r12_money(r['new_value_num'], cur, lg)}")) for lg in ("tr", "en")}
                    return {"tr": f"{nm[0]} {val['tr']}", "en": f"{nm[1]} {val['en']}"}, (r["subcategory"], val)
                d1, k1 = describe(evs[a - 1][1], False)
                d2, k2 = describe(cur_rows, True)
                if k1 and k2 and k1[0] == k2[0]:
                    d2 = k2[1]  # same setting twice: name it once
                after_tr, after_en = _r12_span(gap, "tr", True), _r12_span(gap, "en", True)
                txt = {"tr": f"{d1['tr']}, {after_tr} {d2['tr']}", "en": f"{d1['en']}; {after_en}, {d2['en']}"}
                raw.append(("bid_target_relearn", evs[a - 1][1] + cur_rows,
                            {"days_since_previous": round(gap / 86400, 1), "subcategories": sorted({sub[i] for i in cur_rows})}, txt, {}))
    # 7 automation_source
    if on("automation_source"):
        groups = defaultdict(list)
        for i in range(n):
            r = rows[i]
            if r.get("actor_type") == "automation" or (r.get("actor_type") == "unknown" and _r12_actor(r)):
                groups[(_r12_actor(r), r.get("actor_type"), acct[i], camp[i], (r.get("timestamp_iso") or "")[:10])].append(i)
        for (label, at, _, _, day), idx in groups.items():
            k = len(idx)
            if at == "automation":
                txt = {"tr": f"Otomatik kaynaktan {_r12_num(k, 'tr')} değişiklik ({label})", "en": f"{k:,} change{'s' if k != 1 else ''} from an automated source ({label})"}
            else:
                txt = {"tr": f"Türü belirlenemeyen kullanıcıdan {_r12_num(k, 'tr')} değişiklik ({label})", "en": f"{k:,} change{'s' if k != 1 else ''} from a user of unknown type ({label})"}
            raw.append(("automation_source", idx, {"count": k, "actor_type": at, "day": day or None}, txt, {}))
    # 8 unknown_user
    if on("unknown_user") and known is not None:
        groups = defaultdict(list)
        for i in range(n):
            if who[i] and not known[i]:
                groups[who[i]].append(i)
        for idx in groups.values():
            k, nc = len(idx), len({(acct[i], camp[i]) for i in idx if camp[i]})
            label = _r12_actor(rows[idx[0]])
            raw.append(("unknown_user", idx, {"count": k, "campaigns": nc},
                        {"tr": f"Bilinen kullanıcılar listesinde olmayan {label}, {_r12_num(nc, 'tr')} kampanyada {_r12_num(k, 'tr')} değişiklik yaptı",
                         "en": f"Not on the known-users list: {label} made {k:,} change{'s' if k != 1 else ''} in {nc:,} campaign{'s' if nc != 1 else ''}"}, {"campaign": None, "ad_group": None}))
    # 9/10 stale rules (calendar days on the chrono_key basis, as in the untouched list)
    chrono = [chrono_key(r) for r in rows]
    end_s = max((c for c in chrono if c), default=None)
    start_s = min((c for c in chrono if c), default=None)
    end_d = datetime.fromisoformat(end_s[:19]).date() if end_s else None
    span_days = (end_d - datetime.fromisoformat(start_s[:19]).date()).days if end_s else 0
    last_c, last_g, last_b = {}, {}, {}
    for i in range(n):
        if not chrono[i] or not camp[i]:
            continue
        ck = (acct[i], camp[i])
        if ck not in last_c or chrono[i] > chrono[last_c[ck]]:
            last_c[ck] = i
        if adg[i]:
            gk = ck + (adg[i],)
            if gk not in last_g or chrono[i] > chrono[last_g[gk]]:
                last_g[gk] = i
        if sub[i] in ("Budget changed", "Budget created") and (ck not in last_b or chrono[i] > chrono[last_b[ck]]):
            last_b[ck] = i

    def days_before_end(i):
        return (end_d - datetime.fromisoformat(chrono[i][:19]).date()).days

    ev_rows = defaultdict(list)
    for i in range(n):
        if evid[i]:
            ev_rows[evid[i]].append(i)

    def same_event_rows(i, keyfn):
        ev = evid[i]
        return [j for j in ev_rows[ev] if keyfn(j)] if ev else [i]

    def acc_ok(accs, i):
        if None in accs:
            return True
        return bool(accs & (_acct_keys(str(rows[i].get("account_name") or "")) | _acct_keys(str(rows[i].get("account_id") or ""))))

    def acct_disp(i):
        r = rows[i]
        return r.get("account_name") or (f"Account {r['account_id']}" if r.get("account_id") else None)

    active_c, active_g, metrics_end = _r12_metrics_active(metrics_files) if metrics_files else (None, None, None)
    stale_c, stale_g = set(), set()
    if on("stale_entity") and end_d:
        D = P("stale_entity")["days"]
        for ck, i in last_c.items():
            d = days_before_end(i)
            if d >= D:
                stale_c.add(ck)
                raw.append(("stale_entity", same_event_rows(i, lambda j: (acct[j], camp[j]) == ck), {"days": d, "basis": "export", "level": "campaign"},
                            {"tr": f"Son değişiklik, verideki en yeni günden {_r12_num(d, 'tr')} gün önce yapıldı",
                             "en": f"Last change {d:,} day{'s' if d != 1 else ''} before the newest day in the data"},
                            {"user": None, "ad_group": None}))
        for gk, i in last_g.items():
            d = days_before_end(i)
            if d >= D and gk[:2] not in stale_c:
                stale_g.add(gk)
                raw.append(("stale_entity", same_event_rows(i, lambda j: (acct[j], camp[j], adg[j]) == gk), {"days": d, "basis": "export", "level": "ad_group"},
                            {"tr": f"Reklam grubunda son değişiklik, verideki en yeni günden {_r12_num(d, 'tr')} gün önce yapıldı",
                             "en": f"Last change in this ad group {d:,} day{'s' if d != 1 else ''} before the newest day in the data"},
                            {"user": None}))
        if active_c is not None and metrics_end:
            # 2026-11-19: entities with spend in the metrics but no recent change; the export alone can't show these.
            by_c, by_g = defaultdict(list), defaultdict(list)
            for ck, i in last_c.items():
                by_c[(_entity_key(rows[i].get("campaign_name") or ""),)].append((ck, i))
            for gk, i in last_g.items():
                by_g[(_entity_key(rows[i].get("campaign_name") or ""), _entity_key(rows[i].get("ad_group_name") or ""))].append((gk, i))
            m_flagged = set()
            for level, table, index in (("campaign", active_c, by_c), ("ad_group", active_g, by_g)):
                for nk, ent in sorted(table.items(), key=lambda kv: (kv[1]["campaign"], kv[1]["ad_group"] or "")):
                    cands = [(k, i) for k, i in index.get(nk, []) if acc_ok(ent["accs"], i)]
                    if any(k[:2] in stale_c or k in stale_g for k, _ in cands) or (level == "ad_group" and nk[:1] in m_flagged):
                        continue
                    last = max((i for _, i in cands), key=lambda i: chrono[i], default=None)
                    d = (metrics_end - datetime.fromisoformat(chrono[last][:19]).date()).days if last is not None else None
                    if d is not None and d < D:
                        continue
                    if level == "campaign":
                        m_flagged.add(nk)
                    tr_tail = f"son değişiklik performans verisinin son gününden {_r12_num(d, 'tr')} gün önce" if d is not None else "dışa aktarımda değişikliği yok"
                    en_tail = f"last change {d:,} day{'s' if d != 1 else ''} before its last day" if d is not None else "no change in the export"
                    ov = {"user": None}
                    if last is None:
                        idx = []
                        ov.update({"account": ent["account"], "campaign": ent["campaign"], "ad_group": ent["ad_group"], "ts_first": None, "ts_last": None})
                    else:
                        idx = same_event_rows(last, lambda j: camp[j] == camp[last] and (level == "campaign" or adg[j] == adg[last]))
                        if level == "campaign":
                            ov["ad_group"] = None
                    raw.append(("stale_entity", idx, {"days": d, "basis": "metrics", "level": level},
                                {"tr": f"Performans verisinin son 14 gününde gösterim ya da harcaması var, {tr_tail}",
                                 "en": f"Impressions or spend in the last 14 days of the performance data, {en_tail}"}, ov))
    if on("stale_budget") and end_d:
        D = P("stale_budget")["days"]
        for ck, i in last_c.items():
            if active_c is not None:
                ent = active_c.get((_entity_key(rows[i].get("campaign_name") or ""),))
                if not ent or not acc_ok(ent["accs"], i):
                    continue
            b = last_b.get(ck)
            if b is not None:
                d = days_before_end(b)
                if d >= D:
                    raw.append(("stale_budget", same_event_rows(b, lambda j: (acct[j], camp[j]) == ck and sub[j] in ("Budget changed", "Budget created")),
                                {"days": d, "basis": "export", "no_budget_change_in_export": False},
                                {"tr": f"Son bütçe değişikliği, verideki en yeni günden {_r12_num(d, 'tr')} gün önce yapıldı",
                                 "en": f"Last budget change {d:,} day{'s' if d != 1 else ''} before the newest day in the data"}, {"user": None, "ad_group": None}))
            elif span_days >= D:
                raw.append(("stale_budget", [], {"days": span_days, "basis": "export", "no_budget_change_in_export": True},
                            {"tr": f"Dışa aktarımda bütçe değişikliği yok, ilk ve son kayıt arasında {_r12_num(span_days, 'tr')} gün var",
                             "en": f"No budget change in the export; its first and last records are {span_days:,} days apart"},
                            {"user": None, "ad_group": None, "account": acct_disp(i), "_ck": {ck},
                             "campaign": rows[i].get("campaign_name") or f"Campaign {rows[i].get('campaign_id')}", "ts_first": None, "ts_last": None}))
    # 11 off_hours (local time each row carries)
    tz_rows = [i for i in range(n) if rows[i].get("timezone") not in (None, "", "unknown") and rows[i].get("timestamp_iso")]
    extra = {"off_hours_no_timezone": bool(n) and not tz_rows}
    tz_rows = [i for i in tz_rows if who[i]]  # people only: scheduled automation runs at night by design
    if on("off_hours") and tz_rows:
        prm = P("off_hours")
        s_m = int(prm["start"][:2]) * 60 + int(prm["start"][3:])
        e_m = int(prm["end"][:2]) * 60 + int(prm["end"][3:])
        evs = defaultdict(list)
        for i in tz_rows:
            evs[evid[i] or ("row", i)].append(i)
        for idx in evs.values():
            ts = datetime.fromisoformat(str(rows[idx[0]]["timestamp_iso"])[:19])
            m = ts.hour * 60 + ts.minute
            off_time = (m >= s_m or m < e_m) if s_m > e_m else (s_m <= m < e_m)
            weekend = ts.weekday() >= 5
            if off_time or (prm["weekends"] and weekend):
                k = len(idx)
                hm = ts.strftime("%H:%M")
                raw.append(("off_hours", idx, {"weekday": ts.weekday(), "time": hm, "weekend": weekend, "outside_hours": off_time, "changes": k},
                            {"tr": f"{_WEEKDAYS['tr'][ts.weekday()]} saat {hm}, {_r12_num(k, 'tr')} değişiklik",
                             "en": f"{_WEEKDAYS['en'][ts.weekday()]} {hm}, {k:,} change{'s' if k != 1 else ''}"}, {}))
    # 12 bulk_edit (sliding window, linear)
    if on("bulk_edit"):
        prm = P("bulk_edit")
        groups = defaultdict(list)
        for i in timed:
            if who[i]:
                groups[who[i]].append(i)
        for idx in groups.values():
            seq = by_time(idx)
            cc, ncamp = Counter(), 0
            a = b = 0  # window is seq[a:b]; ncamp = distinct campaigns in it
            while a < len(seq):
                while b < len(seq) and ep[seq[b]] - ep[seq[a]] <= prm["minutes"] * 60:
                    k = (acct[seq[b]], camp[seq[b]])
                    cc[k] += 1
                    ncamp += cc[k] == 1 and bool(k[1])
                    b += 1
                if b - a >= prm["min_changes"] and ncamp >= prm["min_campaigns"]:
                    win = seq[a:b]
                    span = ep[win[-1]] - ep[win[0]]
                    raw.append(("bulk_edit", win, {"changes": len(win), "campaigns": ncamp, "minutes": round(span / 60, 1)},
                                {"tr": f"{_r12_span(span, 'tr')} içinde {_r12_num(ncamp, 'tr')} kampanyada {_r12_num(len(win), 'tr')} değişiklik",
                                 "en": f"{len(win):,} changes across {ncamp:,} campaigns within {_r12_span(span, 'en')}"}, {"campaign": None, "ad_group": None}))
                    cc.clear()
                    ncamp = 0
                    a = b
                else:
                    k = (acct[seq[a]], camp[seq[a]])
                    cc[k] -= 1
                    if cc[k] <= 0:
                        del cc[k]
                        ncamp -= bool(k[1])
                    a += 1
    return _r12_finish_flags(rows, raw, ep, cfg, acct, camp), extra


def _r12_metrics_active(metrics_files):
    """Entities with impressions, clicks or spend in the last 14 days of the metrics period:
    ({(campaign_key,): ent}, {(campaign_key, ad_group_key): ent}, end date); ent = {accs, account, campaign, ad_group}."""
    days = [m["day"] for mf in metrics_files for m in mf["rows"]]
    if not days:
        return {}, {}, None
    end = max(days)
    c_tab, g_tab = {}, {}
    for mf in metrics_files:
        for m in mf["rows"]:
            if (end - m["day"]).days >= 14 or not any((m["values"].get(f) or 0) > 0 for f in ("impressions", "clicks", "cost")):
                continue
            for tab, key, ag in ((c_tab, (_entity_key(m["campaign"]),), None), (g_tab, (_entity_key(m["campaign"]), _entity_key(m["ad_group"] or "")), m.get("ad_group"))):
                if tab is g_tab and not ag:
                    continue
                ent = tab.setdefault(key, {"accs": set(), "account": m["account"], "campaign": m["campaign"], "ad_group": ag})
                ent["accs"].add(_entity_key(m["account"]) if m["account"] else None)
    return c_tab, g_tab, end


def _r12_finish_flags(rows, raw, ep, cfg, acct, camp):
    order = {name: k for k, (name, _, _) in enumerate(RULE_DEFS)}
    disp = {}

    def labels(i):
        v = disp.get(i)
        if v is None:
            r = rows[i]
            v = disp[i] = (r.get("account_name") or (f"Account {r['account_id']}" if r.get("account_id") else None),
                           r.get("campaign_name") or (f"Campaign {r['campaign_id']}" if r.get("campaign_id") else None),
                           r.get("ad_group_name") or (f"Ad group {r['ad_group_id']}" if r.get("ad_group_id") else None), _r12_actor(r))
        return v

    def one(vals):
        vals = [v for v in dict.fromkeys(vals) if v]
        return vals[0] if len(vals) == 1 else (", ".join(vals) if vals else None)
    flags = []
    for rule, idx, fact, text, ov in raw:
        if len(idx) > 1:
            idx = list(dict.fromkeys(idx))
            tsorted = sorted(idx, key=lambda i: (ep[i] if ep[i] is not None else 0, i))
            lab = list(zip(*(labels(i) for i in idx)))
            a_, c_, g_, u_ = (one(x) for x in lab)
        else:
            tsorted = idx
            a_, c_, g_, u_ = labels(idx[0]) if idx else (None, None, None, None)
        f = {
            "rule": rule, "tier": cfg[rule]["tier"],
            "change_ids": [rows[i].get("change_id") for i in idx],
            "event_ids": list(dict.fromkeys(rows[i].get("event_id") for i in idx if rows[i].get("event_id"))),
            "account": a_, "campaign": c_, "ad_group": g_, "user": u_,
            "ts_first": rows[tsorted[0]].get("timestamp_iso") if idx else None, "ts_last": rows[tsorted[-1]].get("timestamp_iso") if idx else None,
            "fact": fact, "text": text,
            "_ck": {(acct[i], camp[i]) for i in idx if camp[i]},
        }
        f.update(ov)
        flags.append(f)
    flags.sort(key=lambda f: (f["tier"] != "attention", -_r12_sort_ts(f["ts_first"]), order[f["rule"]], str(f.get("campaign") or "")))
    return [dict({"id": f"F{k}"}, **f) for k, f in enumerate(flags, 1)]


def _r12_sort_ts(ts):
    try:
        return datetime.fromisoformat(str(ts)[:19]).replace(tzinfo=dt_timezone.utc).timestamp() if ts else float("-inf")
    except ValueError:
        return float("-inf")


def build_rules_config(cfg, flags, extra=None):
    """The rules_config list for change_history.json / DASH_DATA, in display order."""
    hits = Counter(f["rule"] for f in flags)
    out = []
    for name, tier, _ in RULE_DEFS:
        c = cfg[name]
        p = c["params"]
        off = c["off"]
        if not off and name == "off_hours" and (extra or {}).get("off_hours_no_timezone"):
            off = "no_timezone"
        fill = {lg: {k: _r12_param(v, lg) for k, v in p.items()} for lg in ("tr", "en")}
        if name == "off_hours":
            fill["tr"]["weekend_tr"] = " ya da hafta sonu" if p["weekends"] else ""
            fill["en"]["weekend_en"] = " or at the weekend" if p["weekends"] else ""
        title, desc = RULE_TEXT[name]
        out.append({"rule": name, "tier": tier, "enabled": not off, "params": dict(p),
                    "title": {lg: title[lg].format(**fill[lg]) for lg in ("tr", "en")},
                    "description": {lg: desc[lg].format(**fill[lg]) for lg in ("tr", "en")},
                    "hits": hits.get(name, 0), "disabled_reason": _RULE_OFF_REASON[off] if off else None})
    return out


def build_campaign_table(rows, flags):
    """One entry per campaign in the report (2026-11-19), most changes first."""
    camps = {}
    end_s = max((chrono_key(r) for r in rows if chrono_key(r)), default=None)
    end_d = datetime.fromisoformat(end_s[:19]).date() if end_s else None
    for r in rows:
        cid = campaign_identity(r)
        if not cid:
            continue
        ck = (account_identity(r), cid)
        e = camps.setdefault(ck, {"account": r.get("account_name") or (f"Account {r['account_id']}" if r.get("account_id") else None),
                                  "campaign": r.get("campaign_name") or f"Campaign {r.get('campaign_id')}", "changes": 0, "budget_changes": 0,
                                  "last_change": None, "days_since": None, "flags_by_rule": {}, "_last": ""})
        e["changes"] += 1
        e["budget_changes"] += r.get("subcategory") == "Budget changed"
        ck_ts = chrono_key(r)
        if ck_ts and ck_ts > e["_last"]:
            e["_last"], e["last_change"] = ck_ts, r.get("timestamp_iso")
            e["days_since"] = (end_d - datetime.fromisoformat(ck_ts[:19]).date()).days
    for f in flags:
        for ck in f["_ck"]:
            if ck in camps:
                fr = camps[ck]["flags_by_rule"]
                fr[f["rule"]] = fr.get(f["rule"], 0) + 1
    out = sorted(camps.items(), key=lambda kv: (-kv[1]["changes"], str(kv[1]["campaign"])))
    for _, e in out:
        del e["_last"]
    return [dict(e, _ck=ck) for ck, e in out]


def build_exec_summary(rows, flags, campaign_table):
    """Totals, people, budget movement, stale list, top campaigns and template sentences (2026-11-19)."""
    total = len(rows)
    loc = sorted((r.get("timestamp_iso") or "")[:10] for r in rows if r.get("timestamp_iso"))
    humans, autos, people = set(), set(), Counter()
    for r in rows:
        at = r.get("actor_type")
        label = _r12_actor(r)
        people[(label, at)] += 1
        if at == "human":
            humans.add(user_identity(r) or label)
        elif at == "automation":
            autos.add(label)
    att = sum(1 for f in flags if f["tier"] == "attention")
    deltas, curs = [], set()
    for r in rows:
        o, nw = r.get("old_value_num"), r.get("new_value_num")
        if r.get("subcategory") == "Budget changed" and isinstance(o, (int, float)) and isinstance(nw, (int, float)):
            deltas.append(nw - o)
            curs.add(_r12_cur(r))
    inc = [d for d in deltas if d > 0]
    dec = [d for d in deltas if d < 0]
    currency = next(iter(curs)) if len(curs) == 1 else None
    budget = {"currency": currency, "increase_total": round(sum(inc), 2), "increase_count": len(inc),
              "decrease_total": round(sum(dec), 2), "decrease_count": len(dec), "net": round(sum(deltas), 2), "mixed_currencies": len(curs) > 1}
    stale = sorted(({"account": f["account"], "campaign": f["campaign"], "days": f["fact"]["days"], "basis": f["fact"]["basis"]}
                    for f in flags if f["rule"] == "stale_entity" and f["fact"].get("level") == "campaign"),
                   key=lambda s: (s["days"] is None, -(s["days"] or 0)))
    att_by = Counter()
    for f in flags:
        if f["tier"] == "attention":
            for ck in f["_ck"]:
                att_by[ck] += 1
    top = [{"account": e["account"], "campaign": e["campaign"], "changes": e["changes"], "attention_flags": att_by.get(e["_ck"], 0)} for e in campaign_table[:10]]
    ev = len({r.get("event_id") for r in rows})
    head, sent = {}, {"tr": [], "en": []}
    if not total:
        head = {"tr": "Bu dönemde değişiklik kaydı yok.", "en": "No changes in this period."}
    else:
        nt = {"tr": _r12_num(total, "tr"), "en": f"{total:,}"}
        if att:
            head = {"tr": f"Bu dönemde {nt['tr']} değişiklik yapıldı, {_r12_num(att, 'tr')} durum göz atmayı hak ediyor.",
                    "en": f"{nt['en']} change{'s' if total != 1 else ''} in this period; {att:,} {'item is' if att == 1 else 'items are'} worth a look."}
        else:
            head = {"tr": f"Bu dönemde {nt['tr']} değişiklik yapıldı, hiçbiri kural eşiklerini aşmadı.",
                    "en": f"{nt['en']} change{'s' if total != 1 else ''} in this period; none crossed the rule thresholds."}
        # sentence 1: where (fewest campaigns holding more than half of the changes) and who
        acc, k = 0, 0
        for e in campaign_table:
            acc += e["changes"]
            k += 1
            if acc * 2 > total:
                break
        where = k if campaign_table and acc * 2 > total else None
        h, a = len(humans), len(autos)
        who_tr = (f"{_r12_num(h, 'tr')} kişi" if h else "") + (" ve " if h and a else "") + (f"{_r12_num(a, 'tr')} otomatik kaynak" if a else "")
        who_en = (f"{h:,} {'person' if h == 1 else 'people'}" if h else "") + (" and " if h and a else "") + (f"{a:,} automated source{'s' if a != 1 else ''}" if a else "")
        if where is not None:
            w_tr = "tek kampanyada" if where == 1 else f"{_r12_num(where, 'tr')} kampanyada"
            w_en = "one campaign" if where == 1 else f"{where:,} campaigns"
            sent["tr"].append(f"Değişikliklerin yarısından fazlası {w_tr} yapıldı" + (f", bu dönemde {who_tr} değişiklik yaptı." if who_tr else "."))
            sent["en"].append(f"More than half of the changes were in {w_en}" + (f", and {who_en} made changes in this period." if who_en else "."))
        elif who_tr:
            sent["tr"].append(f"Bu dönemde {who_tr} değişiklik yaptı.")
            sent["en"].append(f"{who_en[0].upper() + who_en[1:]} made changes in this period.")
        # sentence 2: budget movement
        if deltas and not budget["mixed_currencies"]:
            net = budget["net"]
            dec_n = 0 if float(net).is_integer() else 2
            if net > 0:
                sent["tr"].append(f"Bütçeler toplamda günlük {_r12_money(net, currency, 'tr', dec_n)} arttı.")
                sent["en"].append(f"Daily budgets went up by {_r12_money(net, currency, 'en', dec_n)} in total.")
            elif net < 0:
                sent["tr"].append(f"Bütçeler toplamda günlük {_r12_money(-net, currency, 'tr', dec_n)} azaldı.")
                sent["en"].append(f"Daily budgets went down by {_r12_money(-net, currency, 'en', dec_n)} in total.")
            else:
                sent["tr"].append("Bütçe artışları ve azalışları toplamda birbirini dengeledi.")
                sent["en"].append("Budget increases and decreases cancelled each other out.")
        # sentence 3: campaigns left untouched (export basis)
        st = [s for s in stale if s["basis"] == "export" and s["days"] is not None]
        if len(st) == 1:
            sent["tr"].append(f"Bir kampanyaya {_r12_num(st[0]['days'], 'tr')} gündür hiç dokunulmadı.")
            sent["en"].append(f"One campaign has had no change for {st[0]['days']:,} days.")
        elif st:
            lo = min(s["days"] for s in st)
            sent["tr"].append(f"{_r12_num(len(st), 'tr')} kampanyaya en az {_r12_num(lo, 'tr')} gündür dokunulmadı, en uzunu {_r12_num(st[0]['days'], 'tr')} gün.")
            sent["en"].append(f"{len(st):,} campaigns have had no change for at least {lo:,} days; the longest gap is {st[0]['days']:,} days.")
    return {
        "period": {"from": loc[0] if loc else None, "to": loc[-1] if loc else None},
        "totals": {"changes": total, "events": ev if total else 0, "campaigns_changed": len(campaign_table),
                   "ad_groups_changed": len({(account_identity(r), campaign_identity(r), ad_group_identity(r)) for r in rows if ad_group_identity(r)}),
                   "people": len(humans), "automation": len(autos), "attention_flags": att, "info_flags": len(flags) - att},
        "people": [{"label": lbl, "actor_type": at, "changes": c} for (lbl, at), c in sorted(people.items(), key=lambda kv: (-kv[1], str(kv[0][0])))],
        "budget": budget, "stale": stale, "top_campaigns": top, "headline": head, "sentences": sent,
    }


def build_rule_outputs(rows, cfg, known=None, metrics_files=None, ui_lang="en"):
    """Every round-12 field for change_history.json and DASH_DATA, from the final rows."""
    if known is None and not cfg["unknown_user"]["off"]:
        cfg["unknown_user"]["off"] = "no_known_users"
    flags, extra = compute_rule_flags(rows, cfg, known=known, metrics_files=metrics_files)
    table = build_campaign_table(rows, flags)
    summary = build_exec_summary(rows, flags, table)
    rcfg = build_rules_config(cfg, flags, extra)
    for f in flags:
        f.pop("_ck", None)
    return {"ui_lang": ui_lang, "rules_config": rcfg, "flags": flags, "exec_summary": summary,
            "campaign_table": [{k: v for k, v in e.items() if k != "_ck"} for e in table]}


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
    # 2026-11-19: rule flags grouped by rule, up to 3 example lines each; info rules one line each.
    rcfg = aggregation.get("rules_config") or []
    if rcfg and rows:
        by_rule = defaultdict(list)
        for f in aggregation.get("flags") or []:
            by_rule[f["rule"]].append(f)
        flat = lambda v: " ".join(str(v).split())
        stamp = lambda ts: (f"{ts[8:10]}.{ts[5:7]}.{ts[:4]} {ts[11:16]}" if lang == "tr" else f"{ts[:10]} {ts[11:16]}") if ts else None
        L += [f"## {t['worth']}", "", t["worth_note"], ""]
        att = [c for c in rcfg if c["tier"] == "attention" and c["enabled"] and by_rule.get(c["rule"])]
        if not att:
            L += [t["worth_none"], ""]
        for c in att:
            fl = by_rule[c["rule"]]
            L += [f"### {flat(c['title'][lang])} ({num(len(fl))})", ""]
            L += ["- " + " · ".join(flat(x) for x in (stamp(f.get("ts_first")), f.get("campaign"), f["text"][lang]) if x) for f in fl[:3]]
            L += ([f"- {t['worth_more'].format(n=num(len(fl) - 3))}"] if len(fl) > 3 else []) + [""]
        info = [c for c in rcfg if c["tier"] == "info"]
        if info:
            L += [f"**{t['worth_info']}**", ""]
            L += [f"- {flat(c['title'][lang])}: " + (num(c["hits"]) if c["enabled"] else f"{t['worth_off']}. {c['disabled_reason'][lang]}") for c in info] + [""]
        off = [c for c in rcfg if c["tier"] == "attention" and not c["enabled"]]
        if off:
            L += [f"**{t['worth_off_rules']}**", ""] + [f"- {flat(c['title'][lang])}: {c['disabled_reason'][lang]}" for c in off] + [""]
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
                  extra_rules=None, open_browser=False, generated_at=None, account_name=None, currency=None, column_map=None,
                  input_accounts=None, metrics=None, window_days=7, conversion_lag_days=7,
                  rules_config=None, known_users=None, work_hours=None, lang=None):
    # 2026-11-14: input_path may be a list of files (one run, several exports/accounts).
    inputs = [str(p) for p in input_path] if isinstance(input_path, (list, tuple)) else [str(input_path)]
    inputs, dup_inputs = _dedupe_inputs(inputs)
    multi = len(inputs) > 1
    # 2026-11-15 R-6: account names keyed by any spelling of the same file.
    _acc_res = {_resolved(k): v for k, v in (input_accounts or {}).items()}
    input_accounts = {p: _acc_res[_resolved(p)] for p in inputs + [str(m) for m in (metrics or [])] if _resolved(p) in _acc_res}
    metrics = [str(m) for m in (metrics or [])]
    names = _source_file_names(inputs)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    blocked = _prepare_out_dir(out_dir)
    if blocked:
        return blocked
    if window_days < 1 or conversion_lag_days < 0:
        return {"status": "error", "message": "--window-days must be at least 1 and --conversion-lag-days at least 0."}
    # 2026-11-19: rule thresholds (--rules-config/--work-hours) and the UI/digest language (--lang).
    lang = lang or DIGEST_LANG
    if lang not in ("auto", "tr", "en"):
        return {"status": "error", "message": f"--lang must be auto, tr or en, got {lang!r}."}
    rules_cfg, rules_err = resolve_rules_config(rules_config, work_hours)
    if rules_err:
        return {"status": "error", "message": rules_err}
    known_set = {str(k).strip().lower() for k in known_users if str(k).strip()} if known_users is not None else None
    if all(_is_empty_event_list(p) for p in inputs):
        dq = {"input_rows": 0, "rows_added_by_field_split": 0, "left_out": {"unreadable_date": 0, "duplicate": 0, **({"duplicate_across_files": 0} if multi else {})}, "rows_in_report": 0,
              "in_report_with_limits": {}, "ignored_headers": []}
        (out_dir / "changes.jsonl").write_text("", encoding="utf-8")
        (out_dir / "coverage.txt").write_text("Input rows: 0\nThe input is an empty event list: no change in the requested range.\n", encoding="utf-8")
        empty_infos = [{"file": names[p], "source_label": "empty_event_list", "is_api": True} for p in inputs]
        build_dashboard([], out_dir / "dashboard.html", mask_users=mask_users, generated_at=generated_at, json_out=out_dir / "change_history.json", data_quality=dq,
                        meta_extra={"sources": build_sources_meta([], empty_infos), "coverage_notes": build_coverage_notes(empty_infos, False)},
                        extra=build_rule_outputs([], rules_cfg, known=None if known_set is None else [], ui_lang=lang if lang != "auto" else "en"))
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

    # Each file is validated on its own; a stop status names the file (2026-11-14).
    validated, map_used = [], False
    for p in inputs:
        if multi and _is_empty_event_list(p):
            validated.append((p, None))
            continue
        fits = (not multi) or not (mapping_file or column_map) or _mapping_fits(p, mapping_file, column_map)
        map_used = map_used or (fits and bool(mapping_file or column_map))
        v = validate_source(p, mapping_file=mapping_file if fits else None, force=force_review,
                            default_account_name=input_accounts.get(p, account_name), column_map=column_map if fits else None)
        if v["status"] != "ok":
            v["file"] = names[p]
            return v
        validated.append((p, v))
    if multi and (mapping_file or column_map) and not map_used:
        return {"status": "error", "message": "--map/--mapping-file names column headers that none of the input files has."}
    profiles = [v.pop("_profile", None) for _, v in validated if v]
    resolved = validated[0][1] if not multi else {"files": [{"file": names[p], **v} for p, v in validated if v]}
    (out_dir / "resolved_mapping.json").write_text(json.dumps(resolved, ensure_ascii=False), encoding="utf-8")

    SUM_KEYS = ("input_rows", "rows_added_by_field_split", "rows_added_by_statement_split", "possible_duplicate_rows",
                "dst_nonexistent_local_time", "dst_ambiguous_local_time", "orphan_names_dropped", "rows_skipped_unparseable_date")
    n = {k: 0 for k in SUM_KEYS}
    all_rows, duplicate_count, file_infos, file_results, ignored_headers = [], 0, [], [], []
    for p, v in validated:
        if v is None:
            file_infos.append({"file": names[p], "source_label": "empty_event_list", "is_api": True})
            continue
        acct = input_accounts.get(p, account_name)
        nf = normalize_changes(p, v["mapping"], v["source_label"], date_format=date_format, decimal_style=decimal_style, tz=tz, default_account_name=acct)
        if nf["status"] != "ok":
            nf["file"] = names[p]
            return nf
        input_total = len(nf["rows"]) + nf["rows_skipped_unparseable_date"]
        date_skip_pct = round(100 * nf["rows_skipped_unparseable_date"] / input_total, 1) if input_total else 0.0
        if date_skip_pct > 20 and not force_review:
            return {
                "status": "needs_date_review", "file": names[p],
                "message": f"{nf['rows_skipped_unparseable_date']} of {input_total} rows ({date_skip_pct}%) could not be parsed as dates under the resolved format ({nf['date_format_used']}). This is high enough that the date column or --date-format is probably wrong, not just a few bad rows.",
                "rows_skipped_unparseable_date": nf["rows_skipped_unparseable_date"],
                "rows_skipped_unparseable_date_pct": date_skip_pct,
                "date_format_used": nf["date_format_used"],
                "instructions": "Confirm with the user whether this rate is genuinely expected (a source with a lot of malformed dates) or the wrong --date-format was resolved/passed. If it's real, re-run with --force-review.",
            }
        rows_f, dup_f = dedupe_rows(nf["rows"])
        for r in rows_f:
            r["source_file"] = names[p]
        all_rows.extend(rows_f)
        duplicate_count += dup_f
        for k in SUM_KEYS:
            n[k] += nf.get(k, 0)
        n.setdefault("date_format_used", nf["date_format_used"])
        n.setdefault("decimal_style_used", nf["decimal_style_used"])
        ignored_headers.extend(h for h in v["ignored_headers"] if h not in ignored_headers)
        file_infos.append({"file": names[p], "source_label": v["source_label"], "is_api": _is_api_source(v["source_label"], p)})
        file_results.append({"file": names[p], "source_label": v["source_label"], "date_format_used": nf["date_format_used"], "decimal_style_used": nf["decimal_style_used"]})
    n["rows"], duplicate_across_files = dedupe_across_files(all_rows)

    metrics_files = []
    for mp in metrics:
        mf = load_metrics_file(mp, account=input_accounts.get(mp, account_name), date_format=date_format, decimal_style=decimal_style)
        if mf["status"] != "ok":
            return mf
        metrics_files.append(mf)


    c = categorize_changes(n["rows"], allow_unknown=allow_unknown_categories, extra_rules=extra_rules)
    if c["status"] != "ok":
        (out_dir / "unknown-fields.json").write_text(json.dumps(c, indent=2, ensure_ascii=False), encoding="utf-8")
        return c

    # Fixed 2026-08-18 (audit F-04): masked BEFORE any output file is written,
    # so changes.jsonl and the dashboard are never inconsistent and neither
    # can leak an identity --mask-users was asked to hide.
    if currency:
        apply_currency(c["rows"], currency)
    # 2026-11-19: --known-users is matched on the real identity, before masking (row order is kept).
    known_flags = None if known_set is None else [r.get("actor_type") != "human" or bool({str(r.get("user_email") or "").strip().lower(), str(r.get("user_name") or "").strip().lower()} & known_set)
                                                  for r in c["rows"]]
    c["rows"] = mask_rows(c["rows"], mask_users)

    # Fixed 2026-10-10 (6th audit round): a naive timestamp with no
    # --timezone can't be placed on the UTC timeline, so the row is left out
    # of it (correctly — no zone is guessed). But the only trace of that was
    # a footnote under the timeline chart: on a 25k-row export 17.7% of rows
    # were missing from it while this function's own summary said nothing.
    tz_unknown = sum(1 for row in c["rows"] if not row.get("timestamp_utc"))

    sources_meta = build_sources_meta(c["rows"], file_infos)
    before_after = metrics_meta = ba_dq = None
    if metrics_files:
        before_after, ba_dq = compute_before_after(c["rows"], metrics_files, window_days, conversion_lag_days)
        metrics_meta = build_metrics_meta(metrics_files, window_days, conversion_lag_days)

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
        "left_out": {"unreadable_date": n["rows_skipped_unparseable_date"], "duplicate": duplicate_count, **({"duplicate_across_files": duplicate_across_files} if multi else {})},
        "rows_in_report": len(c["rows"]),
        "in_report_with_limits": {
            "category_other": sum(1 for row in c["rows"] if row.get("category") == "Other"),
            "timezone_unknown": tz_unknown,
            "operation_inferred": sum(1 for row in c["rows"] if row.get("operation_confidence") == "inferred"),
            "value_parsed_from_summary": sum(1 for row in c["rows"] if row.get("value_confidence") == "parsed_from_summary"),
            "multi_field_unsplit": sum(1 for row in c["rows"] if row.get("multi_field")),
            "name_without_reference_dropped": n["orphan_names_dropped"],
        },
        "ignored_headers": ignored_headers,
    }
    data_quality.update(semantic_data_quality(n, c["rows"], c["other_pct"]))
    if ba_dq is not None:
        data_quality["before_after"] = ba_dq
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
        f.write(f"Left out: {sum(dq_left.values())} (unreadable date: {dq_left['unreadable_date']}, duplicate: {dq_left['duplicate']}"
                + (f", duplicate across files: {dq_left['duplicate_across_files']})\n" if multi else ")\n"))
        if multi:
            for s_ in sources_meta:
                f.write(f"Source file {s_['file']}: {s_['rows']} rows in the report ({s_['source_label']}, {s_['date_min']} to {s_['date_max']})\n")
        if "before_after" in data_quality:
            ba = data_quality["before_after"]
            f.write(f"Before/after windows: {ba['rows_with_window']} rows with a window, {ba['rows_without_metrics']} without metrics, {ba['entities_unmatched']} campaign(s) not in the metrics files\n")
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

    source_warnings = [w for _, v in validated if v for w in v.get("warnings", [])] + [w for pr in profiles for w in save_mapping_profile(pr)]
    source_warnings += [f"{Path(d).name} was given more than once; it was read once." for d in dup_inputs]
    ui_lang = _digest_lang(c["rows"], {"sources": sources_meta}, lang)
    rule_out = build_rule_outputs(c["rows"], rules_cfg, known=known_flags, metrics_files=metrics_files, ui_lang=ui_lang)
    data = build_dashboard(c["rows"], out_dir / "dashboard.html", mask_users=mask_users, generated_at=generated_at,
                            json_out=out_dir / "change_history.json", rows_skipped_unparseable_date=n["rows_skipped_unparseable_date"],
                            data_quality=data_quality, meta_extra={"sources": sources_meta, "coverage_notes": build_coverage_notes(file_infos, bool(metrics_files))},
                            extra=dict(rule_out, **({"metrics_meta": metrics_meta, "before_after": before_after,
                                                     "metrics_daily": compute_metrics_daily(metrics_files)} if metrics_files else {})))
    write_digest(out_dir, c["rows"], data, data["meta"], ui_lang)

    result = {
        "status": "ok", "out": str(out_dir / "dashboard.html"), "row_count": len(c["rows"]),
        "other_pct": c["other_pct"], "accounts": len(data["accounts"]),
        "human_users": len(data["users"]["human"]), "automation_actors": len(data["users"]["automation"]),
        "rows_skipped_unparseable_date": n["rows_skipped_unparseable_date"],
        "rows_timezone_unknown": tz_unknown,
        "orphan_names_dropped": n["orphan_names_dropped"],
        "duplicates_removed": duplicate_count,
        "date_format_used": n["date_format_used"], "decimal_style_used": n["decimal_style_used"],
        "ignored_headers": ignored_headers,
        "data_quality": data_quality,
        "flags": {"attention": rule_out["exec_summary"]["totals"]["attention_flags"], "info": rule_out["exec_summary"]["totals"]["info_flags"]},
        "ui_lang": ui_lang,
    }
    if multi:
        result["files"] = file_results
        result["duplicate_across_files"] = duplicate_across_files
        if len({fr["date_format_used"] for fr in file_results}) > 1:
            result["date_format_used"] = "mixed"
    if metrics_files:
        result["metrics"] = {"files": [{k: mf[k] for k in ("file", "metrics", "date_format_used", "decimal_style_used", "rows_skipped_unparseable_date")} for mf in metrics_files],
                             **data_quality["before_after"]}
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


# Round 10 dashboard checks (2026-11-14): day filter, before/after, coverage notes, files, rollup.
_JS_R10_DASHBOARD_SUITE = r"""
const assert = require('assert');
let passed = 0, failures = 0;
function check(desc, fn){ try { fn(); passed++; } catch(e){ failures++; console.error('FAIL: ' + desc + '\n  ' + e.message); } }
function row(over){ return Object.assign({ subcategory:null, resource_type:null, operation:null, old_value_num:null, new_value_num:null, old_value:null, new_value:null, raw_summary:'' }, over); }
function ids(){ return getFiltered().map(c=>c.change_id); }
const BASE = [
  row({change_id:'a', account_name:'A', campaign_name:'C1', actor_type:'human', user_name:'U1', source_file:'f1.csv', timestamp_utc:'2026-08-01T21:30:00+00:00', timestamp_iso:'2026-08-02T00:30:00'}),
  row({change_id:'b', account_name:'A', campaign_name:'C2', actor_type:'automation', client_type:'GOOGLE_ADS_SCRIPTS', source_file:'f2.csv', timestamp_utc:'2026-08-02T08:00:00+00:00', timestamp_iso:'2026-08-02T11:00:00'}),
  row({change_id:'c', account_name:'B', campaign_name:'C1', actor_type:'human', user_name:'U2', source_file:'f2.csv', timestamp_utc:'2026-08-05T08:00:00+00:00', timestamp_iso:'2026-08-05T11:00:00'}),
  row({change_id:'d', account_name:'B', campaign_name:'C3', actor_type:'unknown', timestamp_iso:'2026-08-05T11:00:00'}),
];
function load(extra){ DASH_DATA = Object.assign({ changes: BASE.map(r=>Object.assign({}, r)), meta:{} }, extra||{}); resetAll(); }
check('day key is the local day (or the UTC day when the run had no zone); no zone means no day', ()=>{
  load(); assert.strictEqual(dayKey(DASH_DATA.changes[0]), '2026-08-02'); assert.strictEqual(dayKey(DASH_DATA.changes[3]), null);
  DASH_DATA.meta.timeline_reference_tz = 'UTC'; assert.strictEqual(dayKey(DASH_DATA.changes[0]), '2026-08-01');
});
check('state.day filters to that day; ignoreDay keeps the timeline whole; Clear filters drops it', ()=>{
  load(); state.day = '2026-08-02';
  assert.deepStrictEqual(ids(), ['a','b']); assert.strictEqual(getFiltered({ignoreDay:true}).length, 4);
  resetAll(); assert.strictEqual(state.day, ''); assert.strictEqual(ids().length, 4);
});
check('source-file filter, rows with no file are their own group', ()=>{
  load(); state.sourceFiles = new Set(['f2.csv']); assert.deepStrictEqual(ids(), ['b','c']);
  state.sourceFiles = new Set([NONE]); assert.deepStrictEqual(ids(), ['d']); resetAll();
});
check('before/after: plain difference and %, null stays "—", 0 before gives no %, maturing marked', ()=>{
  const ba = {before:{clicks:50, cost:0, conversions:null}, after:{clicks:60, cost:10, conversions:4, maturing_days:2}};
  assert.deepStrictEqual(baMetricRow(ba,'clicks'), {key:'clicks', before:50, after:60, diff:10, pct:20});
  assert.strictEqual(baMetricRow(ba,'cost').pct, null);
  assert.strictEqual(baMetricRow(ba,'conversions').diff, null);
  assert.strictEqual(baCellText(ba,'clicks'), '50 | 60* | +10 | +20%');
  assert.strictEqual(baCellText({before:{clicks:60}, after:{clicks:45}},'clicks'), '60 | 45 | -15 | -25%');
  assert.strictEqual(baCellText(null,'clicks'), '—');
});
check('detail strip: entity, table, overlapping changes as links, missing ids as text; nothing without metrics', ()=>{
  load({ before_after:{ a:{level:'campaign', entity:'C1', window_days:7, before:{days_with_data:7, clicks:5}, after:{days_with_data:3, clicks:8, maturing_days:1}, overlapping:{count:3, change_ids:['b','zzz']}} },
         metrics_meta:{metrics:['clicks','cost'], window_days:7} });
  const h = baDetailHtml(DASH_DATA.changes[0]);
  assert.ok(h.includes('Campaign: C1') && h.includes('the next 6 days') && h.includes('1 of them still maturing'), h);
  assert.ok(h.includes('data-open="b"') && h.includes('zzz (not in this report)') && h.includes('(first 2 listed)'), h);
  assert.strictEqual((h.match(/<tr><th scope="row">/g)||[]).length, 2);
  assert.ok(baDetailHtml(DASH_DATA.changes[1]).includes('No metrics rows matched'));
  load(); assert.strictEqual(baDetailHtml(DASH_DATA.changes[0]), ''); assert.strictEqual(baData(), null); assert.strictEqual(metricsNoteText(), '');
});
check('meta.sources as objects or labels; source_labels preferred; coverage notes tolerate junk', ()=>{
  assert.deepStrictEqual(sourceLabels({sources:['ui_export']}), ['ui_export']);
  assert.deepStrictEqual(sourceLabels({sources:[{file:'f1.csv', source_label:'x'}], source_labels:['y']}), ['y']);
  assert.strictEqual(sourceFilesMeta({sources:['ui_export']}).length, 0);
  DASH_DATA = {changes:[], meta:{coverage_notes:[{id:'a', text:'One.'}, null, {id:'b'}, 'Two.']}};
  assert.deepStrictEqual(coverageNotes().map(n=>n.text), ['One.', 'Two.']);
  DASH_DATA.meta = {}; assert.deepStrictEqual(coverageNotes(), []);
});
check('per-account rollup counts', ()=>{
  load(); const r = accountRollup(DASH_DATA.changes);
  assert.deepStrictEqual(r.map(a=>[a.name, a.changes, a.campaigns, a.users, a.automation, a.files.join(',')]), [['A',2,2,1,1,'f1.csv,f2.csv'], ['B',2,2,1,0,'f2.csv']]);
  assert.strictEqual(r[0].first.change_id, 'a'); assert.strictEqual(r[0].last.change_id, 'b');
});
check('timeline axis: change days only, or every day in range with a metrics series (capped)', ()=>{
  assert.deepStrictEqual(timelineAxis({'2026-08-05':1, '2026-08-02':2}, null), ['2026-08-02','2026-08-05']);
  assert.deepStrictEqual(timelineAxis({'2026-08-03':1}, {'2026-08-01':{}, '2026-08-04':{}}), ['2026-08-01','2026-08-02','2026-08-03','2026-08-04']);
  assert.ok(timelineAxis({'2026-08-03':1}, {'2020-01-01':{}}).length <= 400);
  DASH_DATA = {changes:[], meta:{}, metrics_meta:{daily:[{date:'2026-08-01', clicks:3}]}};
  assert.deepStrictEqual(lineMetrics(metricsDaily()).map(x=>x[0]), ['clicks']);
  DASH_DATA = {changes:[], meta:{}}; assert.strictEqual(metricsDaily(), null);
});
if (failures > 0) { console.error(failures + ' of ' + (passed + failures) + ' round-10 dashboard JS assertions failed'); process.exit(1); }
console.log(passed + ' round-10 dashboard JS assertions passed');
"""


def _run_r10_dashboard_cases(td):
    """Round 10 (2026-11-14) dashboard: day filter, before/after, coverage notes, source files, rollup."""
    import shutil
    import subprocess
    with _test_case('r10 dashboard: neutral wording, labelled new controls, optional sections hidden by default'):
        t = DASHBOARD_TEMPLATE
        js = t.split("// ---- round 10 (2026-11-14)")[1].split("// RULE MATCHES")[0]
        for word in (r"\bbetter", r"\bworse", r"\bimprov", r"\bdeclin", r"\bgood\b", r"\bbad\b", r"\bpositive", r"\bnegative", r"\bverdict", r"--lab-good", r"--lab-warn"):
            assert not re.search(word, js, re.I), f"round-10 helpers must stay neutral: {word!r}"
        for el in ('id="coverageSection" aria-labelledby="hCoverage" hidden', 'id="rollupSection" aria-labelledby="hRollup" hidden',
                   'id="fldSourceFile" hidden', 'id="baCtl" hidden', 'id="tlMetricCtl" hidden', 'id="baTh" class="num" hidden'):
            assert el in t, el
        # 2026-11-15 B-1: display:flex on .filters/.fld must not override hidden.
        assert re.search(r"(?m)^\s*\[hidden\]\s*\{\s*display:\s*none\s*!important;?\s*\}", t), "global [hidden]{display:none !important} rule missing"
        assert 'class="tl-bar' in t and '<button type="button" class="tl-bar' in t and 'aria-pressed="${state.day===d}"' in t
        print("[PASS] r10 dashboard: neutral wording, labelled new controls, optional sections hidden by default")
    with _test_case('r10 dashboard: a run without round-10 fields still embeds and renders the same shape'):
        f = td / "r10_plain.csv"
        f.write_text('Date & time,User,Campaign,Ad group,Changes\n"Aug 16, 2026, 10:42:26 PM",user.a@example.test,Campaign A,,Budget changed\n', encoding="utf-8")
        r = run_pipeline(f, td / "r10_plain_out", account_name="Account A", tz="UTC", generated_at="2026-08-17T00:00:00")
        assert r["status"] == "ok", r
        html = (td / "r10_plain_out" / "dashboard.html").read_text(encoding="utf-8")
        assert "/*__DASH_DATA__*/{" in html and '"before_after"' not in html and '"metrics_meta"' not in html
        print("[PASS] r10 dashboard: a run without round-10 fields still embeds and renders the same shape")
    if not shutil.which("node"):
        print("[SKIP] r10 dashboard JS suite (Node not found on PATH)")
        return
    with _test_case('r10 dashboard JS suite (Node)'):
        body = re.search(r"<script>(.*?)</script>", DASHBOARD_TEMPLATE, re.S).group(1)
        body = body.replace("const DASH_DATA = /*__DASH_DATA__*/null;", "let DASH_DATA = null;", 1)
        harness = "global.document = { addEventListener: function(){} };\n" + body + "\n" + _JS_R10_DASHBOARD_SUITE
        proc = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=15)
        assert proc.returncode == 0, f"r10 dashboard JS suite failed:\n{proc.stdout}\n{proc.stderr}"
        print(f"[PASS] r10 dashboard JS suite (Node): {proc.stdout.strip()}")


# Round 12 UI (2026-11-19): three pages, i18n table, flags, deep links, fallbacks for old reports.
_JS_R12_UI_SUITE = r"""
const assert = require('assert');
let passed = 0, failures = 0;
function check(desc, fn){ try { fn(); passed++; } catch(e){ failures++; console.error('FAIL: ' + desc + '\n  ' + e.message); } }
function row(over){ return Object.assign({ subcategory:null, resource_type:null, operation:'UPDATE', old_value_num:null, new_value_num:null, old_value:null, new_value:null, raw_summary:'', category:'Other', source:'ui_export', actor_type:'human', user_name:'User A' }, over); }
const ROWS = [
  row({change_id:'c1', event_id:'e1', account_name:'Acc A', campaign_name:'Camp Alpha', subcategory:'Budget changed', category:'Budget', old_value_num:300, new_value_num:695, currency:'TRY', timestamp_iso:'2026-08-04T14:21:00', timestamp_utc:'2026-08-04T11:21:00+00:00'}),
  row({change_id:'c2', event_id:'e2', account_name:'Acc A', campaign_name:'Camp Alpha', subcategory:'Budget changed', category:'Budget', old_value_num:200, new_value_num:150, currency:'TRY', timestamp_iso:'2026-08-05T10:00:00', timestamp_utc:'2026-08-05T07:00:00+00:00'}),
  row({change_id:'c3', event_id:'e3', account_name:'Acc A', campaign_id:'77', campaign_name:'Camp Beta', subcategory:'Keyword added', category:'Keyword', user_name:'User B', timestamp_iso:'2026-06-01T09:00:00', timestamp_utc:'2026-06-01T06:00:00+00:00'}),
  row({change_id:'c4', event_id:'e4', account_name:'Acc A', campaign_name:'Camp Alpha', subcategory:'Paused', category:'Status', actor_type:'automation', user_name:null, client_type:'GOOGLE_ADS_SCRIPTS', timestamp_iso:'2026-08-06T21:30:00', timestamp_utc:'2026-08-06T18:30:00+00:00'}),
];
const RULES = [
  {rule:'budget_jump', tier:'attention', enabled:true, params:{pct:50}, title:{tr:'Bütçe tek seferde %50\'den fazla değişti', en:'Budget changed by more than 50% in one step'}, description:{tr:'Açıklama.', en:'Description.'}, hits:1, disabled_reason:null},
  {rule:'repeat_edit', tier:'attention', enabled:true, params:{minutes:10, min_count:2}, title:{tr:'Aynı ayar tekrar değişti', en:'Same setting changed again'}, description:{tr:'A.', en:'B.'}, hits:0, disabled_reason:null},
  {rule:'unknown_user', tier:'attention', enabled:false, params:{}, title:{tr:'Listede olmayan kişi', en:'Not on the list'}, description:{tr:'A.', en:'B.'}, hits:0, disabled_reason:{tr:'Liste verilmedi.', en:'No list given.'}},
  {rule:'stale_entity', tier:'info', enabled:true, params:{days:30}, title:{tr:'Uzun süredir dokunulmayan', en:'Untouched for long'}, description:{tr:'A.', en:'B.'}, hits:1, disabled_reason:null},
  {rule:'off_hours', tier:'info', enabled:true, params:{start:'20:00', end:'08:00', weekends:true}, title:{tr:'Mesai dışı', en:'Off hours'}, description:{tr:'A.', en:'B.'}, hits:1, disabled_reason:null},
];
const FLAGS = [
  {id:'F1', rule:'budget_jump', tier:'attention', change_ids:['c1'], event_ids:['e1'], account:'Acc A', campaign:'Camp Alpha', ad_group:null, user:'User A', ts_first:'2026-08-04T14:21:00', ts_last:'2026-08-04T14:21:00', fact:{old:300, new:695, pct:131.7}, text:{tr:'Bütçe ₺300,00 → ₺695,00 (+%132)', en:'Budget ₺300.00 → ₺695.00 (+132%)'}},
  {id:'F2', rule:'off_hours', tier:'info', change_ids:['c4'], event_ids:['e4'], account:'Acc A', campaign:'Camp Alpha', ad_group:null, user:'GOOGLE_ADS_SCRIPTS', ts_first:'2026-08-06T21:30:00', ts_last:'2026-08-06T21:30:00', fact:{count:1}, text:{tr:'1 değişiklik mesai dışında', en:'1 change outside working hours'}},
  {id:'F3', rule:'stale_entity', tier:'info', change_ids:[], event_ids:[], account:'Acc A', campaign:'Camp Beta', ad_group:null, user:null, ts_first:'2026-06-01T09:00:00', ts_last:'2026-06-01T09:00:00', fact:{days:66}, text:{tr:'66 gündür değişiklik yok', en:'No change for 66 days'}},
];
const ES = {period:{from:'2026-06-01T09:00:00', to:'2026-08-06T21:30:00'}, totals:{changes:4, events:4, campaigns_changed:2, ad_groups_changed:0, people:2, automation:1, attention_flags:1, info_flags:2},
  people:[{label:'User A', actor_type:'human', changes:2}, {label:'User B', actor_type:'human', changes:1}, {label:'GOOGLE_ADS_SCRIPTS', actor_type:'automation', changes:1}],
  budget:{currency:'TRY', increase_total:395, increase_count:1, decrease_total:-50, decrease_count:1, net:345}, stale:[{account:'Acc A', campaign:'Camp Beta', days:66, basis:'export'}],
  top_campaigns:[], headline:{tr:'Bu dönemde 4 değişiklik yapıldı, 1 tanesi göz atmayı hak ediyor.', en:'4 changes in this period; 1 is worth a look.'}, sentences:{tr:['Değişikliklerin 3 tanesi 1 kampanyada.'], en:['3 of the changes were in 1 campaign.']}};
function load(lang, opt){
  DASH_DATA = { changes: ROWS.map(r=>Object.assign({}, r)), meta:{row_count:4, event_count:4, generated_at:'2026-11-19T10:42:00', timeline_reference_tz:'Europe/Istanbul'} };
  if(lang) DASH_DATA.ui_lang = lang;
  if(!(opt && opt.old)) Object.assign(DASH_DATA, {rules_config: RULES, flags: FLAGS, exec_summary: ES});
  UI_LANG = null; UI_LOCALE = LANG()==='tr' ? 'tr-TR' : 'en-US'; resetAll();
  for(const k of Object.keys(optionLabels)) delete optionLabels[k];
}
function ids(){ return getFiltered().map(c=>c.change_id); }
function html(id){ return document.getElementById(id).innerHTML; }

check('T: tr and en carry the same keys, no em-dash, no judging words', ()=>{
  const a = Object.keys(T.en).sort(), b = Object.keys(T.tr).sort();
  assert.deepStrictEqual(a.filter(k=>!T.tr.hasOwnProperty(k)), []); assert.deepStrictEqual(b.filter(k=>!T.en.hasOwnProperty(k)), []);
  for(const L of ['en','tr']) for(const [k,v] of Object.entries(T[L])){
    assert.ok(!String(v).includes('—'), `${L}.${k} has an em-dash`);
    assert.ok(!/(?<![\w-])(hata|kötü|anomali|error|bad|wrong)(?![\w-])/i.test(v), `${L}.${k}: ${v}`);
    if(k!=='limits_3') assert.ok(!/risky|riskli/i.test(v), `${L}.${k}: ${v}`);
  }
});
check('T: every Turkish string that starts with a letter starts with a capital (TR-aware)', ()=>{
  const bad = Object.entries(T.tr).filter(([k,v])=> !/_lc$/.test(k) && /^\p{L}/u.test(v) && v[0]!==v[0].toLocaleUpperCase('tr'));
  assert.deepStrictEqual(bad, []);
});
check('T: every key the template uses exists; static markup text equals T.en', ()=>{
  assert.deepStrictEqual(USED_KEYS.filter(k=>T.en[k]===undefined), []);
  for(const [k, txt] of STATIC_TEXT) assert.strictEqual(T.en[k], txt, k);
});
check('T: Python categories, subcategories, coverage notes and contract rule ids are all translated', ()=>{
  for(const c of PY.categories) assert.ok(T.en['cat.'+c]!==undefined, 'cat.'+c);
  for(const s of PY.subcategories) assert.ok(T.en['sub.'+s]!==undefined, 'sub.'+s);
  for(const [id, text] of Object.entries(PY.coverage)) assert.strictEqual(T.en['cov.'+id], text, id);
  for(const r of ['budget_jump','budget_cumulative','reverted','repeat_edit','burst_session','bid_target_relearn','automation_source','unknown_user','stale_entity','stale_budget','off_hours','bulk_edit']) assert.ok(T.en['rs.'+r]!==undefined, r);
  load('tr'); assert.strictEqual(catName('Budget'), 'Bütçe'); assert.strictEqual(subName('Target CPA'), 'Hedef EBM'); assert.strictEqual(subName('Custom thing'), 'Custom thing');
  load('en'); assert.strictEqual(catName('AdGroup'), 'Ad group'); assert.strictEqual(subName('Target CPA'), 'Target CPA');
});
check('language: ui_lang wins; an old report falls back to its rows', ()=>{
  load('tr'); assert.strictEqual(pickLang(), 'tr'); load('en'); assert.strictEqual(pickLang(), 'en');
  load(null, {old:true}); assert.strictEqual(pickLang(), 'en');
  DASH_DATA.changes.forEach(c=>{ c.raw_summary = 'Kampanya değiştirildi'; }); assert.strictEqual(pickLang(), 'tr');
});
check('hash routing: TR and EN slugs, sections, params round-trip, unknown hash is page 1', ()=>{
  load('tr');
  assert.deepStrictEqual(parseHash('#tum?durum=F3'), {page:'p3', section:null, params:{flag:'F3'}});
  assert.deepStrictEqual(parseHash('#details/flags'), {page:'p2', section:'s2', params:{}});
  assert.deepStrictEqual(parseHash('#nope'), {page:'p1', section:null, params:{}});
  assert.deepStrictEqual(parseHash(''), {page:'p1', section:null, params:{}});
  const h = buildHash('p3', null, {account:'A & B', campaign:'Camp – Brand #2?'});
  assert.ok(h.startsWith('#tum?hesap='), h);
  assert.deepStrictEqual(parseHash(h).params, {account:'A & B', campaign:'Camp – Brand #2?'});
  assert.strictEqual(buildHash('p2', 's7'), '#detaylar/kapsam');
  load('en'); assert.strictEqual(buildHash('p2', 's7'), '#details/coverage'); assert.strictEqual(hrefAll({rule:'off_hours'}), '#all?rule=off_hours');
});
check('flag filters: by change ids, by rule, any flag; Clear filters drops them', ()=>{
  load('en');
  state.flagIds = new Set(['c1']); assert.deepStrictEqual(ids(), ['c1']);
  resetAll(); state.flagRule = 'off_hours'; assert.deepStrictEqual(ids(), ['c4']);
  state.flagRule = '__any__'; assert.deepStrictEqual(ids(), ['c1','c4']);
  resetAll(); assert.strictEqual(state.flagRule, ''); assert.strictEqual(state.flagIds, null); assert.strictEqual(ids().length, 4);
  assert.deepStrictEqual(flagsForChange(DASH_DATA.changes[0]).map(f=>f.id), ['F1']); assert.deepStrictEqual(flagsForChange({change_id:'zz'}), []);
});
check('deep links: flag, flag without changes, rule, campaign by name (id rows too), user, category', ()=>{
  load('tr');
  applyDeepLink({flag:'F1'}); assert.deepStrictEqual(ids(), ['c1']); assert.strictEqual(state.flagRef, 'F1');
  applyDeepLink({flag:'F3'}); assert.strictEqual(state.flagIds, null); assert.deepStrictEqual(ids(), ['c3']);
  applyDeepLink({rule:'budget_jump'}); assert.deepStrictEqual(ids(), ['c1']);
  applyDeepLink({account:'Acc A', campaign:'Camp Beta'}); assert.deepStrictEqual([...state.campaigns], ['77']); assert.deepStrictEqual(ids(), ['c3']);
  applyDeepLink({campaign:'No Such'}); assert.deepStrictEqual(ids(), []);
  applyDeepLink({user:'User B'}); assert.deepStrictEqual(ids(), ['c3']);
  applyDeepLink({category:'Budget'}); assert.deepStrictEqual(ids(), ['c1','c2']);
  state.ruleMatch = '__matched__'; ruleState.enabled = true; applyDeepLink({rule:'off_hours'}); assert.strictEqual(state.ruleMatch, ''); assert.strictEqual(ruleState.enabled, true); resetAll();
});
check('rule groups: attention first, config order, disabled and zero-hit rules left out', ()=>{
  load('en');
  assert.deepStrictEqual(ruleGroups().map(g=>[g.rule, g.tier, g.hits]), [['budget_jump','attention',1], ['stale_entity','info',1], ['off_hours','info',1]]);
  assert.strictEqual(ruleTitle('budget_jump'), 'Budget changed by more than 50% in one step');
  load('tr'); assert.strictEqual(ruleTitle('budget_jump'), 'Bütçe tek seferde %50\'den fazla değişti'); assert.strictEqual(ruleShort('nope_rule'), 'nope_rule');
  DASH_DATA.rules_config = null; assert.deepStrictEqual(ruleGroups().map(g=>g.rule), ['budget_jump','off_hours','stale_entity']);
});
check('thresholds read as facts in both languages', ()=>{
  load('tr'); assert.strictEqual(fmtParams({pct:50, hours:24}), '%50 · 24 saat'); assert.strictEqual(fmtParams({start:'20:00', end:'08:00', weekends:true}), '20:00–08:00 dışı · Hafta sonu dahil');
  load('en'); assert.strictEqual(fmtParams({pct:50, hours:24}), '50% · 24 h'); assert.strictEqual(fmtParams({minutes:10, min_count:2, future_x:3}), '10 min · 2+ edits · future_x: 3');
  assert.strictEqual(fmtParams(null), ''); assert.strictEqual(fmtParams({}), '');
});
check('fallbacks for old reports: budget sums, people, campaign table, stale list', ()=>{
  load('en', {old:true});
  assert.strictEqual(hasFlagData(), false); assert.strictEqual(execSummary(), null);
  assert.deepStrictEqual(computeBudget(DASH_DATA.changes), {currency:'TRY', increase_total:395, increase_count:1, decrease_total:-50, decrease_count:1, net:345});
  assert.deepStrictEqual(computePeople(DASH_DATA.changes).map(p=>[p.label, p.actor_type, p.changes]), [['User A','human',2], ['User B','human',1], ['GOOGLE_ADS_SCRIPTS (automation)','automation',1]]);
  const ct = campaignTableRows(); const beta = ct.find(r=>r.campaign==='Camp Beta'), alpha = ct.find(r=>r.campaign==='Camp Alpha');
  assert.deepStrictEqual([alpha.changes, alpha.budget_changes, alpha.days_since, beta.days_since], [3, 2, 0, 66]);
  assert.deepStrictEqual(staleList(), [{account:'Acc A', campaign:'Camp Beta', days:66, basis:'export'}]);
  load('en'); DASH_DATA.rules_config = [{rule:'stale_entity', params:{days:90}}]; DASH_DATA.exec_summary = null; assert.strictEqual(staleDays(), 90); assert.deepStrictEqual(staleList(), []);
});
check('formatters: short date, period, money, headline number, bold lede numbers', ()=>{
  load('tr');
  assert.strictEqual(fmtShort('2026-08-04T14:21:00'), '04.08 14:21'); assert.strictEqual(fmtPeriod('2026-01-26T10:00:00', '2026-08-11T02:33:00'), '26 Oca – 11 Ağu 2026');
  assert.strictEqual(fmtMoney(2050, 'TRY'), '₺2.050'); assert.strictEqual(fmtMoney(5, null), '5'); assert.strictEqual(fmtMoney(5, 'bad'), '5 bad');
  assert.strictEqual(emFirstNumber('Bu dönemde 1.187 değişiklik <x>'), 'Bu dönemde <em>1.187</em> değişiklik &lt;x&gt;');
  assert.strictEqual(boldNumbers('Bütçeler günlük ₺1.240 arttı. Bir kampanyaya 111 gündür'), 'Bütçeler günlük <b>₺1.240 arttı</b>. Bir kampanyaya <b>111 gündür</b>');
  load('en'); assert.strictEqual(fmtPeriod('2026-08-04T10:00:00', '2026-08-04T12:00:00'), 'Aug 4, 2026'); assert.strictEqual(fmtShort('bad'), 'bad');
});
check('page 1 renders from the new fields: headline, stats with the dark card, rule cards, people, budget, stale', ()=>{
  load('tr');
  renderHero(); renderStats(); renderFlagCards(); renderWho(); renderBudget(); renderStale();
  assert.ok(html('headline').includes('<em>4</em>')); assert.ok(html('lede').includes('<b>'));
  assert.ok(html('metaList').includes('İstanbul') && html('metaList').includes('1 Haz – 6 Ağu 2026'), html('metaList'));
  assert.ok(html('statCards').includes('stat focus') && html('statCards').includes('Kişi · 1 otomasyon'), html('statCards'));
  const fc = html('flagCards');
  assert.ok(fc.indexOf('budget_jump') < fc.indexOf('stale_entity') && fc.includes('href="#tum?kural=budget_jump"') && fc.includes('href="#tum?durum=F1"'), fc);
  // 2026-11-19: page 1 shows info rules as one compact line; the per-flag campaign link lives in flagHref (used on page 2).
  assert.ok(flagHref(flagList().find(f=>f.rule==='stale_entity')).includes('kampanya=Camp%20Beta'), 'a flag with no change ids links to its campaign');
  assert.ok(fc.includes('class="count info"') && fc.includes('04.08 14:21'), fc);
  assert.ok(html('whoBody').includes('Otomasyon ve Google') && html('whoBody').includes('role="img"'));
  const btxt = html('budgetBody').replace(/<[^>]+>/g, ''); assert.ok(btxt.includes('+₺395') && btxt.includes('−₺50'), btxt);
  assert.ok(html('staleBody').includes('66 gün'));
});
check('page 1 and 2 degrade on an old report: no dark card, a plain notice, tables from the rows', ()=>{
  load('en', {old:true});
  renderHero(); renderStats(); renderFlagCards(); renderBudget(); renderCampaignTable(); renderFlagGroups(); renderThresholds();
  assert.ok(html('headline').includes('<em>4</em> changes in this period.'), html('headline'));
  assert.ok(!html('statCards').includes('focus') && html('statCards').includes('Ad groups changed'));
  assert.ok(html('flagCards').includes('no rule results'));
  assert.ok(html('budgetBody').replace(/<[^>]+>/g, '').includes('+₺395'), html('budgetBody'));
  assert.ok(html('campTableWrap').includes('Camp Alpha') && html('campTableWrap').includes('66 days ago'), html('campTableWrap'));
  assert.ok(html('threshTable').includes('No rule settings'));
});
check('page 2: flag groups link every flag, thresholds table shows params, off rules with their reason', ()=>{
  load('en'); renderFlagGroups(); renderThresholds(); renderCampaignTable();
  const g = html('flagGroups');
  assert.ok(g.includes('href="#all?flag=F1"') && g.includes('Threshold: 50%') && g.includes('Budget ₺300.00 → ₺695.00'), g);
  const th = html('threshTable');
  assert.ok(th.includes('No list given.') && th.includes('Off') && th.includes('Outside 20:00–08:00 · Weekends'), th);
});
check('Explorer: flag column with rule tags, hidden without flag data; Turkish unknown pill', ()=>{
  load('en'); renderExplorer(getFiltered());
  let b = html('explorerBody');
  assert.ok(b.includes('Budget, one step') && b.includes('class="tag info"'), b.slice(0, 600));
  load('en', {old:true}); renderExplorer(getFiltered());
  assert.ok(!html('explorerBody').includes('class="tag'));
  load('tr'); DASH_DATA.changes[0].actor_type = 'unknown'; DASH_DATA.changes[0].user_name = null; renderExplorer(getFiltered());
  assert.ok(html('explorerBody').includes('<span class="pill">Bilinmiyor</span>'));
});
if (failures > 0) { console.error(failures + ' of ' + (passed + failures) + ' round-12 UI JS assertions failed'); process.exit(1); }
console.log(passed + ' round-12 UI JS assertions passed');
"""


def _r12_ui_harness():
    """Template script + a stub DOM + the keys, static texts and Python names the suite cross-checks."""
    import html as _html
    t = DASHBOARD_TEMPLATE
    body = re.search(r"<script>(.*?)</script>", t, re.S).group(1).replace("const DASH_DATA = /*__DASH_DATA__*/null;", "let DASH_DATA = null;", 1)
    used = set(re.findall(r"\btn?\('([^']+)'(?!\s*\+)", body)) | set(re.findall(r'data-t="([^"]+)"', t))
    for spec in re.findall(r'data-ta="([^"]+)"', t):
        used |= {p.split(":", 1)[1] for p in spec.split(";") if ":" in p}
    static = [(k, _html.unescape(v)) for k, v in re.findall(r'data-t="([^"]+)">([^<]*)<', t)]
    cats, subs = set(), set()

    def walk(o):
        if isinstance(o, dict):
            if isinstance(o.get("category"), str):
                cats.add(o["category"])
            if isinstance(o.get("subcategory"), str):
                subs.add(o["subcategory"])
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(CATEGORY_RULES)
    py = {"categories": sorted(cats), "subcategories": sorted(subs), "coverage": COVERAGE_NOTE_TEXT}
    stub = ("const __els = {};\nfunction __el(){ return {hidden:false, innerHTML:'', textContent:'', disabled:false, dataset:{}, style:{}, "
            "classList:{add(){}, remove(){}, toggle(){}, contains(){ return false; }}, setAttribute(){}, removeAttribute(){}, getAttribute(){ return null; }, "
            "addEventListener(){}, querySelectorAll(){ return []; }, querySelector(){ return null; }, appendChild(){}}; }\n"
            "global.document = { addEventListener(){}, getElementById(id){ return __els[id] || (__els[id] = __el()); }, "
            "querySelectorAll(){ return []; }, querySelector(){ return null; }, createElement(){ return __el(); }, body: __el() };\n")
    return (stub + body + "\nconst USED_KEYS = " + json.dumps(sorted(used)) + ";\nconst STATIC_TEXT = " + json.dumps(static, ensure_ascii=False)
            + ";\nconst PY = " + json.dumps(py, ensure_ascii=False) + ";\n" + _JS_R12_UI_SUITE)


def _run_r12_ui_cases(td):
    """2026-11-19 round 12 UI: offline three-page dashboard, embedded fonts, new data fields, Node suite."""
    import shutil
    import subprocess
    with _test_case("r12 UI: one offline file, three pages, embedded fonts, no network reference"):
        t = DASHBOARD_TEMPLATE
        assert all(f'id="{p}"' in t for p in ("p1", "p2", "p3")) and t.count('class="tab"') == 3, "three pages behind the pill nav"
        assert t.count("@font-face") == 5 and t.count("url(data:font/woff2;base64,") == 5 and "OFL-1.1" in t, "Literata + Inter embedded as woff2"
        for bad in ("<link", "@import", "fetch(", "XMLHttpRequest", "src=\"http", "url(http", "href=\"http"):
            assert bad not in t, f"network reference in the template: {bad}"
        assert re.findall(r"https?://[^\s\"')]+", t) == ["http://www.w3.org/2000/svg"], "only the SVG namespace may name a URL"
        print("[PASS] r12 UI: one offline file, three pages, embedded fonts, no network reference")
    with _test_case("r12 UI: the new fields reach DASH_DATA and the page size stays in proportion"):
        f = _r9_csv(td, "r12_ui.csv", [["2026-08-04 14:21:00", "u1@example.test", "Camp A", "", "Kampanya değiştirildi\n  ₺300,00 olan Bütçe tutarı, ₺695,00 değerine artırıldı"],
                                       ["2026-08-05 21:30:00", "u2@example.test", "Camp B", "", "1 dil eklendi\n  Dil A"]])
        r = run_pipeline(f, td / "out_r12_ui", account_name="Acc", tz="Europe/Istanbul", generated_at="2026-11-19T00:00:00")
        assert r["status"] == "ok", r
        rows = _r9_rows(td / "out_r12_ui")
        extra = {"ui_lang": "tr", "flags": [{"id": "F1", "rule": "budget_jump", "tier": "attention", "change_ids": [rows[0]["change_id"]], "event_ids": [rows[0]["event_id"]],
                                             "account": "Acc", "campaign": "Camp A", "ad_group": None, "user": "u1@example.test", "ts_first": rows[0]["timestamp_iso"],
                                             "ts_last": rows[0]["timestamp_iso"], "fact": {"pct": 131.7}, "text": {"tr": "Bütçe ₺300,00 → ₺695,00", "en": "Budget ₺300.00 → ₺695.00"}}],
                 "rules_config": [], "exec_summary": {"headline": {"tr": "Bu dönemde 2 değişiklik yapıldı.", "en": "2 changes."}}, "campaign_table": []}
        out = td / "out_r12_ui" / "dashboard_extra.html"
        build_dashboard(rows, out, generated_at="2026-11-19T00:00:00", extra=extra)
        html = out.read_text(encoding="utf-8")
        payload = json.JSONDecoder().raw_decode(html.split("/*__DASH_DATA__*/", 1)[1])[0]
        assert all(payload[k] == v for k, v in extra.items()), "every new field is embedded as given"
        assert len(html.encode()) < len(DASHBOARD_TEMPLATE.encode()) + 20000, "two rows must not cost more than the template plus a little"
        print("[PASS] r12 UI: the new fields reach DASH_DATA and the page size stays in proportion")
    if not shutil.which("node"):
        print("[SKIP] r12 UI JS suite (Node not found on PATH)")
        return
    with _test_case("r12 UI JS suite (Node): i18n table, routing, flag filters, deep links, fallbacks, renders"):
        proc = subprocess.run(["node", "-e", _r12_ui_harness()], capture_output=True, text=True, timeout=30)
        assert proc.returncode == 0, f"r12 UI JS suite failed:\n{proc.stdout}\n{proc.stderr}"
        print(f"[PASS] r12 UI JS suite (Node): {proc.stdout.strip()}")


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


def _run_r10_pipeline_cases(td):
    """2026-11-14: multiple inputs, coverage notes, before/after metrics (synthetic names only)."""
    a1 = ["2026-10-10 10:00:00", "u1@example.test", "Camp Alpha", "", "1 dil eklendi\n  Dil A"]
    a2 = ["2026-10-12 09:00:00", "u1@example.test", "Camp Beta", "Grup Bir", "1 ülke eklendi\n  Ülke A"]
    b1 = ["2026-10-20 08:00:00", "u2@example.test", "Camp Gamma", "", "1 dil eklendi\n  Dil B"]
    fa = _r9_csv(td, "r10_a.csv", [a1, a1, a2])
    fb = _r9_csv(td, "r10_b.csv", [a1, b1])
    tr_lines = ["Kampanya performansı", "5 Eki 2026 - 14 Eki 2026", "Gün;Kampanya;Gösterim;Tıklama;Maliyet;Dönüşümler;Kampanya durumu"]
    for d in range(5, 15):
        tr_lines.append(f"{d} Eki 2026;Camp Alpha;1.000;{d};12,50;{'--' if d == 8 else '1'};Etkin")
    tr_lines.append("Toplam: Hesap;--;10.000;95;125,00;9;--")
    fm_tr = td / "r10_metrics_tr.csv"
    fm_tr.write_text("\n".join(tr_lines) + "\n", encoding="utf-8")
    en_lines = ["Ad group report", "October 9, 2026 - October 14, 2026", "Campaign,Ad group,Account,Day,Impr.,Clicks,Cost,Conv. value,Labels"]
    for d in range(9, 14):
        en_lines.append(f'CAMP  BETA,GRUP BİR,ACC ONE,2026-10-{d:02d},"1,200",2,3.25,10.00,x')
    en_lines.append('Total: Ad groups,--,--,--,"6,000",10,16.25,50.00,--')
    fm_en = td / "r10_metrics_en.csv"
    fm_en.write_text("\n".join(en_lines) + "\n", encoding="utf-8")
    accts = {str(fa): "Acc One", str(fb): "Acc One"}

    with _test_case("r10 multi-input: rows from two files merge, an overlap is kept once and counted, each row names its file"):
        out = td / "out_r10_multi"
        r = run_pipeline([fa, fb], out, input_accounts=accts, tz="Europe/Istanbul", generated_at="2026-11-14T00:00:00",
                         metrics=[fm_tr, fm_en], window_days=3, conversion_lag_days=3)
        assert r["status"] == "ok", f"multi run failed: {r}"
        dq = r["data_quality"]
        assert r["row_count"] == 4 and dq["left_out"] == {"unreadable_date": 0, "duplicate": 0, "duplicate_across_files": 1}, f"dq: {dq}"
        assert dq["input_rows"] + dq["rows_added_by_field_split"] + dq["rows_added_by_statement_split"] - sum(dq["left_out"].values()) == dq["rows_in_report"], "counts must add up"
        rows = _r9_rows(out)
        assert [x["source_file"] for x in rows] == ["r10_a.csv"] * 3 + ["r10_b.csv"], [x["source_file"] for x in rows]
        assert len({x["change_id"] for x in rows}) == 4, "change_id stays unique"
        data = json.loads((out / "change_history.json").read_text(encoding="utf-8"))
        src = data["meta"]["sources"]
        assert [(s["file"], s["rows"], s["accounts"], s["date_min"], s["date_max"]) for s in src] == [
            ("r10_a.csv", 3, ["Acc One"], "2026-10-10", "2026-10-12"), ("r10_b.csv", 1, ["Acc One"], "2026-10-20", "2026-10-20")], src
        assert [n["id"] for n in data["meta"]["coverage_notes"]] == ["auto_applied_may_be_missing", "metrics_conversion_lag"], data["meta"]["coverage_notes"]
        print("[PASS] r10 multi-input: two files merge, overlap kept once and counted, source_file + meta.sources per file")

    with _test_case("r10 before/after: TR and EN metrics files (preamble, total row) give hand-checked window sums, missing metrics stay null"):
        ba = data["before_after"]
        alpha = ba[rows[0]["change_id"]]
        assert alpha["level"] == "campaign" and alpha["window_days"] == 3, alpha
        assert alpha["before"] == {"days_with_data": 3, "impressions": 3000.0, "clicks": 24.0, "cost": 37.5, "conversions": 2.0, "conversion_value": None}, alpha["before"]
        assert alpha["after"] == {"days_with_data": 3, "impressions": 3000.0, "clicks": 33.0, "cost": 37.5, "conversions": 3.0, "conversion_value": None, "maturing_days": 1}, alpha["after"]
        assert alpha["overlapping"] == {"count": 1, "change_ids": [rows[1]["change_id"]]}, alpha["overlapping"]
        beta = ba[rows[2]["change_id"]]
        assert beta["level"] == "ad_group" and beta["entity"] == "Camp Beta / Grup Bir", beta
        assert beta["before"] == {"days_with_data": 3, "impressions": 3600.0, "clicks": 6.0, "cost": 9.75, "conversions": None, "conversion_value": 30.0}, beta["before"]
        assert beta["after"] == {"days_with_data": 2, "impressions": 2400.0, "clicks": 4.0, "cost": 6.5, "conversions": None, "conversion_value": 20.0, "maturing_days": 2}, beta["after"]
        assert rows[3]["change_id"] not in ba and data["meta"]["data_quality"]["before_after"] == {"rows_with_window": 3, "rows_without_metrics": 1, "entities_unmatched": 1}
        mm = data["metrics_meta"]
        assert mm["levels"] == ["campaign", "ad_group"] and (mm["date_min"], mm["date_max"]) == ("2026-10-05", "2026-10-14"), mm
        assert mm["metrics"] == list(METRIC_FIELDS) and mm["ignored_columns"] == ["Kampanya durumu", "Labels"], mm
        html = (out / "dashboard.html").read_text(encoding="utf-8")
        payload = json.JSONDecoder().raw_decode(html.split("/*__DASH_DATA__*/", 1)[1])[0]
        assert payload["before_after"] == ba and payload["meta"]["sources"] == src and payload["metrics_meta"] == mm, "DASH_DATA carries the new fields"
        print("[PASS] r10 before/after: TR + EN metrics, preamble and total rows skipped, sums match hand count, missing = null")

    with _test_case("r10 multi-input: a stop status names the file; single-file output keeps its old left_out shape"):
        fc = _r9_csv(td, "r10_c.csv", [["x", "y"]], header="Foo,Bar")
        r_c = run_pipeline([fa, fc], td / "out_r10_c", input_accounts={str(fa): "Acc One"}, tz="Europe/Istanbul")
        assert r_c["status"] == "needs_mapping" and r_c["file"] == "r10_c.csv", r_c
        try:
            parse_input_accounts(["nope.csv=X"], [str(fa)])
            raise AssertionError("an --input-account naming no input must be an error")
        except ValueError:
            pass
        assert parse_input_accounts(["r10_a.csv=Acc=One"], [str(fa)]) == {str(fa): "Acc=One"}
        r_s = run_pipeline(fa, td / "out_r10_single", account_name="Acc One", tz="Europe/Istanbul")
        assert r_s["status"] == "ok" and r_s["data_quality"]["left_out"] == {"unreadable_date": 0, "duplicate": 0} and "before_after" not in r_s["data_quality"], r_s["data_quality"]
        print("[PASS] r10 multi-input: needs_mapping names the file; single-file left_out unchanged")


def _run_r10_fix3_cases(td):
    """2026-11-14: eval-set fixes (TR target set value, EN asset removed, system actors) + metrics_daily."""
    with _test_case("fix3: new-only target set lines give new value + unit, old stays None"):
        got = [parse_statement_values(t) for t in ("Kampanya değiştirildi\n  Hedef EBM: ₺180,00", "Kampanya değiştirildi\n  Hedef ROAS: %300",
                                                    "Kampanya değiştirildi\n  Teklif sınırı: ₺15,00", "Campaign changed\n  Target CPA: $20.00")]
        assert got == [(None, "₺180,00", "TRY"), (None, "300", "%"), (None, "₺15,00", "TRY"), (None, "$20.00", "USD")], got
        assert normalize_number("₺180,00".lstrip("₺"), "TR") == 180.0
        print("[PASS] fix3: Hedef EBM / Hedef ROAS / Teklif sınırı / Target CPA set lines parse as new-only values")

    with _test_case("fix3: EN 'Campaign asset changed' status lines mirror the TR asset rule"):
        cases = [("Campaign asset changed\n  Status changed from Enabled to Removed", ("Asset", "Asset removed")),
                 ("Campaign asset changed\n  Status changed from Enabled to Paused", ("Status", "Paused")),
                 ("Campaign asset changed\n  Status changed from Removed to Enabled", ("Status", "Enabled")),
                 ("1 ad changed\n  Status changed from Enabled to Removed", ("Status", "Removed"))]
        bad = [(t, match_summary({"raw_summary": t}), want) for t, want in cases if match_summary({"raw_summary": t}) != want]
        assert not bad, bad
        print("[PASS] fix3: EN asset removed / paused / enabled match the TR rows; other removals stay Status/Removed")

    with _test_case("fix3: Google system users are automation, Google Ads Team is unknown, look-alike names stay human"):
        bots = ["Recommendations Auto-Apply", "Google Ads system", "Automated rules", "Google Ads scripts", "Google Ads API", "ads-dartsearch-budget"]
        people = ["Kapil Rao", "Ann Abbott", "Jan Botha", "Sam Napier", "Capital One", "Rec Ommend", "apiary@example.test"]
        assert [derive_actor_type(n, None) for n in bots] == ["automation"] * len(bots), [(n, derive_actor_type(n, None)) for n in bots]
        assert [derive_actor_type(n, None) for n in people] == ["human"] * len(people), [(n, derive_actor_type(n, None)) for n in people]
        assert derive_actor_type("Google Ads Team [with user permission]", None) == "unknown"
        assert derive_actor_type("Google Ads Team [with user permission]", "WEB_CLIENT") == "human", "client_type still decides first"
        print("[PASS] fix3: system user strings -> automation, Google Ads Team -> unknown, look-alike names -> human")

    with _test_case("fix3: metrics_daily sums per day, null for missing, campaign rows win over ad-group rows of the same campaign-day"):
        fc = td / "fix3_camp.csv"
        fc.write_text("Day,Campaign,Impr.,Clicks,Cost\n2026-10-01,Camp A,100,5,2.50\n2026-10-01,Camp B,50,1,1.00\n2026-10-02,Camp A,10,--,0.50\n", encoding="utf-8")
        fg = td / "fix3_adgroup.csv"
        fg.write_text("Day,Campaign,Ad group,Impr.,Clicks,Conversions\n2026-10-01,Camp A,G1,60,3,1\n2026-10-01,Camp A,G2,40,2,1\n"
                      "2026-10-03,Camp A,G1,7,1,2\n2026-10-03,Camp A,G2,8,1,--\n", encoding="utf-8")
        mfs = [load_metrics_file(fc), load_metrics_file(fg)]
        assert all(m["status"] == "ok" for m in mfs), mfs
        got = compute_metrics_daily(mfs)
        # 10-01: Camp A from the campaign file only (100+50 impr); 10-03: no campaign row, ad-group rows summed.
        assert got == [{"date": "2026-10-01", "impressions": 150.0, "clicks": 6.0, "cost": 3.5, "conversions": None, "conversion_value": None},
                       {"date": "2026-10-02", "impressions": 10.0, "clicks": None, "cost": 0.5, "conversions": None, "conversion_value": None},
                       {"date": "2026-10-03", "impressions": 15.0, "clicks": 2.0, "cost": None, "conversions": 2.0, "conversion_value": None}], got
        multi = json.loads((td / "out_r10_multi" / "change_history.json").read_text(encoding="utf-8"))["metrics_daily"]
        d10 = next(x for x in multi if x["date"] == "2026-10-10")
        assert d10 == {"date": "2026-10-10", "impressions": 2200.0, "clicks": 12.0, "cost": 15.75, "conversions": 1.0, "conversion_value": 10.0}, d10
        assert next(x for x in multi if x["date"] == "2026-10-08")["conversions"] is None and len(multi) == 10, multi
        assert "metrics_daily" not in json.loads((td / "out_r10_single" / "change_history.json").read_text(encoding="utf-8")), "only with --metrics"
        print("[PASS] fix3: metrics_daily hand-checked sums, missing metric -> null, no campaign/ad-group double count")


_JS_R11_EXPLORER_SUITE = r"""
const assert = require('assert');
function row(over){ return Object.assign({ subcategory:null, resource_type:null, operation:null, old_value_num:null, new_value_num:null, old_value:null, new_value:null, raw_summary:'', category:'Other', source:'ui_export' }, over); }
DASH_DATA = { changes:[row({change_id:'n', actor_type:'unknown', user_name:'Rep Named', timestamp_iso:'2026-08-05T11:00:00'}),
                       row({change_id:'z', actor_type:'unknown', timestamp_iso:'2026-08-04T11:00:00'})], meta:{} };
resetAll();
renderExplorer(getFiltered());
const html = document.getElementById('explorerBody').innerHTML;
const want = actorLabel(DASH_DATA.changes[0]);
assert.strictEqual(want, 'Rep Named (actor type unknown)');
assert.ok(html.includes('>' + want + '<'), 'Explorer User cell must show actorLabel(): ' + html.slice(0, 400));
assert.ok(html.includes('<span class="pill">unknown</span>'), 'a nameless unknown actor keeps the unknown pill');
console.log('r11 explorer user cell matches actorLabel');
"""


def _run_r11_cases(td):
    """2026-11-15: round-11 regressions (R-1 create value, B-2 explorer actor label, R-6 same file twice, R-7 shared basenames)."""
    import shutil
    import subprocess
    with _test_case("r11 R-1: a created campaign keeps its budget value; target-set lines only on change headlines"):
        tr_create = "Kampanya oluşturuldu\n  Bütçe tutarı: ₺500,00\n  Hedef EBM: ₺40,00"
        en_create = "Campaign created\n  Budget amount: $50.00\n  Target CPA: $5.00"
        assert parse_statement_values(tr_create, "CREATE") == (None, "₺500,00", "TRY"), parse_statement_values(tr_create, "CREATE")
        assert parse_statement_values(tr_create) is None, "no operation, no budget path (as before R-1)"
        assert parse_statement_values(en_create, "CREATE") == (None, "$50.00", "USD")
        assert parse_statement_values("Kampanya oluşturuldu\n  Hedef EBM: ₺40,00", "CREATE") is None, "no budget line: no value"
        assert parse_statement_values("Kampanya oluşturuldu\n  Teklif sınırı: ₺15,00") is None
        assert parse_statement_values("Kampanya değiştirildi\n  Hedef EBM: ₺180,00", "UPDATE") == (None, "₺180,00", "TRY")
        f = _r9_csv(td, "r11_create.csv", [["2026-10-10 10:00:00", "u1@example.test", "Camp New", "", tr_create],
                                           ["2026-10-11 10:00:00", "u1@example.test", "Camp Old", "", "Kampanya değiştirildi\n  Hedef EBM: ₺180,00"]])
        r = run_pipeline(f, td / "out_r11_create", account_name="Acc", tz="Europe/Istanbul", generated_at="2026-11-15T00:00:00")
        assert r["status"] == "ok", r
        got = {x["campaign_name"]: (x["operation"], x["old_value_num"], x["new_value_num"], x.get("currency")) for x in _r9_rows(td / "out_r11_create")}
        assert got["Camp New"][:3] == ("CREATE", None, 500.0), got
        assert got["Camp Old"][:3] == ("UPDATE", None, 180.0), got
        print("[PASS] r11 R-1: creation statement keeps the budget value, change statement keeps the target-set value")

    with _test_case("r11 R-6: the same file given twice (two spellings) is read once, with a source warning"):
        f = _r9_csv(td, "r11_twice.csv", [["2026-10-10 10:00:00", "u1@example.test", "Camp A", "", "1 dil eklendi\n  Dil A"]])
        alt = td / "." / "r11_twice.csv"
        r = run_pipeline([f, alt], td / "out_r11_twice", account_name="Acc", tz="Europe/Istanbul", input_accounts={str(alt): "Acc Two"})
        assert r["status"] == "ok" and r["row_count"] == 1, r
        assert r["data_quality"]["left_out"].get("duplicate_across_files", 0) == 0, r["data_quality"]
        assert any("given more than once" in w for w in r["source_warnings"]), r["source_warnings"]
        rows = _r9_rows(td / "out_r11_twice")
        assert [x["source_file"] for x in rows] == ["r11_twice.csv"] and rows[0]["account_name"] == "Acc Two", rows
        assert parse_input_accounts(["r11_twice.csv=X"], [str(f), str(alt)]) == {str(f): "X"}, "same file twice is not ambiguous"
        print("[PASS] r11 R-6: one file passed twice is read once; account name follows it; warning says so")

    with _test_case("r11 R-7: a basename shared by two inputs is a structured error for --input-account and never a full path in outputs"):
        (td / "r11_d1").mkdir(exist_ok=True)
        (td / "r11_d2").mkdir(exist_ok=True)
        p1 = _r9_csv(td / "r11_d1", "export.csv", [["2026-10-10 10:00:00", "u1@example.test", "Camp A", "", "1 dil eklendi\n  Dil A"]])
        p2 = _r9_csv(td / "r11_d2", "export.csv", [["2026-10-12 10:00:00", "u2@example.test", "Camp B", "", "1 dil eklendi\n  Dil B"]])
        try:
            parse_input_accounts(["export.csv=X"], [str(p1), str(p2)])
            raise AssertionError("an ambiguous --input-account must be an error")
        except InputAccountError as e:
            assert e.candidates == [str(p1), str(p2)] and "matches 2 inputs" in str(e), (e, e.candidates)
        assert parse_input_accounts([f"{p2}=Y"], [str(p1), str(p2)]) == {str(p2): "Y"}, "the full path still picks one"
        cli = subprocess.run([sys.executable, str(Path(__file__).resolve()), "run", str(p1), str(p2), "--out-dir", str(td / "out_r11_cli"),
                              "--input-account", "export.csv=X"], capture_output=True, text=True, timeout=60)
        err = json.loads(cli.stdout)
        assert cli.returncode == 2 and err["status"] == "error" and err["candidates"] == [str(p1), str(p2)], (cli.returncode, cli.stdout)
        out = td / "out_r11_shared"
        r = run_pipeline([p1, p2], out, account_name="Acc", tz="Europe/Istanbul", generated_at="2026-11-15T00:00:00")
        assert r["status"] == "ok", r
        assert sorted(x["source_file"] for x in _r9_rows(out)) == ["r11_d1/export.csv", "r11_d2/export.csv"], [x["source_file"] for x in _r9_rows(out)]
        for name in ("changes.jsonl", "change_history.json", "dashboard.html"):
            assert str(td.resolve()) not in (out / name).read_text(encoding="utf-8") and str(td) not in (out / name).read_text(encoding="utf-8"), f"{name} carries a local path"
        print("[PASS] r11 R-7: ambiguous basename is a structured error naming candidates; outputs use <parent>/<basename>")

    if not shutil.which("node"):
        print("[SKIP] r11 explorer JS check (Node not found on PATH)")
        return
    with _test_case("r11 B-2: Explorer User cell uses actorLabel() for a named unknown actor (Node)"):
        body = re.search(r"<script>(.*?)</script>", DASHBOARD_TEMPLATE, re.S).group(1)
        body = body.replace("const DASH_DATA = /*__DASH_DATA__*/null;", "let DASH_DATA = null;", 1)
        stub = ("const __els = {};\nfunction __el(){ return {hidden:false, innerHTML:'', textContent:'', disabled:false, dataset:{}, style:{}, "
                "classList:{add(){}, remove(){}, toggle(){}, contains(){ return false; }}, setAttribute(){}, removeAttribute(){}, getAttribute(){ return null; }, "
                "addEventListener(){}, querySelectorAll(){ return []; }, querySelector(){ return null; }, appendChild(){}}; }\n"
                "global.document = { addEventListener(){}, getElementById(id){ return __els[id] || (__els[id] = __el()); }, "
                "querySelectorAll(){ return []; }, querySelector(){ return null; }, createElement(){ return __el(); }, body: __el() };\n")
        proc = subprocess.run(["node", "-e", stub + body + "\n" + _JS_R11_EXPLORER_SUITE], capture_output=True, text=True, timeout=15)
        assert proc.returncode == 0, f"r11 explorer JS check failed:\n{proc.stdout}\n{proc.stderr}"
        print(f"[PASS] r11 B-2 (Node): {proc.stdout.strip()}")


def _r12_rules_fixture():
    """Synthetic export: each rule fires once on purpose, plus near-misses that must not fire."""
    B = lambda c, a, b, inc=True: f"1 bütçenin tutarı {'artırıldı' if inc else 'azaltıldı'}\n  {c}: ₺{a} olan Bütçe tutarı, ₺{b} olarak değiştirildi"
    T = lambda a, b: f"Kampanya değiştirildi\n  ₺{a} olan Hedef EBM değeri, ₺{b} değerine artırıldı"
    lang = lambda x: f"1 dil eklendi\n  Dil {x}"
    S = lambda slot, v: f"1 teklif ayarlama değişikliği\n  {slot}: Reklam zaman planlaması teklif ayarlaması %{v} olarak ayarlandı"
    AD = "1 duyarlı arama ağı reklamı değiştirildi\n  etkin olan Durum, duraklatıldı olarak değiştirildi"
    u1, u2, u3 = "u1@example.test", "u2@example.test", "u3@example.test"
    rows = [
        ["2026-08-03 10:00:00", u1, "Camp Stale", "", B("Camp Stale", "100,00", "110,00")],          # stale_budget
        ["2026-08-20 10:00:00", u1, "Camp Stale", "", lang("A")],                                    # stale_entity
        ["2026-10-05 10:00:00", u1, "Camp Budget", "", B("Camp Budget", "300,00", "695,00")],       # budget_jump +132%
        ["2026-10-05 11:00:00", u1, "Camp Near", "", B("Camp Near", "100,00", "149,00")],           # near: +49%
        ["2026-10-05 10:00:00", u2, "Camp Cum", "", B("Camp Cum", "100,00", "130,00")],             # budget_cumulative
        ["2026-10-05 15:00:00", u2, "Camp Cum", "", B("Camp Cum", "130,00", "160,00")],
        ["2026-10-06 10:00:00", u1, "Camp Revert", "", "Kampanya öğesi oluşturuldu\n  Durum: Etkin\n  Uzantı türü: Site bağlantısı\n  Öğe kimliği: 111"],
        ["2026-10-06 10:05:00", u1, "Camp Revert", "", "Kampanya öğesi değiştirildi\n  Etkin olan Durum, Kaldırıldı olarak değiştirildi\n  Uzantı türü: Site bağlantısı\n  Öğe kimliği: 111"],
        ["2026-10-06 13:00:00", u1, "Camp Revert", "G1", '1 sıralı eşlemeli anahtar kelime eklendi\n  "kelime b"'],      # near: removed after 90 min
        ["2026-10-06 14:30:00", u1, "Camp Revert", "G1", '1 sıralı eşlemeli anahtar kelime kaldırıldı\n  "kelime b"'],
        ["2026-10-02 10:00:00", u2, "Camp Revert", "", B("Camp Revert", "100,00", "120,00")],       # reverted value (3 days)
        ["2026-10-05 12:00:00", u2, "Camp Revert", "", B("Camp Revert", "120,00", "100,00", False)],
        ["2026-10-08 10:00:00", u1, "Camp Revert", "G1", "1 geniş eşlemeli anahtar kelime duraklatıldı\n  kelime c: etkin olan durum duraklatıldı olarak değiştirildi"],
        ["2026-10-08 15:00:00", u1, "Camp Revert", "G1", "1 geniş eşlemeli anahtar kelime etkin\n  kelime c: duraklatıldı olan durum etkin olarak değiştirildi"],
        ["2026-09-01 10:00:00", u2, "Camp Near2", "", B("Camp Near2", "200,00", "220,00")],         # near: back after 8 days
        ["2026-09-09 10:00:00", u2, "Camp Near2", "", B("Camp Near2", "220,00", "200,00", False)],
        ["2026-10-06 16:00:00", u2, "Camp Repeat", "", B("Camp Repeat", "500,00", "520,00")],       # repeat_edit
        ["2026-10-06 16:03:00", u2, "Camp Repeat", "", B("Camp Repeat", "520,00", "540,00")],
        ["2026-10-06 15:00:00", u2, "Camp Gap", "", B("Camp Gap", "500,00", "520,00")],             # near: not a chain (600 != 520)
        ["2026-10-06 15:02:00", u2, "Camp Gap", "", B("Camp Gap", "600,00", "620,00")],
        ["2026-10-06 15:00:00", u1, "Camp Sched", "", S("Pazartesi 08:00 ÖÖ - 12:00 ÖS", "+10")],     # near: two different slots
        ["2026-10-06 15:00:04", u1, "Camp Sched", "", S("Pazartesi 12:00 ÖS - 04:00 ÖS", "+10")],
        ["2026-10-06 15:10:00", u1, "Camp Ads", "G1", AD],                                            # near: two ads, can't be told apart
        ["2026-10-06 15:10:02", u1, "Camp Ads", "G1", AD],
        ["2026-10-06 15:20:00", u1, "Camp Slot", "", S("Salı 08:00 ÖÖ - 12:00 ÖS", "+10")],           # repeat_edit: same slot twice
        ["2026-10-06 15:22:00", u1, "Camp Slot", "", S("Salı 08:00 ÖÖ - 12:00 ÖS", "+20")],
        ["2026-10-07 11:00:00", u1, "Camp Setup", "", "Kampanya oluşturuldu\n  Bütçe tutarı: ₺500,00"],      # near: setup burst
        ["2026-10-07 11:00:40", u1, "Camp Setup", "", "1 teklif ayarlama değişikliği\n  Mobil: %10 olarak ayarlandı"],
        ["2026-10-07 11:01:20", u1, "Camp Setup", "", "1 teklif ayarlama değişikliği\n  Mobil: %20 olarak ayarlandı"],
        ["2026-10-07 11:02:00", u1, "Camp Setup", "", lang("B")],
        ["2026-10-06 14:00:00", u2, "Camp Burst", "", lang("A")],                                    # burst_session
        ["2026-10-06 14:01:00", u2, "Camp Burst", "", lang("B")],
        ["2026-10-06 14:02:00", u2, "Camp Burst", "", lang("C")],
        ["2026-10-06 14:00:00", u2, "Camp Burst2", "", lang("A")],                                   # near: 5 minutes
        ["2026-10-06 14:02:30", u2, "Camp Burst2", "", lang("B")],
        ["2026-10-06 14:05:00", u2, "Camp Burst2", "", lang("C")],
        ["2026-10-05 10:00:00", u1, "Camp Bid", "", T("40,00", "45,00")],                           # bid_target_relearn
        ["2026-10-08 10:00:00", u1, "Camp Bid", "", T("45,00", "50,00")],
        ["2026-09-24 10:00:00", u1, "Camp Bid2", "", T("40,00", "45,00")],                          # near: 8 days
        ["2026-10-02 10:00:00", u1, "Camp Bid2", "", T("45,00", "50,00")],
        ["2026-10-07 10:00:00", "Automated rule", "Camp Auto", "", lang("A")],                       # automation_source
        ["2026-10-07 10:30:00", u3, "Camp Auto", "", lang("B")],                                     # unknown_user (with --known-users)
        ["2026-10-10 11:00:00", u1, "Camp Off", "", lang("A")],                                      # off_hours: Saturday
        ["2026-10-07 21:30:00", u1, "Camp Off", "", lang("B")],                                      # off_hours: 21:30
        ["2026-10-06 08:00:00", u1, "Camp Off", "", lang("C")],                                      # near: weekday 08:00
    ]
    for k in range(20):  # bulk_edit: 20 statements, 5 campaigns, under 8 minutes, no 3-minute burst
        rows.append([f"2026-10-09 09:{(k % 5) * 10 // 60 + (k // 5) * 150 // 60:02d}:{((k % 5) * 10 + (k // 5) * 150) % 60:02d}", u1, f"Bulk {k % 5}", "", lang(k)])
    for k in range(19):  # near: 19 statements
        rows.append([f"2026-10-09 10:{(k % 5) * 10 // 60 + (k // 5) * 150 // 60:02d}:{((k % 5) * 10 + (k // 5) * 150) % 60:02d}", u2, f"Bulk {5 + k % 5}", "", lang(k)])
    rows.append(["2026-10-12 12:00:00", u1, "Camp End", "", lang("Z")])
    return rows

def _run_r12_rules_cases(td):
    """2026-11-19 round 12 (RULES): rule flags, rules config, exec summary, campaign table, digest section. Synthetic data only."""
    import subprocess
    global DIGEST_LANG
    f = _r9_csv(td, "r12_rules.csv", _r12_rules_fixture())
    known = ["u1@example.test", "U2@Example.test"]
    run = lambda out, **kw: run_pipeline(f, td / out, account_name="Acc", generated_at="2026-10-13T00:00:00", **dict({"tz": "Europe/Istanbul"}, **kw))
    load = lambda out: json.loads((td / out / "change_history.json").read_text(encoding="utf-8"))
    judge_re = re.compile(r"\b(hata|hatalı|kötü|anomali|riskli|error|bad|wrong|risky|anomal\w*)\b", re.IGNORECASE)
    with _test_case("r12 rules: every rule fires once on its fixture and the near-misses stay quiet"):
        r = run("r12_all", known_users=known)
        assert r["status"] == "ok" and r["ui_lang"] == "tr", r
        d = load("r12_all")
        fl = d["flags"]
        att = sorted((x["rule"], x["campaign"]) for x in fl if x["tier"] == "attention")
        want = sorted([("budget_jump", "Camp Budget"), ("budget_cumulative", "Camp Cum"), ("reverted", "Camp Revert"), ("reverted", "Camp Revert"),
                       ("reverted", "Camp Revert"), ("repeat_edit", "Camp Repeat"), ("repeat_edit", "Camp Slot"), ("burst_session", "Camp Burst"), ("bid_target_relearn", "Camp Bid"),
                       ("automation_source", "Camp Auto"), ("unknown_user", None)])
        assert att == want, att
        kinds = sorted(x["fact"]["kind"] for x in fl if x["rule"] == "reverted")
        assert kinds == ["add_remove", "status", "value"], kinds
        assert not any("kelime b" in x["text"]["tr"] for x in fl), "a removal 90 minutes later is outside 60 minutes"
        rp = {x["campaign"]: x["text"]["tr"] for x in fl if x["rule"] == "repeat_edit"}
        assert rp == {"Camp Repeat": "Bütçe 3 dakika içinde 2 kez art arda değişti (₺500,00 → ₺520,00 → ₺540,00)",
                      "Camp Slot": 'Teklif ayarlaması "Salı 08:00 ÖÖ - 12:00 ÖS" 2 dakika içinde 2 kez art arda değişti'}, rp
        ju = [x for x in fl if x["rule"] == "budget_jump"][0]
        assert ju["fact"]["pct"] == 131.7 and ju["text"] == {"tr": "Bütçe ₺300,00 → ₺695,00 (+%132)", "en": "Budget ₺300.00 → ₺695.00 (+132%)"}, ju
        uu = [x for x in fl if x["rule"] == "unknown_user"][0]
        assert uu["user"] == "u3@example.test" and uu["fact"] == {"count": 1, "campaigns": 1}, uu
        off = sorted(x["ts_first"] for x in fl if x["rule"] == "off_hours")
        assert off == ["2026-10-07T21:30:00", "2026-10-10T11:00:00"], f"Saturday and 21:30 only, not a weekday 08:00: {off}"
        bulk = [x for x in fl if x["rule"] == "bulk_edit"]
        assert len(bulk) == 1 and bulk[0]["user"] == "u1@example.test" and bulk[0]["fact"]["changes"] == 20 and bulk[0]["fact"]["campaigns"] == 5, bulk
        st = {x["campaign"]: x["fact"] for x in fl if x["rule"] == "stale_entity"}
        assert st["Camp Stale"] == {"days": 53, "basis": "export", "level": "campaign"}, st
        sb = {x["campaign"]: x["fact"] for x in fl if x["rule"] == "stale_budget"}
        assert sb["Camp Stale"]["days"] == 70 and sb["Camp Stale"]["no_budget_change_in_export"] is False and sb["Camp End"]["no_budget_change_in_export"] is True, sb
        print("[PASS] r12 rules: every rule fires once on its fixture and the near-misses stay quiet")

    with _test_case("r12 rules: setup sessions are left out of burst_session and repeat_edit, and only those"):
        rows = _r9_rows(td / "r12_all")
        cfg, _ = resolve_rules_config(None)
        with_setup, _ = compute_rule_flags(rows, cfg)
        without, _ = compute_rule_flags([x for x in rows if x["subcategory"] != "Campaign created"], cfg)
        pick = lambda fl: sorted(x["rule"] for x in fl if x["campaign"] == "Camp Setup" and x["tier"] == "attention")
        assert pick(with_setup) == [] and pick(without) == ["burst_session", "repeat_edit"], (pick(with_setup), pick(without))
        print("[PASS] r12 rules: setup sessions are left out of burst_session and repeat_edit, and only those")

    with _test_case("r12 rules: flag, rules_config, exec_summary and campaign_table shapes; same data in DASH_DATA"):
        ids = {x["change_id"] for x in d["changes"]}
        assert [x["id"] for x in fl] == [f"F{k}" for k in range(1, len(fl) + 1)], "ids F1.. in order"
        keys = ["id", "rule", "tier", "change_ids", "event_ids", "account", "campaign", "ad_group", "user", "ts_first", "ts_last", "fact", "text"]
        for x in fl:
            assert sorted(x) == sorted(keys) and set(x["change_ids"]) <= ids and set(x["text"]) == {"tr", "en"}, x
            assert x["ts_first"] is None or x["ts_first"] <= x["ts_last"], x
        tiers = [x["tier"] for x in fl]
        assert tiers == sorted(tiers, key=lambda t: t != "attention"), "attention first"
        a_ts = [x["ts_first"] for x in fl if x["tier"] == "attention"]
        assert a_ts == sorted(a_ts, reverse=True), "newest first within a tier"
        rc = d["rules_config"]
        assert [c["rule"] for c in rc] == [n for n, _, _ in RULE_DEFS] and all(sorted(c) == ["description", "disabled_reason", "enabled", "hits", "params", "rule", "tier", "title"] for c in rc), rc[0]
        assert {c["rule"]: c["hits"] for c in rc} == dict(Counter(x["rule"] for x in fl), **{c["rule"]: 0 for c in rc if not any(x["rule"] == c["rule"] for x in fl)})
        es = d["exec_summary"]
        assert sorted(es) == ["budget", "headline", "people", "period", "sentences", "stale", "top_campaigns", "totals"], sorted(es)
        assert es["period"] == {"from": "2026-08-03", "to": "2026-10-12"} and es["totals"]["attention_flags"] == 11 and es["totals"]["automation"] == 1, es
        assert es["budget"] == {"currency": "TRY", "increase_total": 634.0, "increase_count": 11, "decrease_total": -40.0, "decrease_count": 2, "net": 594.0, "mixed_currencies": False}, es["budget"]
        assert es["headline"]["tr"] == f"Bu dönemde {_r12_num(len(d['changes']), 'tr')} değişiklik yapıldı, 11 durum göz atmayı hak ediyor." and es["headline"]["en"].endswith("11 items are worth a look."), es["headline"]
        assert "Bütçeler toplamda günlük ₺594 arttı." in es["sentences"]["tr"] and "Daily budgets went up by ₺594 in total." in es["sentences"]["en"], es["sentences"]
        assert es["stale"][0] == {"account": "Acc", "campaign": "Camp Stale", "days": 53, "basis": "export"}, es["stale"]
        ct = {c["campaign"]: c for c in d["campaign_table"]}
        assert len(ct) == len({x["campaign_name"] for x in d["changes"]}) and ct["Camp Revert"]["flags_by_rule"] == {"reverted": 3}, ct["Camp Revert"]
        assert ct["Camp Stale"]["days_since"] == 53 and ct["Camp Stale"]["last_change"] == "2026-08-20T10:00:00" and ct["Camp Cum"]["budget_changes"] == 2, ct["Camp Stale"]
        html = (td / "r12_all" / "dashboard.html").read_text(encoding="utf-8")
        emb = json.loads(re.search(r"/\*__DASH_DATA__\*/(\{.*?\});?\s*\n", html, re.S).group(1))
        assert all(emb[k] == d[k] for k in ("flags", "rules_config", "exec_summary", "campaign_table", "ui_lang")), "DASH_DATA carries the same rule data"
        print("[PASS] r12 rules: flag, rules_config, exec_summary and campaign_table shapes; same data in DASH_DATA")

    with _test_case("r12 rules: wording is factual, Turkish strings start with a capital, no em-dash"):
        texts = [c[k][lg] for c in rc for k in ("title", "description") for lg in ("tr", "en")] + [x["text"][lg] for x in fl for lg in ("tr", "en")]
        texts += [es["headline"][lg] for lg in ("tr", "en")] + es["sentences"]["tr"] + es["sentences"]["en"]
        assert not any(judge_re.search(t) or _DIGEST_JUDGEMENT_RE.search(t) for t in texts), [t for t in texts if judge_re.search(t) or _DIGEST_JUDGEMENT_RE.search(t)]
        assert not any("\u2014" in t for t in texts), "no em-dash"
        tr = [c[k]["tr"] for c in rc for k in ("title", "description")] + [x["text"]["tr"] for x in fl] + [es["headline"]["tr"]] + es["sentences"]["tr"]
        bad = [t for t in tr if not (t[0].isdigit() or t[0] in '"%₺' or t[0] == t[0].upper() != t[0].lower())]
        assert not bad, bad
        print("[PASS] r12 rules: wording is factual, Turkish strings start with a capital, no em-dash")

    with _test_case("r12 rules: --rules-config overrides and validation, --work-hours, no timezone, masking"):
        r = run("r12_cfg", rules_config={"budget_jump": {"pct": 40}, "off_hours": {"enabled": False}}, work_hours="09-18")
        d2 = load("r12_cfg")
        rc2 = {c["rule"]: c for c in d2["rules_config"]}
        assert r["status"] == "ok" and sorted(x["campaign"] for x in d2["flags"] if x["rule"] == "budget_jump") == ["Camp Budget", "Camp Near"], "49% fires at a 40% threshold"
        assert rc2["off_hours"]["enabled"] is False and rc2["off_hours"]["hits"] == 0 and rc2["off_hours"]["disabled_reason"]["en"] == "Turned off in the rules config.", rc2["off_hours"]
        assert rc2["off_hours"]["params"] == {"start": "18:00", "end": "09:00", "weekends": True} and rc2["budget_jump"]["params"] == {"pct": 40}, rc2["off_hours"]
        assert rc2["unknown_user"]["enabled"] is False and "--known-users" in rc2["unknown_user"]["disabled_reason"]["tr"], rc2["unknown_user"]
        r = run("r12_wh", work_hours="09-18")
        assert sorted(x["ts_first"][11:16] for x in load("r12_wh")["flags"] if x["rule"] == "off_hours" and x["campaign"] == "Camp Off") == ["08:00", "11:00", "21:30"], "08:00 is outside 09-18"
        for bad_cfg, frag in (({"nope": {}}, "Unknown rule"), ({"budget_jump": {"pct": -1}}, "positive"), ({"budget_jump": {"foo": 1}}, "unknown setting"),
                              ({"repeat_edit": {"min_count": 2.5}}, "whole"), ({"off_hours": {"start": "25:00"}}, "time like"), ([], "JSON object")):
            r = run("r12_bad", rules_config=bad_cfg)
            assert r["status"] == "error" and frag in r["message"], (bad_cfg, r)
        r = run("r12_bad", work_hours="9-9")
        assert r["status"] == "error" and "--work-hours" in r["message"], r
        r = run("r12_notz", tz="unknown")
        oh = {c["rule"]: c for c in load("r12_notz")["rules_config"]}["off_hours"]
        assert oh["enabled"] is False and oh["hits"] == 0 and "--timezone" in oh["disabled_reason"]["en"], oh
        r = run("r12_mask", known_users=known, mask_users=True)
        d3 = load("r12_mask")
        uu = [x for x in d3["flags"] if x["rule"] == "unknown_user"]
        assert len(uu) == 1 and "@" not in json.dumps(d3["flags"]) + json.dumps(d3["exec_summary"]), uu
        print("[PASS] r12 rules: --rules-config overrides and validation, --work-hours, no timezone, masking")

    with _test_case("r12 rules: zero flags headline, empty input and metrics-basis stale entities"):
        r = run("r12_none", rules_config={n: {"enabled": False} for n, _, _ in RULE_DEFS})
        es0 = load("r12_none")["exec_summary"]
        assert es0["headline"]["tr"].endswith("değişiklik yapıldı, hiçbiri kural eşiklerini aşmadı.") and es0["headline"]["en"].endswith("none crossed the rule thresholds."), es0["headline"]
        empty = td / "r12_empty.json"
        empty.write_text("[]", encoding="utf-8")
        r = run_pipeline(empty, td / "r12_empty_out", generated_at="2026-10-13T00:00:00")
        de = json.loads((td / "r12_empty_out" / "change_history.json").read_text(encoding="utf-8"))
        assert r["status"] == "ok" and de["flags"] == [] and len(de["rules_config"]) == 12 and de["campaign_table"] == [] and de["exec_summary"]["totals"]["changes"] == 0, de["exec_summary"]
        m = td / "r12_metrics.csv"
        m.write_text("Day,Campaign,Impressions,Clicks,Cost\n" + "".join(f"2026-10-{d:02d},{c},100,5,10\n" for d in range(1, 13) for c in ("Camp Stale", "Camp Metric")), encoding="utf-8")
        r = run("r12_metrics", metrics=[m])
        dm = load("r12_metrics")
        se = {(x["campaign"], x["fact"]["basis"]): x["fact"] for x in dm["flags"] if x["rule"] == "stale_entity"}
        assert se[("Camp Metric", "metrics")] == {"days": None, "basis": "metrics", "level": "campaign"} and ("Camp Stale", "metrics") not in se, se
        assert sorted(x["campaign"] for x in dm["flags"] if x["rule"] == "stale_budget") == ["Camp Stale"], "with metrics, only campaigns active in the metrics"
        print("[PASS] r12 rules: zero flags headline, empty input and metrics-basis stale entities")

    with _test_case("r12 rules: digest 'Göz atılacaklar / Worth a look' section, both languages"):
        md = (td / "r12_all" / "digest.md").read_text(encoding="utf-8")
        sec = md[md.index("## Göz atılacaklar"):md.index("## Kategoriye")]
        for want in ("### Bütçe tek seferde %50 ya da daha fazla değişti (1)", "- 05.10.2026 10:00 · Camp Budget · Bütçe ₺300,00 → ₺695,00 (+%132)",
                     "### Eklenip hemen geri alınan değişiklik (3)", "**Bilgi için**", "- Mesai dışı yapılan değişiklik: 2", "- Toplu düzenleme: 1"):
            assert want in sec, (want, sec)
        assert sec.count("\n- ") >= 12 and not _DIGEST_JUDGEMENT_RE.search(sec), sec
        DIGEST_LANG = "en"
        try:
            run("r12_en", rules_config={n: {"enabled": False} for n, _, _ in RULE_DEFS})
        finally:
            DIGEST_LANG = "auto"
        md = (td / "r12_en" / "digest.md").read_text(encoding="utf-8")
        assert "## Worth a look" in md and "No rule threshold was crossed." in md and "- Bulk edit: off. Turned off in the rules config." in md, md
        assert load("r12_en")["ui_lang"] == "en", "--lang en sets ui_lang"
        print("[PASS] r12 rules: digest 'Göz atılacaklar / Worth a look' section, both languages")

    with _test_case("r12 rules: CLI --lang/--digest-lang alias, --known-users file, --rules-config errors"):
        ku = td / "r12_known.txt"
        ku.write_text("# team\nu1@example.test\nU2@EXAMPLE.TEST  # lead\n\n", encoding="utf-8")
        rc_file = td / "r12_rc.json"
        rc_file.write_text('{"bulk_edit": {"min_changes": 50}}', encoding="utf-8")
        base = [sys.executable, str(Path(__file__).resolve()), "run", str(f), "--account-name", "Acc", "--timezone", "Europe/Istanbul"]
        env = dict(os.environ, XDG_DATA_HOME=str(td / "r12_xdg"))
        p = subprocess.run(base + ["--out-dir", str(td / "r12_cli"), "--digest-lang", "en", "--known-users", str(ku), "--rules-config", str(rc_file), "--work-hours", "08-20"],
                           capture_output=True, text=True, timeout=60, env=env)
        res = json.loads(p.stdout)
        dc = load("r12_cli")
        assert p.returncode == 0 and res["ui_lang"] == "en" and dc["ui_lang"] == "en", (p.returncode, p.stderr[-300:])
        assert [x["user"] for x in dc["flags"] if x["rule"] == "unknown_user"] == ["u3@example.test"] and not any(x["rule"] == "bulk_edit" for x in dc["flags"]), "file list read, case-insensitive, comments skipped"
        p = subprocess.run(base + ["--out-dir", str(td / "r12_cli2"), "--lang", "tr"], capture_output=True, text=True, timeout=60, env=env)
        assert p.returncode == 0 and json.loads(p.stdout)["ui_lang"] == "tr", p.stderr[-300:]
        rc_file.write_text("{not json", encoding="utf-8")
        p = subprocess.run(base + ["--out-dir", str(td / "r12_cli3"), "--rules-config", str(rc_file)], capture_output=True, text=True, timeout=60, env=env)
        assert p.returncode == 2 and json.loads(p.stdout)["status"] == "error" and "not valid JSON" in json.loads(p.stdout)["message"], p.stdout
        print("[PASS] r12 rules: CLI --lang/--digest-lang alias, --known-users file, --rules-config errors")


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
    _run_r10_pipeline_cases(td)
    _run_r10_docs_cases(td)
    _run_r10_dashboard_cases(td)
    _run_r10_fix3_cases(td)
    _run_r11_cases(td)
    _run_r12_rules_cases(td)
    _run_r12_ui_cases(td)


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
    p_run.add_argument("inputs", nargs="+", metavar="input", help="One or more change-history files (CSV/TSV/JSON). Several files are merged; a change found in more than one file is kept once.")
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
    p_run.add_argument("--input-account", action="append", metavar="FILE=NAME", help="Account name for one input (or --metrics) file with no account column, repeatable. --account-name still covers the other files.")
    p_run.add_argument("--metrics", action="append", metavar="PATH", help="Daily performance CSV (Day, Campaign, optional Ad group/Account, Impressions/Clicks/Cost/Conversions/Conv. value; TR or EN headers), repeatable. Adds before/after windows per change.")
    p_run.add_argument("--window-days", type=int, default=7, help="Days in each before/after window (default 7).")
    p_run.add_argument("--conversion-lag-days", type=int, default=7, help="Most recent days of metrics whose conversions may still grow (default 7).")
    p_run.add_argument("--open", action="store_true")
    p_run.add_argument("--lang", "--digest-lang", dest="lang", choices=["auto", "tr", "en"], default="auto", help="Language of the dashboard and digest.md: auto (default) is tr for a Turkish export, else en. --digest-lang is the old name.")
    p_run.add_argument("--rules-config", metavar="FILE.json", help="Rule thresholds: {rule: {enabled, ...params}}, e.g. {\"budget_jump\": {\"pct\": 30}, \"off_hours\": {\"enabled\": false}}. Unlisted rules keep their defaults.")
    p_run.add_argument("--known-users", metavar="FILE", help="One e-mail or name per line (# comments, case-insensitive). Turns on the unknown_user rule: people not on the list are listed.")
    p_run.add_argument("--work-hours", metavar="HH-HH", help="Working hours for the off_hours rule, e.g. 08-20 or 08:30-19:00 (local time). Overrides off_hours start/end from --rules-config.")
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
        DIGEST_LANG = args.lang
        rules_config = known_users = None
        # 2026-11-19: same JSON error envelope as --extra-rules for the two new input files.
        try:
            if args.rules_config:
                with open(args.rules_config, encoding="utf-8") as f:
                    rules_config = json.load(f)
            if args.known_users:
                with open(args.known_users, encoding="utf-8-sig") as f:
                    known_users = [ln.split("#", 1)[0].strip() for ln in f]
                known_users = [k for k in known_users if k]
        except OSError as e:
            print(json.dumps({"status": "error", "message": f"Could not read {e.filename}: {e.strerror}"}, indent=2, ensure_ascii=False))
            sys.exit(2)
        except json.JSONDecodeError as e:
            print(json.dumps({"status": "error", "message": f"--rules-config file is not valid JSON: {args.rules_config} ({e})"}, indent=2, ensure_ascii=False))
            sys.exit(2)
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
            input_accounts = parse_input_accounts(args.input_account, args.inputs + (args.metrics or []))
        except ValueError as e:
            err = {"status": "error", "message": str(e)}
            if getattr(e, "candidates", None):
                err["candidates"] = e.candidates
            print(json.dumps(err, indent=2, ensure_ascii=False))
            sys.exit(2)
        result = run_pipeline(
            args.inputs if len(args.inputs) > 1 else args.inputs[0], args.out_dir, mapping_file=args.mapping_file, column_map=column_map or None, date_format=args.date_format,
            decimal_style=args.decimal_style, tz=args.timezone, mask_users=args.mask_users,
            allow_unknown_categories=args.allow_unknown_categories, force_review=args.force_review,
            extra_rules=extra_rules, open_browser=args.open, account_name=args.account_name, currency=args.currency,
            input_accounts=input_accounts, metrics=args.metrics, window_days=args.window_days, conversion_lag_days=args.conversion_lag_days,
            rules_config=rules_config, known_users=known_users, work_hours=args.work_hours, lang=args.lang,
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
