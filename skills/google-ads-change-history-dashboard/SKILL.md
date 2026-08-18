---
name: google-ads-change-history-dashboard
description: Google Ads change history — who changed what, when, in which account/campaign/ad group, old value, new value, category. Never judges whether a change was good or risky. Use when the user asks "who changed X", "what changed in the last 30 days", "show me the change history", "which campaigns haven't been touched", "change history dashboard", or gives a Google Ads change export (CSV, TSV, or pre-flattened Google Ads ChangeEvent JSON) and wants it turned into a report or dashboard. Does NOT read XLSX, Google Sheets URLs, or call the Google Ads API directly — those need converting to CSV/JSON first (correction 2026-08-18: the description used to claim all of these, the code never did).
---

# Ads Change History

Single-code-file skill. Everything — canonical schema, header-alias table,
category rules, validation/normalization/categorization/aggregation logic, the
dashboard HTML/CSS/JS template, and a self-test with built-in synthetic
fixtures — lives in **`ads_change_history.py`**. Read that file's module
docstring first; it's the actual spec. (`mapping-profiles/` is generated at
runtime, not part of the skill definition itself.)

## What it answers (and refuses to answer)

Who changed it, when, which account/campaign/ad group, what it changed to and
from, what category it falls into, which campaigns were touched recently,
which weren't. It states facts ("Campaign X hasn't changed in 23 days"), never
judgment ("the agency is neglecting Campaign X") — that's explicitly out of
scope for this version.

## Running it

```bash
python3 ads_change_history.py run <input.csv|.json> --out-dir ./out --open
python3 ads_change_history.py query ./out/changes.jsonl --user "User A" --since 7d
python3 ads_change_history.py self-test
```

`query`'s `--user`/`--account`/`--campaign` are substring matches (`"User A"`
also matches `"User AB"`) — intentionally more forgiving than the dashboard's
exact-match filter dropdowns, which are populated from a closed picklist and
don't need that leniency. Two different interfaces, two different correct
defaults — not an inconsistency to reconcile.

## When the pipeline stops and asks

The script never guesses past a real ambiguity — it exits non-zero with a
structured JSON `status` instead. Whoever is driving it (a Claude session, in
practice) reads that JSON, asks the user in chat, and re-invokes with the
answer:

- `needs_mapping` — unrecognized column headers. Show `unmapped_headers` +
  `sample_values` to the user, ask which canonical field each is, write a
  `{"source_label": ..., "mapping": {...}}` JSON file, re-run with
  `--mapping-file`. Gets saved to `mapping-profiles/` keyed by a fingerprint of
  the *sorted column-header set* — so it's asked once per distinct header
  schema, not once per file. (Two genuinely different sources that happen to
  export identical column headers would share a profile — that's a deliberate
  tradeoff, not a bug, but worth knowing.)
- `needs_review` — a mapped column is >30% empty on required data — usually a
  wrong mapping, not sparse data. Confirm with the user; `--force-review` to
  proceed anyway.
- `needs_date_format` / `needs_decimal_style` — genuinely ambiguous date
  (`03/04/2026`) or number (`150.000`). Ask, then pass
  `--date-format DMY|MDY|ISO` / `--decimal-style TR|US`.
- `needs_category_review` — an unrecognized `(resource_type, field_name,
  operation)` combination. Ask the user which category/subcategory it
  belongs to, add a rule to `CATEGORY_RULES["structured_rules"]` in the
  script (or pass a JSON list via `--extra-rules`), re-run.

Don't skip past any of these by guessing — that's the one rule this skill
can't bend on. `--allow-unknown-categories` / `--force-review` exist for
deliberate, user-confirmed overrides, not for convenience.

## Output

`changes.jsonl` (canonical rows), `change_history.json` (aggregated), a
single-file `dashboard.html` (no CDN, works offline — Filters, Summary,
Activity Timeline, User Activity split by human/automation, Account/Campaign
drill-down, Category Distribution, Campaign Last Changes, Change Explorer with
detail panel), `coverage.txt`, `unknown-fields.json` when applicable.

When the dataset includes API-sourced rows, the dashboard shows a caveat
banner: ChangeEvent may not include every entry the Google Ads UI's own
Change History page shows, and can have up to a 3-minute delay reflecting a
very recent change (both per Google's own documentation).

The dashboard also always carries a collapsed "What this report can't do"
note (run-independent, unlike the banner above) — no live API access beyond
the given export, no judgment of whether a change was good or risky, plus the
ChangeEvent limits restated for when that source applies. Same note belongs
in any other output surface this skill grows.

## Privacy

`--mask-users` (default off) replaces human user names/emails with "User
A"/"User B" for external sharing. Account and campaign names are never
masked — that's the real user's own data. Local-only for now; no repo/sharing
decision made yet, so don't commit real customer data anywhere.
