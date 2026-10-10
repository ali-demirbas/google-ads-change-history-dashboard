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
from, what category it falls into, and — for campaigns that appear in the
export — when each was last changed. It states facts ("Campaign X's last
change in this export was 23 days ago"), never judgment ("the agency is
neglecting Campaign X").

Everything is scoped to the file it was given. A campaign with no row in the
export is not "untouched": it may have had no change in the export's date
range, or may not exist. Never tell the user a campaign "wasn't changed"
on the strength of its absence — say it doesn't appear in this export.

## What it reads

| Source | Language | Status |
|---|---|---|
| Google Ads API `ChangeEvent`, flattened to JSON/CSV (what `tools/fetch_live_data.py` writes) | field names, not prose | Structured rules. The live fetch itself is unverified against a real account. |
| Google Ads UI "Change history" CSV download | English, Turkish | Free-text summaries matched by patterns taken from real exports. Phrasings not seen yet fall to "Other". |
| Legacy Turkish summary sheet (`Hesap Adı`/`Tarih`/`Değişiklik Özeti`…) | Turkish | Verified against one real schema. |
| Anything else | — | Stops with `needs_mapping`; categorization then depends on what the columns contain. |

A UI export in any other language will map its columns (after `needs_mapping`)
but most rows will be "Other": the summary patterns are language-specific and
none exist for it. That is reported, not hidden — see `data_quality` below.
Don't add patterns for a language from guesswork; they come from real rows.

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

The script never guesses past a real ambiguity — it exits with code 2 and a
JSON `status`. Read it, ask the user the one question it raises in plain
words, and re-run with their answer. If the answer is missing or doesn't
settle it, ask again — never fill it in.

| `status` | What to show | Ask the user | Their answer becomes |
|---|---|---|---|
| `needs_mapping` | `unmapped_headers` with `sample_values`; `missing_required` | "Which column holds the change date? The account? The old and new value?" — one per missing field | `--map timestamp="Modified On" --map old_value=Was …` (one per answer) |
| `needs_review` | `issues`: the column and how empty it is | "The *Was* column is empty on 33% of rows. Is that right for this file, or is it the wrong column?" | right → `--force-review`; wrong column → `--map` |
| `needs_date_format` | `sample_values` (the ambiguous dates themselves) | "Is 03/04/2026 the 3rd of April or March 4th?" | `--date-format DMY` or `MDY` |
| `needs_decimal_style` | `sample_values` | "Is 150.000 one hundred and fifty thousand, or 150?" | `--decimal-style TR` (thousand) or `US` (150) |
| `needs_date_review` | the count and % of unreadable dates | "N of M dates couldn't be read as *format*. Is the date column right?" | fix `--date-format`/`--map timestamp=…`, or `--force-review` if the bad dates are real |
| `needs_category_review` | each `unknown_combinations` entry with its example old/new | "What kind of change is `proximity_target` on a campaign criterion?" | a rule in a JSON list passed with `--extra-rules`, or — if they'd rather not decide — `--allow-unknown-categories` (those rows become "Other" and are counted) |
| `error` | `message` | nothing to decide — the input or a flag is wrong | fix what the message names |

`--map` answers are remembered per column-header set (in
`mapping-profiles/`), so the same export shape is asked about once. Two
different sources with identical headers would share that answer — a known,
deliberate tradeoff. `--mapping-file` (a JSON file with the same answers)
still works.

`--allow-unknown-categories` and `--force-review` are for an answer the user
actually gave, not for getting past the question.

## After an `ok` run — read `data_quality` before reporting back

`status: ok` means nothing was ambiguous. It does not mean every input row
is in the report. The result's `data_quality` block (also in `coverage.txt`
and `change_history.json` → `meta.data_quality`, and shown under the
dashboard title) says what happened to the rows:

- `input_rows` → `rows_in_report`, with `left_out.unreadable_date` and
  `left_out.duplicate` between them. The numbers add up.
- `in_report_with_limits` — rows that are in the report but less than
  complete: `category_other` (no rule matched), `timezone_unknown` (not on
  the timeline; ask for `--timezone`), `operation_inferred`,
  `value_parsed_from_summary`, `multi_field_unsplit`,
  `name_without_reference_dropped`.
- `ignored_headers` — source columns nothing used. Usually harmless (`feed`,
  `asset`); if one is clearly a name, ID or value column, offer `--map`.

Tell the user every non-zero count in one or two sentences, with the number —
"3 of 1,200 rows had a date I couldn't read and are not in the report" —
however small. Don't call it a problem or a pass; say what it is. The
script's own stops (over 20% unreadable dates, over 30% of a required column
empty, `coverage.txt`'s warning above 10% "Other") are unchanged.

Amounts are shown without a currency unless the user states one: ask, then
pass `--currency TRY` (one ISO code for the whole file; skip it for a
multi-currency MCC export).

## Output

`changes.jsonl` (canonical rows), `change_history.json` (aggregated), a
single-file `dashboard.html` (no CDN, works offline — Filters, Summary,
Activity Timeline, User Activity split by human/automation, Account/Campaign
drill-down, Category Distribution, Campaign Last Changes, Change Explorer with
detail panel), `coverage.txt`, `unknown-fields.json` when applicable. The
dashboard prints cleanly (browser Print → Save as PDF: landscape A4, the
Explorer's current page with every column wrapped instead of clipped).

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

`--mask-users` (default off) removes e-mail addresses from every output file:
a human becomes "User A"/"User B" (the same label on every later run), and an
address on a script/bulk/rule row is dropped. It does **not** mask account,
campaign or ad group names, any ID, resource paths, or old/new values — if
the user wants to share a report without revealing whose account it is, say
plainly that this flag doesn't do that. The label table
(`mapping-profiles/mask-labels.json`) holds the real identities; never share
or commit it. This repo is public: only synthetic data goes in it.
