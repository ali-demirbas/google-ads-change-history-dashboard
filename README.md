# google-ads-change-history-dashboard — who changed what, when, factually

[![validate](https://github.com/ali-demirbas/google-ads-change-history-dashboard/actions/workflows/validate.yml/badge.svg)](https://github.com/ali-demirbas/google-ads-change-history-dashboard/actions/workflows/validate.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
![Single file](https://img.shields.io/badge/skill-single_file-blue)

A Claude Code skill that turns one or more Google Ads change-history exports (CSV, TSV, or a pre-flattened `ChangeEvent` JSON, from one account or several) into an offline, single-file HTML dashboard and a short Markdown digest: who changed what, in which account/campaign/ad group, old value to new value, what category it falls into, and which changes are worth a look. Optional daily performance CSVs add the numbers of the days before and after each change, side by side.

**Facts, never verdicts.** Twelve rules mark changes *worth a look*: a budget that moved 50% or more in one step, a setting changed back soon after, several edits in a few minutes, edits outside working hours, a person not on your team list, a campaign with no change in 30 days, and more. Each flag is a fact measured against a threshold you can set, shown with the rule that fired: "Budget ₺300,00 → ₺695,00 (+132%)" under "one-step budget change of 50% or more". It never says a change was good, bad, wrong, risky or an error, and it uses no red/green. "Campaign X has had no change for 45 days" is something it says; "the agency is neglecting Campaign X" is not. Whether a flag matters is your call.

## What it answers

- Who changed this campaign's budget last week, and what was it before?
- What changed in the last 7 days / 24 hours, across which accounts?
- Of the campaigns in this export, which had their last change 30+ days ago?
- Was this change made by a person or an automation (a script, a bidding rule, a Recommendation)?
- Which category of change is most common right now — budget, bidding, status, keywords?
- Which changes are worth a look, and why: which rule fired, on which numbers, against which threshold?
- What were clicks, cost and conversions in the 7 days before and after this budget change? (numbers only, with the days still maturing marked)

## What it won't do

- Won't tell you if a change was a *good* one. A flag says a threshold you can change was crossed, nothing more.
- Won't read XLSX or a Google Sheets URL directly (an XLSX/ZIP/GZIP file stops with "export as CSV"), and the skill itself never calls the network — the optional `tools/fetch_live_data.py` is the only part that talks to the Google Ads API.
- Won't say anything about a campaign that isn't in the export. No row means no evidence — not "unchanged", and not even that the campaign exists.
- Won't categorize a UI export in a language it has no patterns for. English and Turkish UI exports, the legacy Turkish sheet and API data are categorized. German, Spanish, French, Portuguese, Italian and Dutch exports have their headers and dates read, but most rows land in "Other", and `data_quality` warns that the UI language is the likely cause.
- Won't guess past a genuine ambiguity (an unrecognized column, an ambiguous date format like `03/04/2026`, an unrecognized category combination) — it stops and asks instead of silently picking an answer.

## Install

**In Claude Code:**

```
/plugin marketplace add ali-demirbas/google-ads-change-history-dashboard
/plugin install google-ads-change-history-dashboard@google-ads-change-history-dashboard
```

**Or run it directly, no Claude required:**

```bash
git clone https://github.com/ali-demirbas/google-ads-change-history-dashboard.git
cd google-ads-change-history-dashboard/skills/google-ads-change-history-dashboard
python3 ads_change_history.py run <your-export.csv> --out-dir ./out --open
```

No dependencies beyond the Python 3 standard library.

**Have real Google Ads API access?** [`tools/fetch_live_data.py`](tools/fetch_live_data.py) is an optional, separate script that pulls change history live via the API and writes it in the exact shape this skill reads — skips the manual export step. Doesn't touch the skill's zero-dependency promise; see [`tools/README.md`](tools/README.md).

## Usage

```bash
python3 ads_change_history.py run <input.csv|.json> [<more inputs> ...] --out-dir ./out --open
python3 ads_change_history.py run a.csv b.csv --input-account b.csv="Account B" --account-name "Account A" --timezone Europe/Istanbul --out-dir ./out
python3 ads_change_history.py run export.csv --account-name "Account A" --timezone Europe/Istanbul --metrics daily.csv --window-days 7 --out-dir ./out
python3 ads_change_history.py run export.csv --account-name "Account A" --timezone Europe/Istanbul --known-users team.txt --rules-config rules.json --lang en --out-dir ./out
python3 ads_change_history.py query ./out/changes.jsonl --user "User A" --since 7d
python3 ads_change_history.py self-test
```

Facts only you know, never guessed:

- `--account-name "My account"` for a UI export, which has no account column. With several files, `--input-account FILE=NAME` (repeatable) names the account of one file; `--account-name` covers the rest.
- `--timezone Europe/Istanbul` for timestamps without an offset (UI exports). Without it those rows are left out of the timeline, and the run says how many.
- `--currency TRY` to show budget/bid amounts with their unit, when the text doesn't already carry a currency sign.
- `--encoding cp1254` (or another codec) when the file isn't UTF-8/UTF-16 and the run says it can't read it cleanly.
- `--known-users team.txt` (one e-mail or name per line, `#` comments) to flag changes by people not on your team. Without it that rule is off, and the report says so.

Thresholds and language, optional: `--rules-config rules.json` changes any rule's parameters or turns a rule off (`{"budget_jump": {"pct": 40}, "off_hours": {"enabled": false}}`), `--work-hours 08-20` sets working hours, and `--lang auto|tr|en` sets the language of the dashboard and the digest (auto: Turkish for a Turkish export; `--digest-lang` still works as an alias). Every rule, its default and what it cannot see: [`references/rules.md`](skills/google-ads-change-history-dashboard/references/rules.md).

Several inputs are read in one run: each file is validated and mapped on its own (a question names the file it is about), every row keeps its `source_file`, a change that appears in more than one file (overlapping downloads) is kept once and counted as `duplicate_across_files`, and `meta.sources` lists each file (`file`, `rows`, `accounts`, `source_label`, `date_min`, `date_max`). `--map`/`--mapping-file` apply only to files that contain the named headers; two different unknown formats are mapped one file at a time first, and the saved answer is reused in the combined run.

Before/after numbers, optional: `--metrics daily.csv` (repeatable) takes daily campaign or ad group performance exported from the Google Ads UI or API (English or Turkish headers). For each change on a matching campaign/ad group the report shows impressions, clicks, cost, conversions and conversion value for `--window-days` (default 7) days before and after, the raw and % difference as plain numbers, the after-window days within the last `--conversion-lag-days` (default 7) of the metrics' last day marked as maturing, and the other changes in the same campaign and window. A missing metric stays empty, never 0; a day present in two metrics files counts once, and campaign totals are never summed from ad-group rows. Nothing labels a difference as better or worse.

A UI export can be fed as downloaded: the title lines above the header row are skipped, `;`- and tab-separated files are detected, and spelled-out month names (`10 Kas 2026`, `Nov 10, 2026, 2:15 PM`) are read without asking.

`query --since 7d` counts back from now; add `--since-anchor newest` to count back from the newest row in the file. The anchor used is printed with the results.

When the input has a column the skill doesn't recognize, an ambiguous date/number format, or an uncategorized combination of resource type + field + operation, it exits non-zero with a structured JSON `status` instead of guessing — read that JSON, answer the question it's asking, and re-run with the flag it names (`--map`, `--date-format`, `--decimal-style`, `--account-name`, `--extra-rules`, or `--force-review`). Exit codes: 0 ok, 2 a question or an input error, 3 an internal error (a bug — the JSON says so, never a bare traceback). A successful run ends with a `data_quality` block: how many input rows are in the report, how many were left out and why, and how many are in it with a limitation (no timezone, categorized "Other", …).

## Output

`changes.jsonl` (one row per canonical change), `change_history.json` (aggregated, with `flags`, `rules_config`, `exec_summary` and `campaign_table`), `digest.md`, a single-file `dashboard.html`, `coverage.txt`, `run_status.json` (whether the files next to it come from a run that reached ok — a run that stops clears the previous run's outputs first), and `unknown-fields.json` when applicable. The tool refuses to write through a symbolic link in the output folder.

**dashboard.html**: one file, fully offline (fonts and scripts embedded, no network requests), three pages in a floating tab bar:

1. **Summary**: a headline and two or three factual sentences, the period, sources, time zone and run time, four numbers (changes, campaigns changed, people and automation, flags worth a look), the *Worth a look* list (one card per rule with hits: count, title, what it checks, up to two example lines; attention rules first, info rules after), who changed what, budget movement, the campaigns untouched longest, and the coverage notes. It prints on one A4 page.
2. **Details**: a numbered side list: Campaigns (changes, budget changes, last change and flag tags; click a row to see its changes), Worth a look (every flag grouped by rule, each linked to its changes, with the thresholds used), Activity Timeline (click a day to filter; with metrics, a small neutral clicks or cost line), People, Categories, Before / after (with `--metrics`), Coverage and data.
3. **All changes**: the sortable, searchable Change Explorer with every filter (including source file), a detail dialog with before/after, a *Worth a look* filter by rule and a flags column, the optional Before/After column with `--metrics`, and the *Try your own thresholds* panel.

Pages have their own addresses (`#summary`, `#details`, `#all`, or `#ozet`, `#detaylar`, `#tum`), so back/forward work and a flag or campaign on the summary opens its changes on page 3. `--lang` picks Turkish or English for every label; Turkish shows dates as dd.mm.yyyy HH:MM and Turkish category names.

**digest.md**: a short Markdown summary to paste into Slack or e-mail: period and source files, totals, a *Worth a look* section (each rule that fired, its count, the threshold used and example facts), changes per category, per user (masked with `--mask-users`), per account and per ISO week, the ten most-changed campaigns, campaigns whose last change is 14 or 30+ days before the newest row, coverage notes and one data-quality line. Turkish for a Turkish export, English otherwise; `--lang tr|en` (or its alias `--digest-lang`) overrides it.

**Coverage notes**: every run lists, in `meta.coverage_notes`, the dashboard and the digest, what its sources cannot show: API change history covers 30 days, does not return Google Ads Editor changes and may miss auto-applied changes, a UI export and API data can differ, and recent conversions are still maturing.

One row is one change statement: a summary cell that bundles several statements is split into one row each, sharing an `event_id`. Rows are never dropped as duplicates within one file; only identical rows coming from overlapping files collapse, and the count is reported.

The dashboard stays one file at any size: the rows are embedded column-packed (about a quarter of the plain-JSON size — 25,000 changes is roughly 9 MB). *Print → Save as PDF* gives the summary on one A4 page, and the change table from the all-changes page.

**Flags**: the twelve rules run with every report; their defaults, what each one cannot see, and how to change them are in [`references/rules.md`](skills/google-ads-change-history-dashboard/references/rules.md). A flag always shows the fact (old and new value, %, count, minutes or days) and the rule that fired, and links to the changes behind it. The tier (attention or info) only orders the list.

**Try your own thresholds**: on the all-changes page, collapsed. User-adjustable magnitude thresholds (Budget/Target CPA/Target ROAS/Bid ±%) and structural rules (Campaign paused, Campaign removed, Ad group removed — each independently toggleable, plus an off-by-default Campaign enabled for the reverse), computed entirely in the browser — no re-run needed to try a threshold. A match means "crossed the threshold you set," shown with the exact number and rule next to it, never a bare badge or severity color. It does not change the flags written by the run; to change those, re-run with `--rules-config`.

## Privacy

`--mask-users` (default off) replaces each person with `User A`/`User B` (the same label on every later run) and scrubs e-mail-shaped text and the known identities from every string field of every output file, including the summary text and old/new values. The same labels appear in flags and in the summary's people list. It does **not** mask account, campaign or ad group names, IDs or resource paths — a report that has to hide whose account it is needs more than this flag. The `--known-users` list names real people: keep it outside the repo. The identity-to-label table (`mask-labels.json`) and learned column mappings are kept in a per-user data folder (`$XDG_DATA_HOME` or `~/.local/share/google-ads-change-history-dashboard/`), outside the repo; don't share them. `needs_mapping` sample values shown in chat have e-mail addresses redacted.

## Evaluation

`python3 evals/run_evals.py` scores the skill against hand-labelled cases in `evals/cases/` (synthetic, invented names): per-statement category and operation accuracy, exact old/new value matches, row counts with nothing dropped, per-rule flag precision and recall where a case lists its expected flags (gated at 1.0; a skill that writes no flags fails such a case), and a lexical check that `dashboard.html`, `digest.md` and the generated flag and summary sentences contain no judgement words (English and Turkish: "better", "risky", "error", "anomali", "kötü"…). It fails below each case's thresholds and runs in CI. `--private-dir PATH` (default: `evals-private/` in the per-user data folder) adds private cases built from real exports with the same layout; they are never committed. See [`evals/README.md`](evals/README.md).

## How this repo is organized

The skill's code is one file: [`skills/google-ads-change-history-dashboard/ads_change_history.py`](skills/google-ads-change-history-dashboard/ads_change_history.py) — canonical schema, source detection, category rules, the pipeline, the dashboard template, built-in synthetic sample data, and the self-test suite that *is* the test framework. [`SKILL.md`](skills/google-ads-change-history-dashboard/SKILL.md) is the short workflow Claude follows; the details it points to are in [`references/`](skills/google-ads-change-history-dashboard/references/) (CLI, statuses, categories, flag rules, metrics, privacy). See [`CONTRIBUTING.md`](CONTRIBUTING.md) before making a change. `docs/` holds the self-audit history (rounds 1-5 and round 7) plus real-usage dogfooding notes — every confirmed finding, how it was reproduced, and how it was fixed. [`docs/ROUND-10-FEATURES.md`](docs/ROUND-10-FEATURES.md) lists the round-10 features and [`docs/ROUND-12.md`](docs/ROUND-12.md) the round-12 flags and three-page dashboard. Later rounds are recorded where the fixes are: in the commit messages and in the dated notes beside the code and its test cases.

## License

MIT — see [`LICENSE`](LICENSE).

---

Built by [Ali Demirbaş](https://alidemirbas.com.tr)
