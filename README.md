# google-ads-change-history-dashboard — who changed what, when, factually

[![validate](https://github.com/ali-demirbas/google-ads-change-history-dashboard/actions/workflows/validate.yml/badge.svg)](https://github.com/ali-demirbas/google-ads-change-history-dashboard/actions/workflows/validate.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
![Single file](https://img.shields.io/badge/skill-single_file-blue)

A Claude Code skill that turns a Google Ads change-history export (CSV, TSV, or a pre-flattened `ChangeEvent` JSON) into a filterable, offline, single-file HTML dashboard — who changed what, in which account/campaign/ad group, old value to new value, and what category it falls into.

**It never judges.** It states "Campaign X hasn't changed in 23 days" — never "the agency is neglecting Campaign X." Deciding whether a change was good, risky, or overdue is explicitly out of scope for this version.

## What it answers

- Who changed this campaign's budget last week, and what was it before?
- What changed in the last 7 days / 24 hours, across which accounts?
- Of the campaigns in this export, which had their last change 30+ days ago?
- Was this change made by a person or an automation (a script, a bidding rule, a Recommendation)?
- Which category of change is most common right now — budget, bidding, status, keywords?

## What it won't do

- Won't tell you if a change was a *good* one.
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
python3 ads_change_history.py run <input.csv|.json> --out-dir ./out --open
python3 ads_change_history.py query ./out/changes.jsonl --user "User A" --since 7d
python3 ads_change_history.py self-test
```

Facts only you know, never guessed:

- `--account-name "My account"` for a UI export, which has no account column.
- `--timezone Europe/Istanbul` for timestamps without an offset (UI exports). Without it those rows are left out of the timeline, and the run says how many.
- `--currency TRY` to show budget/bid amounts with their unit, when the text doesn't already carry a currency sign.
- `--encoding cp1254` (or another codec) when the file isn't UTF-8/UTF-16 and the run says it can't read it cleanly.

A UI export can be fed as downloaded: the title lines above the header row are skipped, `;`- and tab-separated files are detected, and spelled-out month names (`10 Kas 2026`, `Nov 10, 2026, 2:15 PM`) are read without asking.

`query --since 7d` counts back from now; add `--since-anchor newest` to count back from the newest row in the file. The anchor used is printed with the results.

When the input has a column the skill doesn't recognize, an ambiguous date/number format, or an uncategorized combination of resource type + field + operation, it exits non-zero with a structured JSON `status` instead of guessing — read that JSON, answer the question it's asking, and re-run with the flag it names (`--map`, `--date-format`, `--decimal-style`, `--account-name`, `--extra-rules`, or `--force-review`). Exit codes: 0 ok, 2 a question or an input error, 3 an internal error (a bug — the JSON says so, never a bare traceback). A successful run ends with a `data_quality` block: how many input rows are in the report, how many were left out and why, and how many are in it with a limitation (no timezone, categorized "Other", …).

## Output

`changes.jsonl` (one row per canonical change), `change_history.json` (aggregated), a single-file `dashboard.html` (works fully offline, no CDN — Filters, Summary, Activity Timeline, User Activity split by human/automation, Account/Campaign drill-down, Category Distribution, Rule Matches, Campaign Last Changes, and a sortable/searchable Change Explorer with a before/after detail panel), `coverage.txt`, `run_status.json` (whether the files next to it come from a run that reached ok — a run that stops clears the previous run's outputs first), and `unknown-fields.json` when applicable. The tool refuses to write through a symbolic link in the output folder.

One row is one change statement: a summary cell that bundles several statements is split into one row each, sharing an `event_id`. Rows are never dropped as duplicates within one file; only identical rows coming from overlapping files collapse, and the count is reported.

The dashboard stays one file at any size: the rows are embedded column-packed (about a quarter of the plain-JSON size — 25,000 changes is roughly 9 MB) and it has a print stylesheet, so the browser's *Print → Save as PDF* gives a landscape report with no clipped columns.

**Rule Matches** — off by default. A visible toggle turns on user-adjustable magnitude thresholds (Budget/Target CPA/Target ROAS/Bid ±%) and structural rules (Campaign paused, Campaign removed, Ad group removed — each independently toggleable, plus an off-by-default Campaign enabled for the reverse), computed entirely in the browser — no re-run needed to change a threshold. It never judges: a match means "crossed the threshold you set," always shown with the exact number and rule next to it, never a bare badge or severity color. A Filters dropdown lets you narrow the Explorer to matched rows, or to one specific rule.

## Privacy

`--mask-users` (default off) replaces each person with `User A`/`User B` (the same label on every later run) and scrubs e-mail-shaped text and the known identities from every string field of every output file, including the summary text and old/new values. It does **not** mask account, campaign or ad group names, IDs or resource paths — a report that has to hide whose account it is needs more than this flag. The identity-to-label table (`mask-labels.json`) and learned column mappings are kept in a per-user data folder (`$XDG_DATA_HOME` or `~/.local/share/google-ads-change-history-dashboard/`), outside the repo; don't share them. `needs_mapping` sample values shown in chat have e-mail addresses redacted.

## How this repo is organized

The entire skill is one file: [`skills/google-ads-change-history-dashboard/ads_change_history.py`](skills/google-ads-change-history-dashboard/ads_change_history.py) — canonical schema, source detection, category rules, the pipeline, the dashboard template, built-in synthetic sample data, and the self-test suite that *is* the test framework. See [`CONTRIBUTING.md`](CONTRIBUTING.md) before making a change. `docs/` holds the self-audit history (rounds 1-5 and round 7) plus real-usage dogfooding notes — every confirmed finding, how it was reproduced, and how it was fixed. Later rounds are recorded where the fixes are: in the commit messages and in the dated notes beside the code and its test cases.

## License

MIT — see [`LICENSE`](LICENSE).

---

Built by [Ali Demirbaş](https://alidemirbas.com.tr)
