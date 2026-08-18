# google-ads-change-history-dashboard — who changed what, when, factually

[![validate](https://github.com/ali-demirbas/google-ads-change-history-dashboard/actions/workflows/validate.yml/badge.svg)](https://github.com/ali-demirbas/google-ads-change-history-dashboard/actions/workflows/validate.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
![Single file](https://img.shields.io/badge/skill-single_file-blue)
![Tests](https://img.shields.io/badge/self--tests-57_passing-brightgreen)

A Claude Code skill that turns a Google Ads change-history export (CSV, TSV, or a pre-flattened `ChangeEvent` JSON) into a filterable, offline, single-file HTML dashboard — who changed what, in which account/campaign/ad group, old value to new value, and what category it falls into.

**It never judges.** It states "Campaign X hasn't changed in 23 days" — never "the agency is neglecting Campaign X." Deciding whether a change was good, risky, or overdue is explicitly out of scope for this version.

## What it answers

- Who changed this campaign's budget last week, and what was it before?
- What changed in the last 7 days / 24 hours, across which accounts?
- Which campaigns haven't been touched in 30+ days?
- Was this change made by a person or an automation (a script, a bidding rule, a Recommendation)?
- Which category of change is most common right now — budget, bidding, status, keywords?

## What it won't do

- Won't tell you if a change was a *good* one.
- Won't read XLSX or a Google Sheets URL directly, and won't call the Google Ads API live — export to CSV/JSON first.
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

When the input has a column the skill doesn't recognize, an ambiguous date/number format, or an uncategorized combination of resource type + field + operation, it exits non-zero with a structured JSON `status` instead of guessing — read that JSON, answer the question it's asking, and re-run with the flag it names (`--mapping-file`, `--date-format`, `--decimal-style`, or `--allow-unknown-categories`).

## Output

`changes.jsonl` (one row per canonical change), `change_history.json` (aggregated), a single-file `dashboard.html` (works fully offline, no CDN — Filters, Summary, Activity Timeline, User Activity split by human/automation, Account/Campaign drill-down, Category Distribution, Rule Matches, Campaign Last Changes, and a sortable/searchable Change Explorer with a before/after detail panel), `coverage.txt`, and `unknown-fields.json` when applicable.

**Rule Matches** — off by default. A visible toggle turns on user-adjustable magnitude thresholds (Budget/Target CPA/Target ROAS/Bid ±%) and structural rules (Campaign paused, Campaign removed, Ad group removed — each independently toggleable, plus an off-by-default Campaign enabled for the reverse), computed entirely in the browser — no re-run needed to change a threshold. It never judges: a match means "crossed the threshold you set," always shown with the exact number and rule next to it, never a bare badge or severity color. A Filters dropdown lets you narrow the Explorer to matched rows, or to one specific rule.

## Privacy

`--mask-users` (default off) replaces human user names/emails with `User A`/`User B` for external sharing — labels are persisted across runs so the same person keeps the same label. Account and campaign names are never masked; that's your own data.

## How this repo is organized

The entire skill is one file: [`skills/google-ads-change-history-dashboard/ads_change_history.py`](skills/google-ads-change-history-dashboard/ads_change_history.py) — canonical schema, source detection, category rules, the pipeline, the dashboard template, built-in synthetic sample data, and the self-test suite that *is* the test framework. See [`CONTRIBUTING.md`](CONTRIBUTING.md) before making a change. `docs/` holds five rounds of self-audit history plus real-usage dogfooding notes — every confirmed finding, how it was reproduced, and how it was fixed.

## License

MIT — see [`LICENSE`](LICENSE).
