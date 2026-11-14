# Round 10 features (2026-11-14)

Facts-only rule unchanged: every new output states numbers and neutral words,
never whether a change was good or bad.

| # | Feature | Where it shows |
|---|---|---|
| 3 | Coverage notes: what the sources cannot show (API 30-day window, Editor and auto-applied changes, UI vs API differences, conversion lag) | `meta.coverage_notes`, dashboard panel, `digest.md` |
| 4 | `digest.md`: Markdown tables (period, sources, totals, per category / user / account / ISO week, top 10 campaigns, campaigns with no change in the last 14 / 30 days of the data, coverage notes, data quality); `--digest-lang auto\|tr\|en` | out folder |
| 5 | Live fetch tool on Google Ads API v26 (sunset November 2027), `google-ads>=34.0.0` | `tools/` |
| 6 | `SKILL.md` cut to a numbered workflow with explicit stop-and-ask steps; details in `references/` (`cli`, `statuses`, `categories`, `metrics`, `privacy`) | skill folder |
| 7 | Before/after metrics: `--metrics PATH` (repeatable), `--window-days` (7), `--conversion-lag-days` (7); before/after windows per change, maturing days, overlapping changes, missing = null | `metrics_meta`, `before_after`, dashboard detail strip and column |
| 8 | Activity Timeline day markers: click a day to filter the Explorer, a clearable day chip, optional neutral clicks/cost line | dashboard |
| 9 | Several inputs per run, `--input-account FILE=NAME`, `source_file` on every row, cross-file duplicates kept once and counted, `meta.sources` | pipeline, dashboard source filter and account rollup |
| 13 | Evaluation set: `evals/run_evals.py`, synthetic cases with statement-level truth and thresholds, never-judge and no-data-loss checks, optional private cases | `evals/`, CI |

Single-file inputs, flags and outputs from earlier rounds behave as before.
