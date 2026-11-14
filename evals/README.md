# Evaluation set

Measures how well the skill reads change-history exports, statement by statement,
against labels written by hand.

```
python3 evals/run_evals.py                  # synthetic cases + private cases, if any
python3 evals/run_evals.py --synthetic-only # what CI runs
python3 evals/run_evals.py --case tr_native_preamble --verbose
python3 evals/run_evals.py --skill path/to/ads_change_history.py   # evaluate another copy
```

Exit 0 when every case ran and met its thresholds, 1 otherwise. A SKIPPED case fails the run
unless `--allow-skip` is passed. Each case runs with its own empty `XDG_DATA_HOME`, so saved
profiles on the machine cannot change the result.

## What is scored

For each case the skill runs into a temporary folder. Output rows are matched to
`truth.json` events by timestamp and first statement line (in event order when
these repeat), then by `statement_index`. The runner reports:

| Measure | Meaning |
|---|---|
| category accuracy | share of statements with the labelled category |
| subcategory accuracy | same for the subcategory, where the label gives one |
| operation accuracy | CREATE / UPDATE / REMOVE as labelled |
| value match rate | `old_value_num` / `new_value_num` equal the labelled numbers (`null` must stay `null`) |
| actor accuracy | `actor_type`, where an event is labelled with one |

The run also fails when the counts in `data_quality` do not add up, any row is left out
(other than declared cross-file duplicates), `rows_in_report` differs, an expected
warning is missing, or a judgement word (English or Turkish, e.g. "better",
"improved", "iyi", "kötüleşti") appears in the dashboard's own text or in `digest.md`.
Data copied from the input is ignored by that check.

## Case layout

```
cases/<name>/
  <input files>
  args.json    {"inputs": [...], "args": [CLI flags], "requires": ["multi_input" | "metrics"]}
  truth.json   {"expected": {...}, "thresholds": {...}, "events": [{"ts", "statements": [...]}]}
```

`statements` entries hold `head` (first line), `category`, `subcategory` (optional),
`operation`, and `old` / `new` where the text states a number. `expected` holds
`rows_in_report`, `dropped` (always 0), and optionally `duplicate_across_files`,
`data_quality`, `warnings_contain`, `meta` and `before_after`.

A case whose `requires` names a feature the installed skill rejects is reported as
SKIPPED, with the reason. Skipped cases do not count as passed and fail the run unless `--allow-skip`.

## Synthetic cases

All names are invented.

| Case | Covers |
|---|---|
| tr_native_preamble | Turkish UI export: preamble, multi-statement cells, Turkish month names, TL amounts, bid adjustments, asset status, Hedef EBM, a created campaign with budget and Hedef EBM lines |
| en_native_preamble | English UI export: AM/PM dates, budgets, target CPA, assets, keywords, a created campaign with budget and Target CPA lines |
| bulk_same_second | one bulk edit across ad groups in one second, plus one exact duplicate row that must be kept |
| automation_users | auto-applied recommendations, rules, scripts, an API service account, two people |
| multi_file_mcc | three files, two accounts, two rows exported twice |
| metrics_before_after | daily campaign metrics, before/after sums worked out by hand |
| unsupported_language | Spanish export: every statement must be Other (gated at 1.0) plus a data_quality warning |

## Private cases

`--private-dir` (default `$XDG_DATA_HOME/google-ads-change-history-dashboard/evals-private`,
else `~/.local/share/...`) holds cases built from real exports, with the same layout.
These are never committed. For private cases the runner prints only the scores and
counts, not the mismatched text.
