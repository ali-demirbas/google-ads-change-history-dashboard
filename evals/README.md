# Evaluation set

Measures how well the skill reads change-history exports, statement by statement,
and which flags it raises, against labels written by hand.

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
| flag precision / recall | per rule, where the truth lists `flags`: flags raised that match the truth / flags raised, and matched / expected. The case scores the lowest rule; both are gated at 1.0 |

The run also fails when the counts in `data_quality` do not add up, any row is left out
(other than declared cross-file duplicates), `rows_in_report` differs, an expected
warning is missing, or a judgement word (English or Turkish, e.g. "better",
"improved", "iyi", "kötüleşti", and for flags "error", "wrong", "anomaly", "hata",
"anomali") appears in the dashboard's own text, in `digest.md` or in the sentences the
skill writes itself (flag texts, rule titles and descriptions, the summary headline and
sentences). Data copied from the input is ignored by that check; in the dashboard's
script, "error" is left to code.

## Flags

A case may list the flags it expects in `expected.flags`. A case that lists them fails
when `change_history.json` has no `flags` list (it is not skipped). For each listed rule:

```json
{"rule": "budget_jump", "count": 2, "match": [{"changes": ["38.0"]}, {"changes": ["33.0"]}]}
{"rule": "stale_entity", "count": 1, "match": [{"campaign": "Demo - A", "ad_group": null}]}
{"rule": "off_hours", "unit": "events", "count": 2, "events": [7, 8]}
{"rule": "reverted", "count": 0}
```

- `count`: flags of that rule the skill must raise. A rule not listed is not scored.
- `match` (optional, one entry per expected flag): every `changes` ref
  (`"<event>.<statement>"`, truth indices) must be among the flag's `change_ids`;
  `campaign`, `ad_group`, `account`, `user` must equal the flag's field. Each output flag
  matches one entry at most.
- `unit: "events"`: scores the set of events covered by the rule's flags instead of the
  number of flags (for rules where one flag per event or per group are both fine).

Also checked when `flags` is listed: each flag has a known rule, the contract's tier,
`text.tr` and `text.en`, `change_ids` that exist in `changes.jsonl`; flags are sorted
attention first; `rules_config` lists the 12 rules in order; `expected.rules.params`
match the parameters used and `expected.rules.disabled` rules are off with a reason;
`exec_summary.totals.attention_flags` / `info_flags` equal the flags per tier; and any
`expected.exec_summary` numbers (`decrease_total` compared as a magnitude).

Truth is written by hand from the rule definitions in
`skills/google-ads-change-history-dashboard/references/rules.md`, never copied from the
skill's output. Where a definition leaves a case open, the rule is left out of that
case's truth until the definition is settled.

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
`data_quality`, `warnings_contain`, `meta`, `before_after`, `flags`, `rules` and
`exec_summary`. Extra files a case passes (`--known-users`, `--rules-config`) sit in the
case folder.

A case whose `requires` names a feature the installed skill rejects is reported as
SKIPPED, with the reason. Skipped cases do not count as passed and fail the run unless `--allow-skip`.

## Synthetic cases

All names are invented.

| Case | Covers | Flags in the truth |
|---|---|---|
| anomaly_rules | Turkish UI export (2-line preamble, Europe/Istanbul) built for the 12 rules: each fires a known number of times, near-misses must not (a campaign setup burst with repeated keyword adds, a 49% budget step, an add/remove 65 minutes apart, a budget back to its old value after 8 days, a target change 9 days after the last, a weekday 08:00 change, a 2-event burst, a campaign 39 days untouched under a 45-day threshold); `--known-users` (mixed case, a comment, a blank line) and `--rules-config` (`stale_entity` 45 days, `bulk_edit` 10 changes) | all 12 rules; budget movement totals |
| tr_native_preamble | Turkish UI export: preamble, multi-statement cells, Turkish month names, TL amounts, bid adjustments, asset status, Hedef EBM, a created campaign with budget and Hedef EBM lines | all 12 (3 stale campaigns and the stale ad group inside one of them, 3 stale budgets, 4 weekend events; three bid-adjustment slots in one save are not a repeat) |
| en_native_preamble | English UI export: AM/PM dates, budgets, target CPA, assets, keywords, a created campaign with budget and Target CPA lines | all 12 (2 Saturday events; no `stale_budget`: a 29-day export cannot show 60 days) |
| bulk_same_second | one bulk edit across ad groups in one second, plus one exact duplicate row that must be kept | all 12 (one burst; the duplicate keyword row is an addition, not a repeat edit) |
| automation_users | auto-applied recommendations, rules, scripts, an API service account, two people | all 12 (4 automation flags; `off_hours` counts the two weekend changes by people, not the automation at night) |
| multi_file_mcc | three files, two accounts, two rows exported twice | all 12, none firing: the same campaign name in two accounts is two campaigns |
| metrics_before_after | daily campaign metrics, before/after sums worked out by hand | all 12 rules, none firing (metrics basis included) |
| unsupported_language | Spanish export: every statement must be Other (gated at 1.0) plus a data_quality warning | all 12 (one Saturday event) |

## Private cases

`--private-dir` (default `$XDG_DATA_HOME/google-ads-change-history-dashboard/evals-private`,
else `~/.local/share/...`) holds cases built from real exports, with the same layout.
These are never committed. For private cases the runner prints only the scores and
counts, not the mismatched text.

The private case built from the real Turkish UI export should carry these flag counts,
measured on that file with the default thresholds (no `--known-users`, no
`--rules-config`): `budget_jump` 5, `budget_cumulative` 1, `reverted` 8, `repeat_edit` 1,
`burst_session` 3 (6 windows without the setup exclusion, 3 of them campaign setups),
`bid_target_relearn` 1, `automation_source` 0, `stale_entity` 2 (1 campaign, 1 ad group,
30 days), `stale_budget` 2, `off_hours` 4 events, `bulk_edit` 1, and `unknown_user` off
(`"rules": {"disabled": ["unknown_user"]}`). Write them as `expected.flags` in that case's
`truth.json` in the private folder, with `count` (and `unit: "events"` for `off_hours`)
only, no names.
