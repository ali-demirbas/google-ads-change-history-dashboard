# Round 12: flags worth a look, three-page dashboard (2026-11-19)

The "never judge" promise becomes a wording rule. A flag is a fact measured
against a threshold the user sets; the report says "worth a look" ("göz
atılacak"), shows the fact and the rule that fired, and never says good, bad,
wrong, risky, error or anomaly, with no red/green.

| Area | What changed | Where it shows |
|---|---|---|
| Rules | Twelve rules in two tiers (attention, info) with defaults; `--rules-config FILE.json`, `--known-users FILE`, `--work-hours 08-20`; a rule that cannot run is listed as off with its reason | `flags`, `rules_config` in `change_history.json`; `references/rules.md` |
| Summary data | Totals, people, budget movement, longest-untouched campaigns, top campaigns, a headline and 2-3 factual sentences from templates; one row per campaign with its flags | `exec_summary`, `campaign_table` |
| Dashboard | Three pages (Summary, Details, All changes) with hash routes and links from a flag or campaign to its changes; the in-browser threshold panel moves to page 3 as *Try your own thresholds*; fonts embedded, no network requests; summary prints on one A4 page | `dashboard.html` |
| Language | `--lang auto\|tr\|en` for the dashboard and the digest (`--digest-lang` kept as an alias); Turkish dates, numbers and category names | dashboard, `digest.md` |
| Digest | A *Worth a look* section | `digest.md` |
| Evals | Optional `flags` expectations per case, per-rule precision/recall gated at 1.0, a missing `flags` list fails the case; the wording check also covers generated flag and summary sentences; new synthetic case `anomaly_rules` | `evals/` |

The rows of `changes.jsonl` keep their schema. Single-run commands from
earlier rounds behave as before.
