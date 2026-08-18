# Forensic Audit — Round 5 — Resolution Log

Fifth audit round, the first one aimed specifically at a just-shipped
feature (Rule Matches, Phase 1) rather than the whole codebase — checking
both that it works and that it hasn't quietly broken the tool's "never
judge" principle. All 3 confirmed findings below were independently
reproduced against the real code before being fixed, same standing
practice as rounds 1–4.

**Full self-test: 57/57 passing** (1 new top-level `[PASS]` line — a
16-assertion Node-executed regression suite covering the new client-side
logic; see "No regression coverage" below for why this needed a different
mechanism than the other 56).

## Confirmed and fixed

| Finding | Verified | Fix | Re-verified |
|---|---|---|---|
| **Rule Matches wasn't a real filter.** Turning it on added a separate section and an Explorer column, but the Filters row had no way to narrow the Change Explorer itself to matched rows or to one specific rule — a user asking "show me just the budget changes over 50%" had to scroll to a second table instead of using the Explorer they already know | Reproduced: `getFiltered()` had no `state.ruleMatch` field and no corresponding filter branch — confirmed by reading the function directly | Added a `Rule Match` dropdown to Filters (`All changes` / `Matched a rule` / one option per rule) wired into `getFiltered()`. Picking a specific rule auto-enables Rule Matches highlighting if it was off, so the Explorer's Matches column never shows a filtered-but-unexplained `—` | Reproduced in a live dashboard: selecting "Budget change" with the threshold lowered to 10% dropped the Explorer from 10 to 3 rows, all three showing the `Budget change` pill; Clear Filters resets the dropdown without touching the separate "Configure rules" toggle |
| **State-rule match detail wasn't as factual as magnitude's.** Magnitude always shows the real number (`+72% · rule: ≥±50%`); the three state rules just echoed their own label back (`rule: Campaign paused`) — true, but not the same "here's what was actually observed" standard the rest of the feature holds itself to | Confirmed via code read: `STATE_RULES` had no equivalent of magnitude's `pctChange` computation | Added a `fact(c)` to each state rule: `ENABLED → PAUSED · rule: Campaign paused` when the source has explicit before/after values, falling back to `status → PAUSED` for a text-summary source that only ever said "N campaigns paused" with no per-row values; `Operation: REMOVE · rule: Campaign removed` / `... Ad group removed` | Reproduced via the new Node regression suite: both fixtures assert the enriched detail string, not just that a match occurred |
| **No regression coverage for the new client-side matching logic.** `MAGNITUDE_RULES`/`STATE_RULES`/`computeRuleMatches()`/the new filter branch are dashboard-template JS, invisible to `self_test()`'s `run_pipeline()`/`validate_source()`-based assertions — a real gap of the same *class* (JS silently diverging from what Python assumes) this repo has hit before via dogfooding, just inverted (JS untested rather than JS not mirroring a Python fix) | Confirmed: no assertion anywhere touched `computeRuleMatches` or the rule arrays; manually verified this session's initial implementation via a live browser instead, which caught a real bug (see below) that a regression test would have caught permanently | Added `_run_js_rule_matches_regression()` — extracts `DASHBOARD_TEMPLATE`'s `<script>` verbatim and runs a 16-assertion suite under Node (boundary values, `old_value_num == 0`, CREATE never matching a magnitude rule, the PAUSED/ENABLED asymmetry, the pause-ambiguity text fallback, live threshold changes, per-rule on/off, both new filter behaviors). Skips with `[SKIP]` (not a failure) when `node` isn't on `PATH` — the one exception to "no dependencies beyond the Python standard library," and only for this one check | Reproduced: 16/16 pass with Node present; the whole suite (56 other tests + this one) still reports "All self-tests passed" with Node absent from `PATH` |

## A bug this audit's own verification step caught, not the audit itself

While reproducing the findings above, testing the *original* implementation
against this repo's own bundled sample data (not a pasted claim) surfaced a
real, separate defect: `MAGNITUDE_RULES`/`STATE_RULES` were written to match
on `field_name`/`resource_type`, but those two are only populated for
structured (field-column) sources — a text-summary source (this repo's own
Turkish legacy sample, and the real native Google Ads UI export documented
in `DOGFOODING-NOTES.md`) leaves both `null`. Every rule silently matched
nothing on the bundled sample. Fixed by keying all rules on
`category`/`subcategory` instead — the one signal `categorize_changes()`
always resolves regardless of source shape — with a `raw_summary` fallback
specifically for disambiguating "campaign paused" from "ad group paused"
from "keyword paused" text, three phrasings that collapse to the same
`subcategory: "Paused"` when no `resource_type` column exists.

## Noted, not applied — a product decision, not a confirmed bug

- **Should `Campaign enabled` (PAUSED→ENABLED) be offered as an optional,
  default-off state rule alongside `Campaign paused`?** The audit's
  argument — that a two-month-dormant Brand campaign getting re-enabled can
  matter as much operationally as a pause — is reasonable and doesn't
  contradict "never judge" (a default-off checkbox judges nothing; the user
  ticking it is the one deciding it's worth watching). Left unimplemented
  this round: it wasn't one of the three items the audit asked to be acted
  on, and the existing PAUSED→ENABLED asymmetry was a deliberate Phase 1
  scope decision from the original design review, not an oversight. Worth
  revisiting alongside the already-planned V1.1 (Reversal)/V2 (Burst) work
  rather than folding in unreviewed.

## What changed in scope during this round

Only the 3 confirmed items above, plus the bug this round's own
verification surfaced while checking finding #3. Burst/activity-pattern
rules and same-field reversal detection remain out of scope, per the
original Phase 1 design review.
