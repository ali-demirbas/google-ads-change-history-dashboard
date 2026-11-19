## What this changes

## Checklist

- [ ] `python3 ads_change_history.py self-test` passes locally (run from `skills/google-ads-change-history-dashboard/`)
- [ ] Any new behavior has a matching `_test_case(...)` block in the self-test — this repo has no separate test framework, the self-test *is* the test suite
- [ ] No real account, campaign, or user data — synthetic only (Account A/B, Campaign Alpha/Beta, User A/B/C), including in commit messages
- [ ] No judgmental output (a score, a grade, a "good/bad", "error", "risky" or "anomaly" label, red/green); a flag states a fact, its threshold and the rule that fired, and says "worth a look"
- [ ] `python3 evals/run_evals.py --synthetic-only` passes; a new or changed flag rule has hand-written `flags` truth in an eval case
- [ ] `mapping-profiles/*.json` not committed (gitignored — it's generated runtime state)
