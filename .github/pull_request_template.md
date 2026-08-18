## What this changes

## Checklist

- [ ] `python3 ads_change_history.py self-test` passes locally (run from `skills/google-ads-change-history-dashboard/`)
- [ ] Any new behavior has a matching `self_test()` regression assertion — this repo has no separate test framework, the self-test *is* the test suite
- [ ] No real account, campaign, or user data — synthetic only (Account A/B, Campaign Alpha/Beta, User A/B/C), including in commit messages
- [ ] No new judgmental output (a score, a grade, a "good/bad" label) — V1 stays strictly factual
- [ ] `mapping-profiles/*.json` not committed (gitignored — it's generated runtime state)
