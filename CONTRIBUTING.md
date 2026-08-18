# Contributing

## The skill is one file

`skills/google-ads-change-history-dashboard/ads_change_history.py` is the entire skill — canonical schema, header-alias table, category rules, the pipeline (validate → normalize → categorize → aggregate), the dashboard HTML/CSS/JS template, built-in synthetic sample fixtures, and the self-test suite, all in one file, by design. `SKILL.md` in the same folder is the short trigger/usage doc; the module docstring at the top of `ads_change_history.py` is the actual spec.

Don't split it into multiple files or add a build step — that's a deliberate constraint, not an oversight.

## There is no separate test framework

`self_test()`, at the bottom of `ads_change_history.py`, is the entire test suite — built-in synthetic fixtures, no pytest, no external dependency. Run it with:

```bash
cd skills/google-ads-change-history-dashboard
python3 ads_change_history.py self-test
```

Every behavioral change needs a matching assertion added to `self_test()`, in the same style as the existing ones: a small synthetic fixture, a `run_pipeline()`/`validate_source()` call, an assertion with a message naming what would break and why, and a `print(f"[PASS] ...")` line describing what was verified. Look at an existing block before adding a new one.

## Adding a source format or column alias

`HEADER_ALIASES["aliases"]` — add the new header string to the relevant canonical field's list. If it's a genuinely new, recognizable source shape (not just one new column), also add an entry to `HEADER_ALIASES["known_sources"]` so `detect_known_source()` can name it instead of falling back to `"alias_match"`.

## Adding a category rule

`CATEGORY_RULES["structured_rules"]` — a `(resource_type, field_name, operation)` combination mapped to a `(category, subcategory)`. Rules are matched in order, first match wins — a more specific rule must come before a more general one that would otherwise shadow it (see the comment above the `CAMPAIGN`/`AD_GROUP` REMOVE rules for a worked example of this exact failure mode and its fix).

If the phrasing is for a text-only source (no separate `field_name` column, e.g. the legacy CSV format), it goes in `CATEGORY_RULES["summary_text_rules"]` instead — a regex over `raw_summary`.

## The "never guess" rule

The skill stops and returns a structured `needs_*` status rather than guessing at a genuine ambiguity (unrecognized header, ambiguous date format, ambiguous decimal separator, unrecognized category combination). Don't add a new "just default to X" branch — if something is genuinely ambiguous, it should stop and ask, matching the existing pattern.

## The "never judge" rule

V1 is strictly factual — it states "Campaign X hasn't changed in 23 days," never "the agency is neglecting Campaign X." No scores, no grades, no red/green good-bad framing on top of a change. A PR that adds evaluative output is out of scope for this version; open an issue to discuss it as a deliberate V2 instead.

## No real data, ever

Every fixture, example, and test case in this repo uses synthetic data only — `Account A`/`Account B`, `Campaign Alpha`/`Beta`/`Gamma`/`Delta`, `User A`/`B`/`C`, `*.test` email domains. This applies to commits, issues, and PRs too — never paste a real company name, real campaign name, or real account data anywhere in this repo.

## Before opening a PR

```bash
cd skills/google-ads-change-history-dashboard
python3 -c "import ads_change_history"   # syntax/import check
python3 ads_change_history.py self-test  # full suite
```

Both run in CI (`.github/workflows/validate.yml`) on every PR.
