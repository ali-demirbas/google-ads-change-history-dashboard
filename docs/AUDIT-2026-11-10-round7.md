# Audit round 7 (2026-11-10)

Two passes over the skill, the dashboard, the query CLI, the fetch tool and the docs, with synthetic fixtures plus one real native export kept outside the repo. Every finding below was reproduced before it was fixed. Each fix has a self-test case.

## Found and fixed

**Reading files**
- A native UI export, as downloaded, stopped at `needs_mapping`: title lines above the header, an unknown date-and-time header, and Turkish month names were not read. All three are handled; a spelled-out month is never asked about as DD/MM.
- UTF-16, `;`-separated and tab-separated files dead-ended; non-UTF-8 files could turn into mojibake with status ok. Now detected, with `--encoding` for the rest. XLSX/ZIP/GZIP stop with "export as CSV".
- Header and date shapes of de/es/fr/pt/it/nl exports, 12-hour times and dates with a comma were not read.
- A summary column could also be taken as the operation column.
- Learned mappings were written next to the script (fails on a read-only install). They now live in a per-user data folder and are saved only after an ok run.

**Meaning of the rows**
- Distinct changes in the same second, same campaign, were dropped as duplicates. Rows within one file are never dropped now.
- A cell bundling several statements got one category from its first line. It is split into one row per statement.
- Legacy "status changed A -> B" was categorized by the old value. Many Turkish detail lines (bidding targets, schedules, assets, audiences, budgets) fell to "Other" or the wrong category.
- `--mask-users` left e-mail addresses inside summary text and values. Every string field is scrubbed now.
- Automation detection matched ordinary surnames. It uses word boundaries now.
- The timeline grouped by UTC day; it uses the local day when the time zone is known.

**Dashboard**
- The campaign drill-down set a text search rather than a filter. Filters had no labels and the detail panel was not a dialog. Search skipped the summary text and wasn't Turkish-aware. The rule filter stayed on with highlighting off. Old/new values were coloured red/green. Tables overflowed on a phone.

**CLI and fetch tool**
- `query --since` counted back from the newest row, silently. It now counts from now, with `--since-anchor newest` and the anchor printed. Matching is Turkish-aware. A missing or malformed input file gives a JSON error.
- An unexpected exception printed a traceback. It is now `status: internal_error`, exit 3. Output is UTF-8 on any console.
- `--open` built an unencoded URL and reported nothing. It uses a file URI and reports `opened`.
- A symlink in the output folder was written through. It is now refused. A failed run left the previous dashboard looking current. It clears the old outputs and writes `run_status.json`.
- The fetch tool serialized enums as integers (the library default), and `--days` other than 7/14/30 built an invalid query that also skipped today. Both fixed. The output file is 0600, and zero events give an empty, valid report.
- Three code mutations survived the tests (the empty-column gate, a structured budget rule, `new_value` in `change_id`). All three are caught now. An `assert True` was replaced with real checks of the Turkish value phrases.
- `needs_mapping` sample values could show e-mail addresses in chat. Redacted now.
- CI ran Python 3.12 only, could skip the JS suite and failed on a date check. It now runs a 3.9 + 3.12 matrix, requires Node, validates the plugin manifests and pins actions by SHA. The sunset check is a warning.
- The docs overstated masking, language support and test counts, and SECURITY.md left out the fetch tool's network use. Corrected; the test-count badge is gone.

## Knowingly left

- Categorization for languages other than English and Turkish: headers and dates are read, but rows stay mostly "Other" with a warning. Patterns come from real rows only.
- `change_id` is an unsalted content hash, so it's documented as not anonymous.
- The fetch tool's handling of nested old/new resources is still checked against fakes shaped like the library, not against a live account.
