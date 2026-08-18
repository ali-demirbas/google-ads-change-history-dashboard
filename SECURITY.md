# Security Policy

## Supported versions

This project is pre-1.0. Only the latest commit on `main` is supported; there are no maintained release branches yet.

## Reporting a vulnerability

Please report security issues privately through [GitHub Security Advisories](https://github.com/ali-demirbas/google-ads-change-history-dashboard/security/advisories/new) rather than opening a public issue.

Include what you'd include in any report: the affected file, how to reproduce, and the impact you think it has.

## Scope

This repo has no server and no hosted service — it's a single Python script that reads a local file and writes a local, self-contained HTML dashboard. It never calls the Google Ads API or any network endpoint. The realistic attack surfaces:

- **XSS in the generated dashboard.** Campaign names, user names, and other source-derived text are attacker-controlled if the input file itself is untrusted (e.g. shared with you by someone else). All such text is escaped before being written into the dashboard's HTML — a crafted campaign name like `<img src=x onerror=...>` should never execute. A case where it does is in scope. (`self_test()` has a dedicated regression case for this — script-context JSON escaping and innerHTML escaping are two separate mechanisms and both need to hold.)
- **Malformed input causing an unhandled crash instead of a structured error.** `read_rows()`/`validate_source()` are meant to turn every malformed-input case (bad encoding, invalid JSON, missing required columns, non-object JSON rows) into a clear `{"status": "error", ...}` response, never a raw Python traceback. A reproducible case that still crashes is in scope.
- **Data leakage across `--mask-users` runs.** Masking is meant to be consistent across every output file (`changes.jsonl`, `change_history.json`, `dashboard.html`) and stable across separate runs (labels persist in `mapping-profiles/mask-labels.json`). A case where a masked run still leaks a raw email/name anywhere is in scope.

Issues that only affect your own local run with your own trusted data (e.g. a wrong category assignment, a parsing edge case) are bug reports, not security issues — file those as a normal issue instead.

## Response

This is a solo-maintained open-source project. There's no SLA, but reports will be acknowledged and triaged as soon as reasonably possible.
