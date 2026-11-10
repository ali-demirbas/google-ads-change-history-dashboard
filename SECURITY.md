# Security Policy

## Supported versions

This project is pre-1.0. Only the latest commit on `main` is supported; there are no maintained release branches yet.

## Reporting a vulnerability

Please report security issues privately through [GitHub Security Advisories](https://github.com/ali-demirbas/google-ads-change-history-dashboard/security/advisories/new) rather than opening a public issue.

Include what you'd include in any report: the affected file, how to reproduce, and the impact you think it has.

## Scope

This repo has no server and no hosted service. The skill is a single Python script that reads a local file and writes local files; it makes no network calls. The optional `tools/fetch_live_data.py` is the one part that uses the network: it calls the Google Ads API with credentials read from environment variables, and writes its result owner-readable only (mode 0600). The realistic attack surfaces:

- **XSS in the generated dashboard.** Campaign names, user names, and other source-derived text are attacker-controlled if the input file itself is untrusted. All such text is escaped before being written into the dashboard; a crafted name like `<img src=x onerror=...>` should never execute. A case where it does is in scope.
- **Malformed input causing an unhandled crash instead of a structured error.** Every malformed input (bad encoding, invalid JSON, a spreadsheet instead of CSV, missing columns, a malformed `changes.jsonl` given to `query`) should give a JSON `{"status": "error", ...}`. An unexpected exception is wrapped as `status: internal_error` with exit code 3. A raw traceback is in scope.
- **Writing outside the output folder.** The run refuses to write through a symbolic link placed in `--out-dir`. A way around that is in scope.
- **Data leakage with `--mask-users`.** Masking replaces people with stable labels and scrubs e-mail-shaped text and known identities from every string field of `changes.jsonl`, `change_history.json` and `dashboard.html`. A masked run that still shows a raw e-mail or a masked person's name is in scope. Account, campaign and ad group names and IDs are deliberately not masked. The label table (`mask-labels.json`) holds the real identities and lives in a per-user data folder outside the repo.
- **`change_id` is a content hash, not a secret.** It is not salted; with the inputs known it can be recomputed. Don't treat it as anonymous.

Issues that only affect your own local run with your own trusted data (e.g. a wrong category assignment, a parsing edge case) are bug reports, not security issues — file those as a normal issue instead.

## Response

This is a solo-maintained open-source project. There's no SLA, but reports will be acknowledged and triaged as soon as reasonably possible.
