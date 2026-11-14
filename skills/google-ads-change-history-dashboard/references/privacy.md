# Privacy

## `--mask-users`

Off by default. Replaces each person with `User A`, `User B`… (the same
label on every later run) and scrubs e-mail-shaped text and the known
identities from every string field of every output file, `digest.md`, summary
text and old/new values included.

It does **not** mask account, campaign or ad group names, IDs or resource
paths. If the user wants to share a report without revealing whose account
it is, say plainly that this flag does not do that.

## Where state is kept

The label table (`mask-labels.json`) and remembered column mappings live in
a per-user data folder, `$XDG_DATA_HOME` or
`~/.local/share/google-ads-change-history-dashboard/`, outside any repo.
They hold real identities: never share or commit them. Private evaluation
cases live under `evals-private/` in the same folder.

## Rules for working with real exports

- Real exports are a customer's data. Never copy their names, e-mails, URLs,
  IDs or campaign names into a repo, test, fixture, doc, comment or commit
  message.
- Only synthetic data with invented names goes into this public repo.
- `needs_mapping` sample values shown in chat have e-mail addresses
  redacted; the rest of the sample is real data, so keep it in the chat.
- The skill never calls the network. Only the optional
  `tools/fetch_live_data.py` talks to the Google Ads API, with credentials
  from environment variables only, and writes its file with mode 0600.
