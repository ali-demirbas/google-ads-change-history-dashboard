# CLI reference

Run from this skill's folder, or prefix the script with `${CLAUDE_SKILL_DIR}/`.

## Contents

- `run`: inputs and account names
- `run`: facts only the user knows
- `run`: answers to a stop
- `run`: performance data and the digest
- `query`
- `self-test`
- Exit codes

## `run`: inputs and account names

```bash
python3 ads_change_history.py run <input> [<input> ...] --out-dir ./out [--open]
```

- One or more inputs: UI change-history CSV/TSV (as downloaded, title lines
  are skipped), the legacy Turkish summary sheet, or ChangeEvent JSON/CSV as
  written by `tools/fetch_live_data.py`. A single input behaves as before.
- Every file is validated and mapped on its own; a stop names the file in
  `file`. Every row keeps its `source_file` (basename).
- `--account-name "Account A"`: account name for every file with no account
  column and no specific name.
- `--input-account FILE=NAME` (repeatable): account name for one file, e.g.
  `--input-account export_b.csv="Account B"`.
- A change that appears in more than one file is kept once and counted in
  `data_quality.left_out.duplicate_across_files`. Inside one file identical
  rows are never dropped.
- `meta.sources` is a list of objects `{file, rows, accounts, source_label,
  date_min, date_max}`, one per file. The plain list of source types is
  `meta.source_labels`.
- In a multi-file run `--map` and `--mapping-file` apply only to the files
  that contain the named headers. Two different unknown formats are mapped
  one at a time: run each file alone, answer its `needs_mapping`, and the
  saved profile is reused in the combined run.

## `run`: facts only the user knows

| Flag | Meaning |
|---|---|
| `--timezone Europe/Istanbul` | IANA zone of timestamps that carry no offset (UI exports). Without it those rows are left off the timeline and counted. |
| `--currency TRY` | One ISO 4217 code shown as the unit on amounts. Skip it for a multi-currency MCC export. |
| `--encoding cp1254` | Input codec when the run says the text cannot be read cleanly. |
| `--mask-users` | Replace people with `User A`, `User B`… (see `privacy.md`). |

## `run`: answers to a stop

| Flag | Answers |
|---|---|
| `--map FIELD=HEADER` (repeatable) | `needs_mapping`: which column holds a canonical field. Remembered per header set after a run reaches ok. |
| `--mapping-file FILE` | The same answers as a JSON file `{"source_label": …, "mapping": {field: header}}`. |
| `--date-format DMY\|MDY\|ISO\|UI_EN` | `needs_date_format`. |
| `--decimal-style TR\|US` | `needs_decimal_style`. |
| `--force-review` | `needs_review` / `needs_date_review` when the user confirms the data is right as it is. |
| `--extra-rules FILE` | `needs_category_review`: a JSON list of extra structured rules. |
| `--allow-unknown-categories` | `needs_category_review` when the user prefers not to decide: those rows become "Other" and are counted. |

## `run`: performance data and the digest

| Flag | Default | Meaning |
|---|---|---|
| `--metrics PATH` (repeatable) | none | Daily performance CSV(s) for before/after numbers. See `metrics.md`. |
| `--window-days N` | 7 | Days compared before and after each change. |
| `--conversion-lag-days N` | 7 | After-window days within the last N days of the metrics' last day are counted as maturing. |
| `--digest-lang auto\|tr\|en` | auto | Language of `digest.md`: Turkish for a Turkish export, else English. |

`digest.md` holds the period and source files, totals, changes per category,
per user (masked with `--mask-users`), per account and per ISO week, the ten
most-changed campaigns, campaigns whose last change is 14 or 30+ days before
the newest row, coverage notes and one data-quality line. It is Markdown
tables, ready to paste.

## `query`

```bash
python3 ads_change_history.py query ./out/changes.jsonl --user "User A" --since 7d [--format json]
```

`--user`, `--account`, `--campaign` are substring matches that ignore case
and accents, Turkish-aware (`ismail` finds `İsmail`). Also `--category`,
`--operation`, `--source`. `--since 7d|2w|1m` counts back from now;
`--since-anchor newest` counts back from the newest row. The anchor is
printed.

## `self-test`

`python3 ads_change_history.py self-test` runs every built-in synthetic case
and exits 0 only when all pass. The scored evaluation set lives in the repo's
`evals/` folder (`python3 evals/run_evals.py`).

## Exit codes

0 ok, 2 a question or an input error (JSON `status`), 3 `internal_error`
(a bug in the tool). `--open` adds `opened: true/false`.
