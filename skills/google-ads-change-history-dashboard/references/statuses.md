# Statuses

Every `run` prints one JSON object. `status` is `ok`, one of the `needs_*`
questions, `error` or `internal_error`. `run_status.json` in the out folder
says whether the files next to it come from a run that reached `ok`
(`outputs_current`).

## Contents

- The rule
- Each status
- After `ok`: `data_quality`
- Remembered answers

## The rule

A `needs_*` status means the script found a real ambiguity and refused to
guess. Show the user what the status names, ask its one question in plain
words, wait for the answer, re-run with the flag the answer becomes. With
several inputs, `file` says which file the question is about.

## Each status

- `needs_mapping`: `unmapped_headers` (with `sample_values`, e-mails
  redacted) and `missing_required`. Ask one question per missing field. When
  the account is the missing field, ask for the account name and pass
  `--account-name` (all files) or `--input-account FILE=NAME`.
- `needs_review`: `issues` names a mapped column that is mostly empty. If
  the user says the file is right, `--force-review`; if it is the wrong
  column, `--map`.
- `needs_date_format`: dates such as `03/04/2026` fit both day-first and
  month-first. `--date-format DMY` or `MDY`.
- `needs_decimal_style`: numbers such as `150.000`. `--decimal-style TR`
  (thousands) or `US`.
- `needs_date_review`: over 20% of dates could not be read. Fix the format
  or the column, or `--force-review` if the unreadable dates are real.
- `needs_category_review`: resource/field/operation combinations no rule
  covers, each with an example. A rule from the user goes in a JSON list for
  `--extra-rules`; `--allow-unknown-categories` files them under "Other".
- `error`: the input or a flag is wrong; the message says what. An encoding
  error is answered with `--encoding`.
- `internal_error` (exit 3): a bug in the tool. Say so; do not retry with
  other flags.

## After `ok`: `data_quality`

`ok` means nothing was ambiguous, not that every input row is in the report.
`data_quality` (also in `coverage.txt`, `change_history.json` →
`meta.data_quality`, under the dashboard title and in `digest.md`):

- `input_rows` + rows added by splitting multi-field events and bundled
  statements - `left_out` = `rows_in_report`. `left_out` has
  `unreadable_date`, `duplicate` and, with several files,
  `duplicate_across_files`.
- `in_report_with_limits`: `category_other`, `timezone_unknown`,
  `operation_inferred`, `value_parsed_from_summary`, `multi_field_unsplit`,
  `name_without_reference_dropped`.
- `ignored_headers`: source columns nothing used; offer `--map` if one is
  clearly a name, ID or value column.
- `warnings`: plain sentences (unknown UI language, empty timeline, DST).
- `before_after` (with `--metrics`): `rows_with_window`,
  `rows_without_metrics`, `entities_unmatched`.

Tell the user every non-zero count with its number. `meta.coverage_notes`
lists what the sources cannot show (for example API data covers 30 days);
repeat those notes too.

## Remembered answers

`--map` answers are saved per column-header set in the per-user data folder
(`$XDG_DATA_HOME` or `~/.local/share/google-ads-change-history-dashboard/`)
after a run reaches ok, so the same export shape is asked about once. Two
sources with identical headers share that answer.
