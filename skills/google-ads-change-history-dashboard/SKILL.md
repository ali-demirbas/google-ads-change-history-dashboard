---
name: google-ads-change-history-dashboard
description: Turns Google Ads change-history exports into an offline dashboard and a short Markdown digest that state who changed what, when, in which account, campaign and ad group, with old and new values and a category. Reads UI "Change history" CSV/TSV downloads in English or Turkish, the legacy Turkish summary sheet and flattened ChangeEvent JSON, several files or accounts at once, and optional daily performance CSVs for before/after numbers. States facts only and never judges whether a change was good or risky. Use when the user asks "who changed X", "what changed in the last 30 days", "show me the change history", "which campaigns haven't been touched", "değişiklik geçmişi", "kim ne değiştirdi", "son 30 günde ne değişti", "hangi kampanyaya dokunulmadı", "değişiklik öncesi ve sonrası", or hands over such an export and wants a report. Not for XLSX files or Google Sheets URLs (export to CSV first).
---

# Google Ads change history

The whole tool is one stdlib-only script, `ads_change_history.py`, in this
folder. Run it as `python3 "${CLAUDE_SKILL_DIR}/ads_change_history.py" …`;
where `${CLAUDE_SKILL_DIR}` is not set, `cd` into this folder and run
`python3 ads_change_history.py …`. Details live in `references/` (read only
the one a step points to).

## Hard rules

- **Never judge.** Say what changed, by whom, when, and the numbers. No
  good/bad, improved/worsened, risky, healthy, scores, verdicts or colour
  meaning, in chat or in any file. A campaign missing from the export is
  "not in this export", never "untouched" or "neglected".
- **Never guess past a stop.** A `needs_*` status is a question for the user.
  Flags like `--force-review` or `--allow-unknown-categories` encode an answer
  the user gave, never a way around the question.
- **Privacy.** The export is the customer's data: never copy its names,
  e-mails, IDs or campaign names into a repo, a test or a public file.
  `--mask-users` hides people, not accounts or campaigns. See
  `references/privacy.md`.

## Workflow

1. **Collect the inputs.** One or more change-history files, and optionally
   daily performance CSVs. If a file has no account column (every UI export),
   you need an account name for it.
2. **MUST stop and ask, then wait for the answer** for each fact only the user
   knows and has not given: the account name per file (`--account-name` for
   all, `--input-account FILE=NAME` per file), the time zone of the times
   (`--timezone Europe/Istanbul`), and the currency if they want units
   (`--currency TRY`). Do not invent any of them.
3. **Run:**
   ```bash
   python3 "${CLAUDE_SKILL_DIR}/ads_change_history.py" run export1.csv export2.csv \
     --input-account export2.csv="Account B" --account-name "Account A" \
     --timezone Europe/Istanbul --out-dir ./out --open
   ```
   With performance data add `--metrics daily.csv` (repeatable),
   `--window-days 7` and `--conversion-lag-days 7`. All flags:
   `references/cli.md`.
4. **Read the JSON `status`.** Exit 0 is `ok`; exit 2 is a question or an
   input error; exit 3 is a bug in the tool (say so, do not retry with other
   flags). For any `needs_*` status: **MUST stop, ask the user the one
   question in plain words, and wait for the answer.** Re-run with the flag
   the answer becomes. If the answer is missing or does not settle it, ask
   again. Table below; full detail in `references/statuses.md`.
5. **On `ok`, read `data_quality` before reporting.** State every non-zero
   count with its number in one or two sentences ("3 of 1,200 rows had a date
   that could not be read and are not in the report"). Do not call it a
   problem or a pass. Also read `meta.coverage_notes` and repeat them.
6. **Report.** Point to `dashboard.html` (or the `out` path if `opened` is
   false) and paste `digest.md` when the user wants a summary for Slack or
   e-mail. Answer follow-up questions with `query`, not by eyeballing rows:
   ```bash
   python3 "${CLAUDE_SKILL_DIR}/ads_change_history.py" query ./out/changes.jsonl --user "User A" --since 7d
   ```

## Status table

| `status` | What to show | What to ask | Flag the answer becomes |
|---|---|---|---|
| `needs_mapping` | `file`, `unmapped_headers` with `sample_values`, `missing_required` | "Which column holds the change date? The old value?" (one per missing field) | `--map timestamp="Modified On"` (repeatable) |
| `needs_mapping`, account missing | `file`, `missing_required` | "Which name should this account have in the report?" | `--account-name "…"` or `--input-account FILE=NAME` |
| `needs_review` | `issues`: the column and how empty it is | "The *Was* column is empty on 33% of rows. Right for this file, or the wrong column?" | right: `--force-review`; wrong: `--map` |
| `needs_date_format` | `sample_values` | "Is 03/04/2026 the 3rd of April or March 4th?" | `--date-format DMY` or `MDY` |
| `needs_decimal_style` | `sample_values` | "Is 150.000 one hundred and fifty thousand, or 150?" | `--decimal-style TR` or `US` |
| `needs_date_review` | count and % of unreadable dates | "N of M dates could not be read. Is the date column right?" | `--date-format` / `--map timestamp=…`, or `--force-review` |
| `needs_category_review` | each `unknown_combinations` entry with its example | "What kind of change is `proximity_target` on a campaign criterion?" | `--extra-rules rules.json`, or `--allow-unknown-categories` |
| `ok`, `timezone_unknown` > 0 | the count | "Which time zone are these times in?" | `--timezone Europe/Istanbul` |
| `error` about encoding | `message` | "Do you know the file's encoding?" (often `cp1254` for older Turkish Windows files) | `--encoding cp1254` |
| `error` | `message` | nothing to decide: fix what the message names | - |

With several input files, a `needs_*` status names the file it is about in
`file`; ask about that file only.

## What it reads

English and Turkish UI exports, the legacy Turkish summary sheet and API
`ChangeEvent` data are categorized. German, Spanish, French, Portuguese,
Italian and Dutch exports have headers and dates read, most rows land in
"Other", and `data_quality` warns about the UI language. Categories and the
"Other" bucket: `references/categories.md`. Before/after performance numbers:
`references/metrics.md`.

## Outputs (in `--out-dir`)

`dashboard.html` (offline, one file), `digest.md` (Markdown tables, Turkish
for a Turkish export unless `--digest-lang en`), `changes.jsonl`,
`change_history.json`, `coverage.txt`, `run_status.json`, and
`unknown-fields.json` when it applies. A run that stops clears the previous
outputs first. Every output carries the same limits: only what is in the
given files, ChangeEvent can miss entries the UI shows and lag by about three
minutes, and nothing in it says whether a change was a good one.
