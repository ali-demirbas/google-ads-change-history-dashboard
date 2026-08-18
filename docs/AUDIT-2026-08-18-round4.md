# Forensic Audit — Round 4 — Resolution Log

Fourth audit round, this time against the actual consolidated repo file
rather than a fresh recreation of the code — a noticeably higher-quality
source than round 3 (every claim cited real line-level behavior, none
described code that doesn't exist in this file). All 5 confirmed findings
below were independently reproduced against this file's real code before
being fixed, same standing practice as rounds 1–3.

**Full self-test: 53/53 passing** (5 new regression guards added this
round).

## Confirmed and fixed

| Finding | Verified | Fix | Re-verified |
|---|---|---|---|
| An invalid `--timezone` (e.g. `Europe/Istanbull`, a typo) was silently swallowed — `ZoneInfo(tz)` raised inside the per-row loop, the exception was caught and discarded per row, `timestamp_utc` stayed `None`, and the row's own `timezone` field kept the invalid string as if it were real. `status: "ok"` throughout, no warning anywhere | Reproduced: `run_pipeline(..., tz="Europe/Istanbull")` returned `status: "ok"`, `timestamp_utc: None`, `timezone: "Europe/Istanbull"` | The timezone is now validated once, up front, before any row is processed — the same "stop on a genuine ambiguity" discipline already applied to date format and decimal separator | Reproduced: invalid timezone now returns `status: "error"` naming the bad value; a valid one still works unchanged |
| `validate_source()`'s `--mapping-file` loading (`open()` + `json.load()`) had no equivalent to `read_rows()`'s structured-error handling — a missing file or invalid JSON crashed with a raw `FileNotFoundError`/`JSONDecodeError`. The CLI's `--extra-rules` loading in `main()` had the identical unprotected pattern | Reproduced both: a missing/invalid `--mapping-file` crashed `validate_source()`; a missing/invalid `--extra-rules` file crashed the CLI | Both now return the same structured `{"status": "error", ...}` shape every other malformed-input case in this codebase already uses | Reproduced: both now fail cleanly with a clear message, no traceback |
| `match_structured()` trusted every `extra_rules` entry's shape completely — a rule missing `category`/`subcategory` crashed with a raw `KeyError` the instant a row matched it, deep inside categorization. `--extra-rules` is this tool's own documented, official fix for `needs_category_review`, so a typo'd rule here crashed mid-run instead of failing at load time | Reproduced: `match_structured(row, extra_rules=[{"resource_type": "CAMPAIGN"}])` → `KeyError: 'category'` | New `validate_extra_rules()` checks list-of-objects shape, required `category`/`subcategory` strings, at least one matcher key, and no unsupported keys — run once in `run_pipeline()` before the input file is even read. Deliberately does *not* restrict `category` to the existing taxonomy, since `needs_category_review`'s own instructions tell the user to introduce a new one this way | Reproduced: a malformed rule now stops with a clear message before any row is processed; a well-formed rule still passes |
| `query`'s `--since` parser (`int(since.rstrip("dwm"))`) crashed with a raw `ValueError` on garbage (`abc`, `7x`) and silently accepted a negative count (`-7d`) — `rstrip` only strips the trailing unit letter, so `-7` parsed as `int(-7)` without complaint and produced a cutoff in the *future*, silently returning wrong (usually empty) results instead of an error | Reproduced all three: `abc`/`7x` crashed; `-7d` returned an empty result set with no error | `--since` is now validated against `^\d+[dwm]$` before parsing; anything else raises `ValueError` with a clear message, caught by the CLI and printed as the same structured status | Reproduced: garbage and negative values now raise cleanly; `0d`/`7d`/`2w`/`1m` still work |
| Any unparseable-date rate, even 90%+, still returned `status: "ok"` — only a `WARNING` line in `coverage.txt` past 5%, nothing that forces a reader to see it or stops the run | Confirmed via code read: no ceiling existed at any percentage | Past 20% unparseable, the pipeline now stops with `needs_date_review` instead of silently building output from a fraction of the real dataset — mirrors the existing `>30%`-empty-required-field gate's `--force-review` escape hatch exactly | Reproduced: a 4-of-5-rows-unparseable fixture (80%) now stops; `--force-review` lets it proceed |

## Noted, not applied — design tradeoffs, not confirmed bugs

The audit itself flagged these as things it "would seriously consider," not
defects with a single correct fix. Both claims are factually accurate as
described; neither was changed, for the reasons below.

- **`infer_operation()`'s fallback defaults to `UPDATE`, not `UNKNOWN`, when nothing in the summary text matches.** True, and already disclosed via `operation_confidence: "inferred"` (a round-1 fix, F-07) — any consumer that cares can already tell a confident `UPDATE` from a guessed one. Changing the fallback value itself to `UNKNOWN` is a real behavior change with downstream effects (any `structured_rules` entry that matches on `operation` would stop matching previously-inferred rows, pushing more of them to "Other") — a genuine product decision, not a bug fix. Left as-is; worth a separate discussion if it's actually wanted.
- **The mapping-fingerprint collision risk (two different sources with identical column headers sharing one learned profile) has no value-shape check (date-like/email-like/numeric/etc.) to disambiguate them.** Already documented as a deliberate, accepted tradeoff in `SKILL.md` before this round — not a new finding. The audit's suggested fix (lightweight per-column type inference) is a real architectural addition with its own risk (a heuristic that can itself misclassify), not a quick correctness fix. Left as a documented future direction, not implemented this round.

## What changed in scope during this round

Only the 5 confirmed items above. Same rule as every prior round: verify
every claim empirically before touching code; fix what's confirmed as an
actual defect; state plainly when a claim is accurate but the "fix" is
really a product decision that deserves its own discussion, not a silent
unilateral change.
