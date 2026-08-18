# Forensic Audit — Round 3 — Resolution Log

Three separate ChatGPT-style audits were pasted this round, ~30 claims total
(with overlap between them). Unlike rounds 1 and 2, this round's source
material was noticeably lower-quality: several claims described code that
doesn't exist anywhere in this file (a `make_change_id()` signature that
doesn't match ours, a `derive_resource_id_from_path()` regex we don't have, a
microsecond-stripping regex in `parse_timestamp()` that was never written), or
described behavior directly contradicted by reading the actual code (no
`min-width:1200px` CSS, an "Unknown" actor filter that already exists, an
Explorer table that already paginates, a `pytz`-specific DST exception this
codebase can't raise because it uses `zoneinfo`). Every claim was
independently reproduced against this file's real code before being accepted
or rejected — per standing practice, no fix without reproduction.

**Full self-test: 45/45 passing** (8 new regression guards added this round,
covering the 8 confirmed-and-fixed findings below).

## Confirmed and fixed

| Finding | Verified | Fix | Re-verified |
|---|---|---|---|
| `normalize_number`: negative values with a single separator type (e.g. `-150.00`, `-3,50`) matched neither `DIGIT_GROUP_SHORT` nor `DIGIT_GROUP_3` (both required a leading digit), fell to the catch-all branch, and had their separator stripped instead of converted | Reproduced: `normalize_number('-150.00', None)` returned `-15000.0` | Added `-?` to both regexes — the sign sits before the digits either way, the `replace()` calls downstream don't touch it | Reproduced: `-150.00` → `-150.0`, `-3,50` (TR) → `-3.5`, `-150.000` (TR) → `-150000.0` |
| `try_alias_match`: a literal lowercase `"campaign"`/`"ad_group"` header was routed to `campaign_resource`/`ad_group_resource` by case alone — an ordinary display-name column happening to be named that way (not Google's own API field) lost `campaign_name`/`campaign_id` entirely | Reproduced: a JSON row with `"campaign": "Campaign Alpha"` came out with `campaign_name=None`, `campaign_resource="Campaign Alpha"` | The exact-case match is now only a candidate signal, confirmed by sampling the column's actual values for `/` (a genuine Google resource path always has one, a display name never does). No match → falls through to the normal alias lookup, where literal `"campaign"` already resolves to `campaign_name` | Reproduced: plain-name case now yields `campaign_name="Campaign Alpha"`, `campaign_resource=None`; the existing real-API-shaped fixture (F-03, `"campaign": "customers/1/campaigns/500"`) still resolves correctly |
| `make_change_id` hashed off `raw_ts` (the source's own un-normalized timestamp string) — two files representing the same event with differently-formatted timestamps (`"2026-08-01 09:00:00"` vs `"2026-08-01T09:00:00"`) never deduped | Reproduced: same event, two raw formats, both rows survived (`row_count=2`) | Hash now uses `ts_utc or dt.isoformat()` — same preference order as the existing `chrono_key()` helper — the normalized identity of "when", independent of source string formatting | Reproduced: `row_count=1`, `duplicates_removed=1` |
| ISO date parsing never accepted `/` as a separator (`2026/08/01`), even with explicit `--date-format ISO` — DMY/MDY already accept both `/` and `.` | Reproduced: `parse_timestamp('2026/08/01 10:00:00', 'ISO')` raised `unparseable ISO timestamp` | Slash-separated ISO-shaped dates are normalized to dashes before `fromisoformat`/`strptime` | Reproduced: parses correctly, `year=2026, month=8, day=1` |
| `read_rows` crashed with an unhandled `AttributeError` on a JSON array of non-object values (e.g. `[1, 2, 3]`) — contradicted its own docstring's "never a raw stdlib exception" contract | Reproduced: `AttributeError: 'int' object has no attribute 'keys'` | Added an explicit type check before `rows[0].keys()`; raises `ValueError` like every other malformed-input case | Reproduced: clean `status: "error"`, no traceback |
| `mask_rows` label assignment was pure per-run encounter order — the same real person could be "User A" in one report and "User B" in a later run on reordered/different data, defeating the point of masking for external comparison over time | Reproduced: same identity, two separate `run_pipeline()` calls with reversed row order, no guarantee of a stable label | Labels now persist in `mapping-profiles/mask-labels.json`, keyed by real identity, reused on every later run, only ever extended for new identities | Reproduced: same identity → same label across two separate runs |
| `HEADER_ALIASES["canonical_fields"]["required"]` named a `"user_identifier"` field that doesn't exist anywhere else and was never enforced (real user-identity check is a soft warning); it also implied `"account_name"` alone and `"operation"` were hard requirements, when the real rule is `account_name`-OR-`account_id`, and `operation` is inferred, not required | Confirmed via code read: `check_hard_required()` doesn't reference `"user_identifier"` at all, and its real logic differs from what the list implied | List now names exactly the 4 things `check_hard_required()` actually enforces | Confirmed no other code references this list (documentation-only) |
| Explorer table's Old/New Value columns sorted lexicographically — `"100"` sorted before `"20"` (values are strings in the payload) | Confirmed via code read; sort comparator was generic `av < bv` for every column including numeric ones | New comparator prefers `old_value_num`/`new_value_num` (already computed) when numeric, falls back to string comparison otherwise | Verified directly under Node: `[100, 20]` (as `old_value_num`) now sorts to `[20, 100]`, not the previous lexicographic `[100, 20]` unchanged |
| `displayValue()`'s unit-prefix path skipped `escapeHtml()` on the display value | Confirmed via code read. Not currently exploitable — the only caller path that sets `unit` (micros conversion) always passes a computed float/`None`, never raw source text — but a real gap in defensive coverage | Added `escapeHtml()` around the prefix, matching every other value in that function | Confirmed `escapeHtml()` handles numbers/`null` safely (`String()` coercion) |

## Confirmed but low-priority (fixed as part of the read_rows pass above)

| Finding | Fix |
|---|---|
| A JSON object without `"results"`/`"changeEvents"` returned a generic `"No headers/columns found in input."` — not silent (a real audit claim was wrong there), but didn't name the actual problem | Error message now says which wrapper key was expected and lists the top-level keys actually found |

## Refuted — described code or behavior that doesn't exist in this file

| Claim | Why it's refuted |
|---|---|
| `derive_resource_id_from_path` uses a regex `r"/(\d+)$"` that fails on composite IDs like `"456~789"` (appeared in 2 separate audits) | The real function is `rsplit("/", 1)[-1]` — no regex, no digit-only requirement. `"customers/123/adGroupAds/456~789"` already resolves to `"456~789"` correctly |
| `make_change_id()` uses SHA-256 truncated to 16 chars, a positional-dict signature | Doesn't match our function at all — real signature takes 8 named args including `source_event_id`, uses SHA-1 truncated to 20 chars |
| `parse_timestamp` strips microseconds via regex before `fromisoformat()` | No such regex exists. Verified directly: `2026-08-01T12:00:00.123456+03:00` parses with `microsecond=123456` intact, and two such timestamps 100000μs apart still compare correctly ordered |
| DST-gap times crash with `NonExistentTimeError`/`AmbiguousTimeError` | This codebase uses `zoneinfo`, not `pytz` — those exceptions are a `pytz`-specific historical wart. Reproduced with a real DST-gap time (Berlin, 2026-03-29 02:30, the "missing hour"): resolves silently to a UTC value, no exception |
| Dashboard tables use `min-width:1200px`, breaking mobile/narrow layouts | No such CSS rule exists anywhere in the file — the only `min-width` rules are 140px (filter selects) and 200px (search box) |
| No "Unknown" option in the actor-type filter, `unknown` rows fall into a logical gap | `<option value="unknown">Unknown</option>` already exists, and the filter condition (`c.actor_type !== state.actor`) already handles it correctly |
| Explorer table has no pagination, 20k+ rows would freeze/OOM the browser | Already paginates at 200 rows/page with Prev/Next — verified in code (`state.pageSize`, `pagerPrev`/`pagerNext`) |
| `detect_date_format`'s "ambiguity illusion" (all sampled days ≤12 forces manual `--date-format`) framed as a bug | This is exactly the tool's own "never guess a genuine ambiguity" design working as intended — a date range where every value could be either DMY or MDY *is* genuinely ambiguous |
| `CATEGORY_RULES` ordering miscategorizes a "budget pause" as a "campaign pause" | `CAMPAIGN_BUDGET` rules use a distinct `resource_type` and sit before the generic rules, so they're never shadowed — and Google Ads budgets don't have an ENABLED/PAUSED status field in the first place; the scenario doesn't correspond to a real API concept |
| Two different fields changed on the same campaign in the same second produce the same `change_id` and one gets wrongly deduped | Reproduced the opposite: `field_name` is already part of the hash, the two IDs don't collide |
| "Untouched Campaigns" scope illusion (only knows about campaigns present in the input, presented as if complete) | Already explicitly disclosed in the dashboard's own text: *"Based only on campaigns that appear in this change log. A campaign never in the log may not exist, not just be untouched."* — not a new finding |
| `"1,234,567"` (pure thousands-grouping) wrongly flagged as an ambiguous decimal | Technically true it gets flagged, but this matches the tool's existing, already-tested "ask, don't guess" design (same category as the `"150.000"` TR-budget fixture) — not a new defect |
| `.xlsx`/binary input crashes with a confusing Python traceback | Reproduced the opposite: falls through to the generic CSV path and returns a clean, structured `needs_mapping` status — not a crash. The `unmapped_headers` shown would be garbled binary text (a real, minor UX rough edge), but the framing ("kullanıcı çöken bir betikle karşılaşıyor") is inaccurate |

## Unverifiable — can't reproduce in this environment

- Windows console codepage `UnicodeEncodeError` on non-ASCII CLI output (a real general Python/Windows gotcha class, but not reproducible on this Mac).
- Safari-specific `new Date()` parsing of space-separated strings — moot regardless: the JS only ever receives `timestamp_iso`, produced by Python's `.isoformat()`, which always uses `T`, never a space.

## What changed in scope during this round

None beyond the fixes above. Same rule as rounds 1 and 2: verify every claim
empirically before touching code; fix what's confirmed; state plainly when a
claim doesn't hold up instead of applying it anyway.
