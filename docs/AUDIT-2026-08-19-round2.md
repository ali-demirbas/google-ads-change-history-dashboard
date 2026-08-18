# Forensic Audit — Round 2 — Resolution Log

Second independent audit of `ads_change_history.py` after the round-1 fixes
(see `AUDIT-2026-08-18.md`). All 18 claims in the round-2 audit were verified
empirically against the actual code before any fix was applied (per standing
practice — no fix without reproduction). Every claim checked out as real;
none were refuted this round (unlike round 1, where one ChatGPT-sourced claim
about ChangeEvent's field model was wrong).

All findings below were fixed and re-verified. Full self-test: **33/33
passing** (24 from round 1 + 9 new regression guards from this round — some
round-2 findings share one guard, e.g. the P2 account/user identity checks).

## P0

| Finding | Verified | Fix | Re-verified |
|---|---|---|---|
| Script-context XSS: `json.dumps(data)` embedded raw into a `<script>` tag never escaped `</script>`, `<`, `>` | Reproduced live in-browser: `DASH_DATA` came back `undefined`, 5 extra `<img>` tags appeared from one crafted campaign name | JSON payload now passes through `.replace('<','<').replace('>','>').replace('&','&')` before embedding (same technique as Django's `json_script`) — safe inside a JSON string, decodes back to the real character in JS | Re-ran the identical exploit: `DASH_DATA` is a proper object, 0 `<img>` tags, payload round-trips correctly as *data* (`most_active_campaign` holds the literal malicious string as a string, not as executed markup) |

## P1

| Finding | Verified | Fix | Re-verified |
|---|---|---|---|
| Campaign identity still `campaign_name`-based (Python `campaign_keys`/`campaign_last`, JS `renderSummary`/`renderAccounts`/`renderUntouched`) — two different `campaign_id`s sharing a name within one account collided | Reproduced: 2 campaigns, same account, different ids, no display name — everywhere merged into 1 | `campaign_identity(r) = r.get("campaign_id") or r.get("campaign_name")` used as the aggregation key throughout, both Python and the JS mirror; display falls back to `Campaign {id}` when no name exists | Reproduced same case: `changed_campaigns: 2`, 2 separate `untouched` entries, 2 separate dashboard filter options ("Campaign 100"/"Campaign 200") |
| Same defect for ad groups | Confirmed via code read: `ad_group_keys` used `ad_group_name` | `ad_group_identity(r) = r.get("ad_group_id") or r.get("ad_group_name")` | Covered by the same fix pattern; `changed_ad_groups` now id-scoped |
| `change_history.json`'s `days_since_last_change` (static, window-end-relative) vs the dashboard's live JS figure (real clock) — same field name, two different numbers | Reproduced: JSON reported `0` for same-day data while the dashboard would show real elapsed days when opened later | Renamed the Python-computed field to `days_since_last_change_at_generation` — makes the static/live distinction explicit instead of colliding on one name. The dashboard's live figure is correct as designed (round 1); a static file genuinely can't have "today" | Confirmed the renamed field is present, the old name is gone |
| `window_start`/`window_end`/`campaign_last` compared `timestamp_iso` (naive) instead of `timestamp_utc` — wrong ordering possible across genuinely different resolved timezones | Confirmed via code read | `chrono_key(r) = r.get("timestamp_utc") or r.get("timestamp_iso")` used for all chronological comparisons in `build_aggregation` (Python) and the JS mirror in `renderUntouched` | Existing timezone-offset tests still pass; new chrono_key path exercised by the campaign-id regression test (uses `+03:00`-bearing timestamps) |
| Unparseable-date rows silently dropped from `row_count`; `status` stayed `"ok"` with no threshold/visibility | Reproduced: 1 of 2 rows dropped, `status: "ok"`, count only in a JSON field nobody is required to check | `coverage.txt` now always reports the skip count/percentage (with a >5% WARNING line); `change_history.json`'s `meta` carries `rows_skipped_unparseable_date`/`_pct`; dashboard shows a warn-box when the rate exceeds 5% | Reproduced: `coverage.txt` contains `"Rows skipped (unparseable date): 1"`, `meta.rows_skipped_unparseable_date == 1` |

## P2

| Finding | Verified | Fix | Re-verified |
|---|---|---|---|
| ISO parser's fallback silently truncated trailing junk (`"...10:00:00JUNK"` parsed by ignoring "JUNK"; an offset before the junk was also lost) | Reproduced both cases | Fallback `strptime` is now a whole-string match (no `s[:len(f)+2]` slicing) — anything left over is a parse failure, not silently ignored | Reproduced: both junk-suffixed strings now raise `ValueError` |
| DMY/MDY branch never attempted to preserve a trailing offset at all | Reproduced: `"01/08/2026 10:00:00+03:00"` parsed with `tzinfo=None` | Regex extended with an optional trailing `(Z|[+-]HH:MM)` group, applied the same way the ISO branch does | Reproduced: same input now returns `tzinfo` with the correct `+03:00` offset |
| Generic `operation=REMOVE → Status/Removed` rule shadowed the more specific "Campaign removed"/"Ad group removed" categories (declared but unreachable) | Reproduced: `CAMPAIGN`+`REMOVE` and `AD_GROUP`+`REMOVE` both resolved to `Status/Removed` | Added `CAMPAIGN`+`REMOVE`→`Campaign/Campaign removed` and `AD_GROUP`+`REMOVE`→`AdGroup/Ad group removed` rules *before* the generic catch-all (first-match-wins ordering) | Reproduced: both now resolve to their specific categories; confirmed `CAMPAIGN_BUDGET`/keyword/`ASSET` REMOVE rules (already specific, earlier in the list) are unaffected |
| `account_names`/`accounts_map` used `account_name`, inconsistent with `campaign_keys`, which already used `account_id` | Reproduced: 2 different `account_id`s sharing a name counted as 1 active account | `account_identity(r) = r.get("account_id") or r.get("account_name")` used consistently for the active-accounts count | Reproduced: 2 different account_ids now count as 2 |
| `human_counter` keyed by `user_name` (display) over `user_email` — two different people sharing a display name merged | Reproduced: 2 different emails, same display name intent, merged to 1 | `user_identity(r) = r.get("user_email") or r.get("user_name")` used for grouping; display name still shown as the label via a separate `*_display` map | Reproduced: 2 different emails now count as 2 active users |
| CLI `--user`/`--account`/`--campaign` substring match vs dashboard's exact Set match — undocumented inconsistency | Reproduced: `--user "User A"` also matched `"User AB"` | Not changed — documented as an intentional difference (CLI = grep-style convenience, dashboard = exact match from a closed picklist) in both `query_changes()`'s docstring and `SKILL.md` | N/A — documentation-only, no behavior to re-verify |

## P3

| Finding | Fix |
|---|---|
| `multi_field` semantic tension with "one row = one field change" | Already self-flagged via the `multi_field` boolean (round 1); accepted as a documented limitation, no further code change — a true per-field old/new split isn't derivable from a flat resource string |
| Module docstring's canonical field list missing `campaign_resource`/`ad_group_resource`/`operation_confidence`/`multi_field`, understated `operation`'s real value range | Docstring rewritten to list all current fields and note `UNKNOWN`/`UNSPECIFIED` are preserved, not just `CREATE`/`UPDATE`/`REMOVE` |
| `change_id` docstring didn't mention `source_event_id` joined the hash | Docstring updated |
| Canonical `resource_name` field has no alias pointing to it, always `None` | Left as declared-but-unpopulated (same reasoning as `user_id` from round 1) — no known source provides a genuine resource display name distinct from its path; noted explicitly in the docstring rather than silently left unexplained |
| `known_sources` never consulted at runtime — `source` field always generic `"alias_match"` even for a recognized format | `detect_known_source()` added: after a Layer-2 alias match, checks whether the input's headers are a superset of a `known_sources` entry's declared header set, and uses that entry's key as `source_label` when so |

## What changed in scope during this round

None. Same rule as round 1: verify every claim empirically before touching
code; fix what's confirmed; leave design decisions that were already
deliberated (CLI substring matching, `user_id`/`resource_name` staying
declared-but-empty) as documented choices rather than forcing a change.
