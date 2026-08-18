# Dogfooding notes

Bugs found by running the tool against a real Google Ads API export, as
opposed to a pasted audit reviewing the code. Different provenance from the
`AUDIT-*.md` files — those are records of verifying an external critique;
this is what actually broke when a real user ran it. No real account data,
customer IDs, or emails from those runs ever entered this repo (`CLAUDE.md`
rule 5 — findings are recorded here generically, the raw export stays with
whoever ran it).

## 2026-08-18 — campaign/ad-group identity missing on self-referential rows

**Symptom:** a real ChangeEvent-JSON export, run through the dashboard,
showed `Changed Campaigns: 0` and `Changed Ad Groups: 0` in Summary despite
dozens of rows correctly categorized "Campaign" and "AdGroup" in Category
Distribution, and every row in Change Explorer showing `—` in the
Campaign/Ad Group columns.

**Cause:** `campaign_id`/`ad_group_id` were only ever derived from a
separate attributed `campaign`/`ad_group` field (Google's own pointer field,
used when the *changed* resource is something below the campaign — a
keyword, an ad, a budget). A row whose changed resource **is** the campaign
or ad group itself doesn't carry that pointer — a campaign doesn't point to
itself — so `campaign_id`/`ad_group_id` stayed empty even though the
resource's own identity (already resolved from `change_resource_name` into
`resource_id`) was sitting right there unused.

**Fix:** when `resource_type` is exactly `CAMPAIGN`/`AD_GROUP` and no
attributed pointer field was found, `campaign_id`/`ad_group_id` now fall
back to the row's own `resource_id`. Doesn't touch `campaign_name`/
`ad_group_name` — no display name is genuinely available in this case, and
the dashboard already falls back to `Campaign {id}`-style display for that.

Regression test added, self-test 53 → 54.

## 2026-08-18 — the real native Google Ads UI "Change history" CSV export

**Symptom:** exporting directly from the Google Ads UI itself (Campaigns >
Change history > Download) — not the API, not a third-party tool — produced
a CSV this tool couldn't process at all, then processed badly. This is
arguably the single most common real input this tool will ever see, and it
took four separate fixes to actually work:

1. **`needs_mapping`, "Changes" column unrecognized.** This export's own
   column name for the free-text summary is literally `Changes` — not
   previously in `raw_summary`'s alias list.
2. **`needs_mapping`, no account column.** You're already scoped into one
   account when you export this report, so Google doesn't repeat it per
   row — genuinely no account column exists. Added `--account-name` to
   supply the one constant account this whole file belongs to, explicitly
   (never guessed).
3. **`needs_mapping`, `resource_type` required even though nothing downstream
   needs it.** This export has no field_name/resource_type column either —
   every row is free text. `categorize_changes()` already routes any row
   with no `field_name` through `match_summary()` (text-only), which never
   reads `resource_type` — the hard requirement was blocking a source from
   something its own code path doesn't use. Now only required when
   `raw_summary` *isn't* mapped either (in which case nothing could
   categorize it regardless, so failing hard is still correct there).
4. **`needs_date_format`, with a misleading "DD/MM vs MM/DD" message.** This
   export's timestamps look like `Aug 16, 2026, 10:42:26 PM` — a
   spelled-out month has no day/month ordering to be ambiguous about; no
   existing branch recognized the shape at all. Added a new `UI_EN` format,
   auto-detected (not asked for, since it's unambiguous) like ISO already
   is.
5. **Once parsing "succeeded," every row still had a stray `ValueError`**
   waiting — the timestamp actually contains a **U+202F (narrow no-break
   space)** before AM/PM, not a plain ASCII space. Neither the detection
   regex nor `strptime`'s `%p` matched across it. Normalized U+202F/U+00A0
   to a plain space before any date parsing, unconditionally (not just for
   `UI_EN` — a real, general export-encoding quirk).
6. **Once it ran end to end, 100% of rows categorized as "Other".** This
   format's free-text phrasing ("Campaign changed", "12 budget amount
   decreased", "Ad group created", "4 phrase match keywords added", ...) is
   completely different from the Turkish legacy-format phrasings
   `summary_text_rules` already covered — there was no English coverage at
   all. Added ~20 new patterns generalized from the actual first-line
   phrasings observed across 210 real rows in this export (product-specific
   text stripped out) — covers roughly 94% of what was seen; the remainder
   (rare account-admin events like "Customer manager changed") are left
   uncovered on purpose rather than force-categorized into an advertising
   bucket they don't belong to.

**Result on the real file this was found against:** `needs_mapping` /
`needs_mapping` / `needs_mapping` / `needs_date_format` (misleading) /
silent per-row crash / 100% Other → `status: "ok"`, 2% Other, 200 rows,
categorized correctly across Budget/Campaign/Ad/Status/AdGroup/Conversion/
Targeting/Keyword.

Regression test added (synthetic fixture reproducing the shape — column
names, the narrow-no-break-space timestamp, no account column), self-test
54 → 55.
