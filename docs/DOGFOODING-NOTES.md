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
