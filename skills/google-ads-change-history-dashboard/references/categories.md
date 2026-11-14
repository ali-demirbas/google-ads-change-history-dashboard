# Categories

Each row gets a `category` and usually a `subcategory`. API rows are matched
by structured rules (resource type, field, operation); UI rows by summary
patterns taken from real English and Turkish exports. A cell that bundles
several statements becomes one row per statement, sharing an `event_id`.

| Category | Subcategories |
|---|---|
| Account | Access changed, Account settings changed, Customer identity changed, Manager account changed, Notification settings changed, Security settings changed |
| Ad | Ad changed, Ad created, Ad removed |
| AdGroup | Ad group created, Ad group name changed, Ad group removed, Ad group settings changed |
| Asset | Asset added, Asset changed, Asset removed, Asset set changed |
| Audience | Audience added, Audience bid adjustment changed |
| Bidding | Bid changed, Bid modifier changed, Strategy changed, Target CPA, Target ROAS |
| Budget | Budget changed, Budget created, Budget removed |
| Campaign | Campaign created, Campaign name changed, Campaign removed, Campaign settings changed |
| Conversion | Conversion action changed, Conversion action created, Conversion tracking changed |
| Feed | Feed attached to ad group / campaign, Feed changed, Feed created, Feed item added / changed / removed, Feed removed |
| Keyword | Keyword added, Keyword removed, Negative keyword |
| Status | Enabled, Paused, Removed |
| Targeting | Ad schedule changed, Device targeting changed, Language targeting changed, Location targeting changed |
| Other | no rule matched |

## "Other"

A row lands in "Other" when no rule matches. It is in the report and counted
in `data_quality.in_report_with_limits.category_other`; `coverage.txt` warns
above 10%. Mostly-"Other" output with unfamiliar words gets a warning that
the export is probably in a UI language without rules (only English and
Turkish have them). Patterns for a new language come from real rows, never
from guesswork.

## Actors

`actor_type` is `human`, `automation` (scripts, rules, recommendations, API
clients) or `unknown`. The dashboard and digest list them separately.

## Adding a rule

`--extra-rules rules.json` takes a JSON list of entries shaped like the
built-in structured rules, e.g.
`{"resource_type": "CAMPAIGN_CRITERION", "field_name_contains": "proximity", "category": "Targeting", "subcategory": "Location targeting changed"}`.
Use it only with an answer the user gave.
