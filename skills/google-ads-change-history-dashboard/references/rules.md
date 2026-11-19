# Flag rules ("worth a look")

Every run checks the changes against twelve rules. A rule that fires adds a
**flag**: a fact measured against a threshold the user can set, for example
"Budget ₺300,00 → ₺695,00 (+132%)" under the rule "one-step budget change of
50% or more". The report calls flags *worth a look* (Turkish: *göz atılacak*)
and always shows the fact next to the rule that fired. It never says a change
was good, bad, wrong, risky, an error or an anomaly, and it uses no red/green.
Whether a flag matters is the reader's call.

## Contents

- Tiers
- The rules
- Setup sessions
- Changing thresholds: `--rules-config`
- `--known-users`
- `--work-hours` and time zones
- What the export cannot show, and how `--metrics` helps
- Where flags appear

## Tiers

- **attention**: listed first under "Worth a look", counted in the dark stat
  card on the summary page.
- **info**: listed after the attention rules, with a muted count.

The tier only orders the list. Neither tier means "bad".

## The rules

Defaults in brackets. "Campaign" means account + campaign, so two accounts
with the same campaign name never mix. Local time is the export's time in the
`--timezone` zone, the same basis as `timestamp_iso`.

| # | Rule id | Tier | Defaults | Fires when |
|---|---|---|---|---|
| 1 | `budget_jump` | attention | `pct` 50 | One "Budget changed" statement with numeric old and new values moves the budget by \|Δ%\| ≥ `pct`. One flag per statement. |
| 2 | `budget_cumulative` | attention | `pct` 50, `hours` 24 | Two or more budget changes on the same campaign within `hours` move it by \|(last new − first old) / first old\| ≥ `pct`, and no single step was already flagged by `budget_jump`. One flag per window. |
| 3 | `reverted` | attention | `add_remove_minutes` 60, `value_days` 7, `status_hours` 24 | (a) The same item (keyword text, asset ID, sitelink text, ad ID; same campaign and ad group) is added and then removed within `add_remove_minutes`; one flag per add/remove pair of saves. (b) A numeric setting (budget, target CPA/ROAS, bid limit) on the same campaign returns to an earlier value within `value_days`. (c) The same entity goes paused → enabled or enabled → paused and back within `status_hours`. |
| 4 | `repeat_edit` | attention | `minutes` 10, `min_count` 2 | The same person edits the same item `min_count` or more times within `minutes`, on the same campaign (and ad group, when there is one) and subcategory, each edit starting from the value the previous one left when both are known (a chain such as budget ₺695 → ₺750 → ₺800). The item is the campaign's budget or bid target, an API resource ID, an ID line in the cell (an asset ID), or the target named before ": " on a detail line (a schedule slot, a device, a keyword); a statement with no such item never counts. Only edits (UPDATE) count, each save once; additions, removals, two different slots or two different ads are not a repeat. Setup sessions are left out. |
| 5 | `burst_session` | attention | `minutes` 3, `min_events` 3 | The same person makes `min_events` or more **events** (one cell of the export, or one `event_id`; not statements) on the same campaign within `minutes`. Setup sessions are left out. |
| 6 | `bid_target_relearn` | attention | `days` 7 | Target CPA, target ROAS or the bid strategy changes on a campaign within `days` of the previous such change. |
| 7 | `automation_source` | attention | none | A change made by automation (scripts, rules, auto-applied recommendations, API clients), or by a named actor whose type is unknown, such as a Google rep acting "with user permission". One flag per actor, campaign and local day. |
| 8 | `unknown_user` | attention | needs `--known-users` | A person not on the user's list made changes. One flag per person, with the number of changes and campaigns. Off without the list. |
| 9 | `stale_entity` | info | `days` 30 | A campaign (and an ad group, when the export has them) whose last change is `days` or more before the data end. An ad group whose campaign is already listed is not listed again. With `--metrics`, also see below. |
| 10 | `stale_budget` | info | `days` 60 | A campaign whose last budget change is `days` or more before the data end, or that has no budget change in the export when the export's first and last records are at least `days` apart (a shorter export cannot show it). With `--metrics`, only campaigns active in the metrics. |
| 11 | `off_hours` | info | `start` "20:00", `end` "08:00", `weekends` true | An event by a person (not automation) at local time from `start` until `end`, or on Saturday/Sunday. 08:00 sharp is working time; 20:00 sharp is not. One flag per event. Off when the time zone is unknown. |
| 12 | `bulk_edit` | info | `minutes` 10, `min_changes` 20, `min_campaigns` 5 | One person (not automation) makes `min_changes` or more statements across `min_campaigns` or more campaigns within `minutes`. |

The data end is the newest change in the export. Each flag carries
`change_ids` (the rows involved), `event_ids`, the account, campaign, ad group
and person, `ts_first`/`ts_last`, a `fact` object with the numbers (old, new,
pct, count, minutes, days) and one plain sentence in Turkish and English.

## Setup sessions

Building a new campaign produces many changes in a few minutes. A window
counts as setup when the same person has a "Campaign created", "Ad group
created" or "Budget created" row for that campaign within ± the rule's
`minutes`. `repeat_edit` and `burst_session` leave setup windows out. A
campaign created before the export period has no such row, so its later edits
are not treated as setup.

## Changing thresholds: `--rules-config`

```bash
python3 ads_change_history.py run export.csv ... --rules-config rules.json
```

`rules.json` holds only what differs from the defaults:

```json
{
  "budget_jump": {"pct": 40},
  "stale_entity": {"days": 45},
  "bulk_edit": {"enabled": true, "min_changes": 10},
  "off_hours": {"weekends": false},
  "automation_source": {"enabled": false}
}
```

A rule left out keeps its defaults; a parameter left out keeps its default.
`"enabled": false` turns a rule off. Counts (`min_count`, `min_events`,
`min_changes`, `min_campaigns`) are positive whole numbers, other numbers are
positive, `start`/`end` are times like `"20:00"` and `weekends` is true or
false. An unknown rule, an unknown setting, a wrong value or a file that is
not valid JSON stops the run with `status: "error"` and a message that names
it. The parameters actually used, the number
of hits and the reason a rule is off are written to `rules_config` in
`change_history.json` and shown on the dashboard's details page next to the
flags, so every number in the report can be traced to its threshold.

The dashboard's *Try your own thresholds* panel on the all-changes page is a
separate, in-browser what-if over budget/target/bid percentages and status
changes. It never changes the flags written by the run.

## `--known-users`

```bash
python3 ads_change_history.py run export.csv ... --known-users team.txt
```

```text
# our team (lines starting with # are comments)
ayse@example.test
Mert Example
```

One e-mail or name per line, matched without regard to case; `#` starts a
comment and blank lines are skipped. With the list,
`unknown_user` flags each person who made changes and is not on it;
automation and Google actors are covered by `automation_source` instead.
Without the list the rule is off and `rules_config` says why. Ask the user for
the list; never build it from the export. The file names real people: keep it
outside any repository (see `privacy.md`).

## `--work-hours` and time zones

`--work-hours 08-20` (or `08:30-19:00`) is a shortcut for `off_hours` `end`
"08:00" and `start` "20:00", and wins over `start`/`end` in
`--rules-config`. Weekends are counted unless `{"off_hours": {"weekends": false}}` is
given in `--rules-config`. UI exports carry no time zone: without
`--timezone` the rule is off, with a reason, because the hour of a change is
not known.

## What the export cannot show, and how `--metrics` helps

- **Inactivity.** A change history lists changes. A campaign that nobody
  touched during the whole export period is not in it at all, so
  `stale_entity` can only name campaigns that appear in the export. With
  `--metrics`, `stale_entity` also lists campaigns and ad groups that have
  metrics rows in the last 14 days of the metrics period but no change in the
  last `days` (`basis: "metrics"`), and `stale_budget` keeps only campaigns
  that are active in the metrics. Flags from the export alone say
  `basis: "export"`.
- **Missing sources.** The API does not return Google Ads Editor changes and
  may miss auto-applied recommendations, so `automation_source` counts only
  what the files list.
- **Setup before the period.** Setup is recognized from creation rows; a
  campaign created before the export starts has none.
- **People.** The export gives an e-mail or a name, not a team. Only
  `--known-users` can say who is outside the team.
- **Effect.** No rule looks at performance. The before/after numbers of
  `--metrics` sit beside a change; they never feed a flag and never label a
  difference as better or worse.

## Where flags appear

- `change_history.json` and the dashboard data: `flags`, `rules_config`,
  `exec_summary` (totals, people, budget movement, longest-untouched
  campaigns, top campaigns, headline and sentences) and `campaign_table`.
  Rows keep their schema; flags point to them through `change_ids`.
- Dashboard page 1 (Summary): the *Worth a look* list, one card per rule with
  hits, its count, title, what it checks and up to two example lines.
- Dashboard page 2 (Details): every flag grouped by rule, each linking to its
  changes, with the thresholds used; flag tags in the campaigns table.
- Dashboard page 3 (All changes): a *Worth a look* filter by rule and a flags
  column.
- `digest.md`: a *Worth a look* section ("Göz atılacaklar" in Turkish): one
  heading per rule that fired, with its threshold and count, and one line per
  flag (date, campaign, fact).
