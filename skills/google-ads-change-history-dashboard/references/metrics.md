# Before/after metrics

Optional. With `--metrics daily.csv` (repeatable) each change that matches a
metrics entity gets the performance numbers of the days before and after it.
The numbers are shown side by side with their raw and % difference; nothing
labels a difference as better or worse.

## Input

A daily performance CSV exported from the Google Ads UI or API, one row per
day and campaign (optionally per ad group). Title lines, encodings,
delimiters, spelled-out month names and decimal styles are read the same way
as change exports. "Total"/"Toplam" rows are skipped. Decimal style comes
from `--decimal-style`, else TR for Turkish headers, else US. An API export's
`metrics.cost_micros` is divided by 1,000,000.

| Field | Accepted headers |
|---|---|
| day (required) | Day, Gün, Date, Tarih |
| campaign (required) | Campaign, Kampanya |
| ad group | Ad group, Reklam grubu |
| account | Account, Hesap |
| impressions | Impr., Impressions, Gösterim |
| clicks | Clicks, Tıklama |
| cost | Cost, Maliyet |
| conversions | Conversions, Dönüşüm, Dönüşümler |
| conversion value | Conv. value, Dönüşüm değeri |

Other columns are ignored and listed in `metrics_meta.ignored_columns`.

## Windows

- `--window-days N` (default 7). Before = the N full days before the
  change's local date. After = the change's local date and the N-1 days
  after it.
- `--conversion-lag-days N` (default 7). After-window days that fall within
  the last N days of the metrics' last day are counted in `maturing_days`:
  their conversions may still be reported later.
- A missing metric is `null`, never 0. `days_with_data` says how many days
  of the window the file covers.
- `overlapping` lists the other changes in the same campaign and window
  (count and up to 20 change ids), because their numbers share days.
- An entity-day that appears in more than one metrics file is counted once.
  Campaign totals are never summed from ad-group rows.

## Matching

By account + campaign name, plus ad group when both sides have one; case,
whitespace and Turkish dotted/dotless i are ignored. A metrics file without
an account column matches any account, unless `--input-account` or
`--account-name` names its account. Unmatched rows have no
`before_after` entry and are counted in `data_quality.before_after`.

## Output

Only with `--metrics`, `change_history.json` and the dashboard carry `metrics_meta` (files, levels,
date range, metrics present, window and lag days, ignored columns) and
`before_after` keyed by `change_id`. The dashboard shows a before/after
strip in the detail panel, an optional Before/After column, and a small
neutral clicks or cost line under the Activity Timeline.
