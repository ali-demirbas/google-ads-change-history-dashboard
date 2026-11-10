# tools/

Optional, opt-in scripts. Nothing in here is part of the skill — the skill
(`skills/google-ads-change-history-dashboard/ads_change_history.py`) stays
single-file, zero-dependency, and fully offline regardless of what's added
here.

## fetch_live_data.py

Pulls real Google Ads change history via the API and writes it as a JSON
file the skill can read directly. For accounts that actually have Google
Ads API access (a developer token + OAuth client) and want to skip the
manual CSV export step.

```bash
pip install -r tools/requirements.txt

export GOOGLE_ADS_DEVELOPER_TOKEN=...
export GOOGLE_ADS_CLIENT_ID=...
export GOOGLE_ADS_CLIENT_SECRET=...
export GOOGLE_ADS_REFRESH_TOKEN=...
export GOOGLE_ADS_LOGIN_CUSTOMER_ID=...   # only if going through a manager account
export GOOGLE_ADS_USE_PROTO_PLUS=true

python3 tools/fetch_live_data.py --customer-id 1234567890 --out changes.json
python3 skills/google-ads-change-history-dashboard/ads_change_history.py run changes.json --out-dir ./out --open
```

**Never** put credentials in a command-line flag, a committed file, or this
repo — environment variables only, set in your own shell/secret store.

**What it does on the network.** One read-only `change_event` query per run to the Google Ads API, for `--days` calendar days ending today (1-30, an explicit date range) and at most `--limit` rows (1-10,000). Nothing is sent anywhere else. The output file is written with mode 0600 because it holds account data and e-mail addresses. Zero events are written as `[]`; the skill turns that into an empty, valid report.

**Enum values.** proto-plus serializes enums as integers by default; this script asks for names (`PAUSED`, not `3`) and the self-test checks it with a proto-plus-shaped fake.

**API version.** Requests go to Google Ads API **v25** (`API_VERSION` in the
script; `--api-version` overrides it for one run). Google retires each
version roughly a year after release and requests to a retired one fail;
v25 is scheduled to sunset in August 2027. `requirements.txt` sets the lowest
`google-ads` release that serves v25 (31.2.0) as a floor, not a pin. Once the
recorded sunset date has passed, the skill's self-test prints a `[WARN]`
line (it does not fail the build of the offline skill).

**Status: needs a real account to fully verify.** The self-test runs this
script against a mocked client with synthetic events — that proves the
version it asks for and the shape it writes, not that Google's real response
matches. The event-level
fields (who/what/when/resource type) are pulled from Google's documented
`change_event` fields directly and should be solid. `old_resource`/
`new_resource` are the part most likely to need a follow-up fix — the real
API returns each as a full nested resource snapshot, not a plain value, and
`flatten_changed_value()` in this file makes a best-effort attempt to pull
out just the one field that changed. If a row's Old/New Value in the
dashboard looks like a big JSON blob instead of a clean value, that's the
fallback path firing — a real, reportable finding, not a crash. See the
module docstring in `fetch_live_data.py` for exactly what's verified vs.
best-effort.
