#!/usr/bin/env python3
"""Evaluation set for the change-history skill (stdlib only, 2026-11-14).

Runs the skill on every case in evals/cases/ (and, when present, the private
cases dir), aligns its output rows to a hand-written truth.json by
(event order, statement_index) and scores category / operation / value
accuracy against per-case thresholds. Where truth.json lists `flags`, it also
scores each rule's flags (precision and recall, gated at 1.0). It checks the
wording rule (flags are facts, never good/bad) in dashboard.html, digest.md and
the generated texts, and that no row was lost.

  python3 evals/run_evals.py                 # synthetic + private (if any)
  python3 evals/run_evals.py --synthetic-only
  python3 evals/run_evals.py --case tr_native_preamble --verbose
Exit 0 = every case passed, 1 = a case failed or was skipped (--allow-skip tolerates skips).
"""
import argparse
import collections
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SKILL = HERE.parent / "skills" / "google-ads-change-history-dashboard" / "ads_change_history.py"
CASES = HERE / "cases"
# 2026-11-15: subcategory_accuracy is gated too (only where the truth labels subcategories).
# 2026-11-19: flag_precision / flag_recall are gated where the truth lists flags (min over rules).
DEFAULT_THRESHOLDS = {"category_accuracy": 1.0, "subcategory_accuracy": 1.0, "operation_accuracy": 1.0, "value_match_rate": 1.0, "actor_accuracy": 1.0,
                      "flag_precision": 1.0, "flag_recall": 1.0}
GATED = ("category_accuracy", "subcategory_accuracy", "operation_accuracy", "value_match_rate", "actor_accuracy", "flag_precision", "flag_recall")
# Rule ids and tiers of the round-12 data contract, in display order.
RULE_TIERS = (
    ("budget_jump", "attention"), ("budget_cumulative", "attention"), ("reverted", "attention"), ("repeat_edit", "attention"),
    ("burst_session", "attention"), ("bid_target_relearn", "attention"), ("automation_source", "attention"), ("unknown_user", "attention"),
    ("stale_entity", "info"), ("stale_budget", "info"), ("off_hours", "info"), ("bulk_edit", "info"),
)

# Judgement words (EN + TR). A hit outside the allowed disclaimer phrases fails the check.
JUDGE_WORDS = (
    r"better|worse|improved?|improvements?|improving|worsen(?:ed|ing|s)?|good|bad|poor(?:ly)?|excellent|great|"
    r"healthy|unhealthy|risky|winners?|losers?|wins?|underperform\w*|outperform\w*|well done|"
    r"daha iyi|daha kötü|iyi|kötü|iyileş\w*|kötüleş\w*|başarılı|başarısız|olumlu|olumsuz|sağlıklı|riskli|harika|"
    r"zayıf|güçlü|kazanan|kaybeden"
)
JUDGE_RE = re.compile(r"(?i)(?<![\w-])(" + JUDGE_WORDS + r")(?![\w-])")
# 2026-11-19: flags say "worth a look", never these; checked in digest.md and generated texts.
FLAG_WORDS_RE = re.compile(r"(?i)(?<![\w-])(anomal\w*|errors?|wrong|hata\w*|yanlış\w*|suspicious|şüpheli)(?![\w-])")
# Disclaimers that name a judgement in order to say the tool never makes one.
DISCLAIMER_RE = re.compile(
    r"(?i)(never (?:whether|says?|judges?|calls?)|not that it was|no good/bad|good / watch|"
    r"(?:asla|hiçbir zaman)\b[^.\n]{0,80}|\byorum (?:yapmaz|yok)\b[^.\n]{0,40})[^.\n]{0,80}"
)


def default_private_dir():
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(base) / "google-ads-change-history-dashboard" / "evals-private"


def strip_dashboard(text):
    """Template text only: drop the embedded data blob, <style>, and JS/CSS comments."""
    marker = "/*__DASH_DATA__*/"
    i = text.find(marker)
    if i >= 0:
        try:
            _, end = json.JSONDecoder().raw_decode(text, i + len(marker))
            text = text[:i] + text[end:]
        except ValueError:
            pass
    text = re.sub(r"(?is)<style\b.*?</style>", " ", text)
    text = re.sub(r"(?s)/\*.*?\*/", " ", text)
    text = re.sub(r"(?m)(^|[^:\"'\\])//[^\n]*", r"\1 ", text)
    return text


def judge_hits(text, template=False):
    """Judgement words, plus the flag-wording words; "error" is left to JS code in the dashboard template."""
    text = DISCLAIMER_RE.sub(" ", text)
    hits = {m.group(1).lower() for m in JUDGE_RE.finditer(text)}
    hits |= {m.group(1).lower() for m in FLAG_WORDS_RE.finditer(text) if not (template and m.group(1).lower().startswith("error"))}
    return sorted(hits)


def generated_texts(ch):
    """Sentences the skill writes itself (not copied from the input): flag texts, rule titles and descriptions, the summary."""
    out = []
    for rc in ch.get("rules_config") or []:
        for k in ("title", "description", "disabled_reason"):
            out += [v for v in (rc.get(k) or {}).values() if isinstance(v, str)] if isinstance(rc.get(k), dict) else []
    for f in ch.get("flags") or []:
        out += [v for v in (f.get("text") or {}).values() if isinstance(v, str)]
    es = ch.get("exec_summary") or {}
    out += [v for v in (es.get("headline") or {}).values() if isinstance(v, str)]
    for v in (es.get("sentences") or {}).values():
        out += [s for s in (v or []) if isinstance(s, str)]
    return out


def num_eq(a, b):
    if a is None or b is None:
        return a is None and b is None
    try:
        return abs(float(a) - float(b)) < 1e-6
    except (TypeError, ValueError):
        return False


def group_events(rows):
    """Rows in output order -> events; a new event starts at statement_index 0 or a new event_id."""
    events, prev = [], None
    for r in rows:
        si = r.get("statement_index") or 0
        if not events or si == 0 or r.get("event_id") != prev:
            events.append([])
        events[-1].append(r)
        prev = r.get("event_id")
    return events


def run_case(case_dir, verbose=False, skill=SKILL):
    args = json.loads((case_dir / "args.json").read_text(encoding="utf-8"))
    truth = json.loads((case_dir / "truth.json").read_text(encoding="utf-8"))
    res = {"case": case_dir.name, "status": "PASS", "problems": [], "requires": args.get("requires", [])}
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "out"
        cmd = [sys.executable, str(skill), "run"] + list(args["inputs"]) + ["--out-dir", str(out)] + list(args.get("args", []))
        # 2026-11-15: a fresh XDG_DATA_HOME per case, so saved user profiles never leak in.
        env = dict(os.environ, XDG_DATA_HOME=str(Path(td) / "xdg"))
        p = subprocess.run(cmd, cwd=str(case_dir), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, encoding="utf-8", env=env)
        if p.returncode != 0 and args.get("requires") and ("unrecognized arguments" in p.stderr or "error: argument" in p.stderr):
            res["status"] = "SKIPPED"
            res["problems"].append("installed skill does not support %s yet: %s" % (
                "/".join(args["requires"]), (p.stderr.strip().splitlines() or ["?"])[-1][:160]))
            return res
        try:
            summary = json.loads(p.stdout)
        except ValueError:
            summary = {"status": "no_json", "stderr": p.stderr[-400:]}
        exp = truth.get("expected", {})
        if summary.get("status") != exp.get("status", "ok"):
            res["status"] = "FAIL"
            res["problems"].append("status %r (expected %r) exit %d %s" % (summary.get("status"), exp.get("status", "ok"), p.returncode, p.stderr[-200:].strip()))
            return res
        rows = [json.loads(l) for l in (out / "changes.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
        ch = json.loads((out / "change_history.json").read_text(encoding="utf-8"))
        dq = ch.get("meta", {}).get("data_quality") or summary.get("data_quality") or {}
        res.update(score(truth, rows, verbose))
        res["problems"].extend(res.pop("align_problems"))
        aligned = res.pop("_aligned")
        for k, v in exp.get("data_quality", {}).items():
            if dq.get(k) != v:
                res["problems"].append("data_quality.%s = %r (expected %r)" % (k, dq.get(k), v))
        # No data loss: the counts add up, nothing left out except declared cross-file duplicates.
        left = dict(dq.get("left_out", {}))
        across = left.pop("duplicate_across_files", 0)
        added = dq.get("rows_added_by_field_split", 0) + dq.get("rows_added_by_statement_split", 0)
        if dq.get("input_rows", 0) + added - sum(left.values()) - across != dq.get("rows_in_report"):
            res["problems"].append("data_quality counts do not add up: %s" % json.dumps(dq.get("left_out")))
        if sum(left.values()) != exp.get("dropped", 0):
            res["problems"].append("rows left out %s (expected %d)" % (json.dumps(left), exp.get("dropped", 0)))
        if across != exp.get("duplicate_across_files", 0):
            res["problems"].append("duplicate_across_files %s (expected %s)" % (across, exp.get("duplicate_across_files", 0)))
        if dq.get("rows_in_report") != exp.get("rows_in_report") or len(rows) != exp.get("rows_in_report"):
            res["problems"].append("rows_in_report %s / changes.jsonl %d (expected %s)" % (dq.get("rows_in_report"), len(rows), exp.get("rows_in_report")))
        for w in exp.get("warnings_contain", []):
            if not any(w in x for x in dq.get("warnings", [])):
                res["problems"].append("missing data_quality warning containing %r" % w)
        for k, v in exp.get("meta", {}).items():
            got = ch.get("meta", {}).get(k)
            if k == "sources_count":
                got = len(ch.get("meta", {}).get("sources") or [])
            if got != v:
                res["problems"].append("meta.%s = %r (expected %r)" % (k, got, v))
        if "before_after" in exp:
            res["problems"].extend(check_before_after(exp["before_after"], ch, aligned))
        if "flags" in exp:
            res.update(check_flags(exp, ch, rows, aligned))
            res["problems"].extend(res.pop("flag_problems"))
        # Wording: dashboard template text, digest.md and the sentences the skill generates.
        hits = {}
        dash = out / "dashboard.html"
        if dash.exists():
            h = judge_hits(strip_dashboard(dash.read_text(encoding="utf-8")), template=True)
            if h:
                hits["dashboard.html"] = h
        dig = out / "digest.md"
        res["digest_checked"] = dig.exists()
        if dig.exists():
            h = judge_hits(dig.read_text(encoding="utf-8"))
            if h:
                hits["digest.md"] = h
        h = judge_hits("\n".join(generated_texts(ch)))
        if h:
            hits["change_history.json texts"] = h
        if hits:
            res["problems"].append("judgement words: %s" % json.dumps(hits, ensure_ascii=False))
    th = dict(DEFAULT_THRESHOLDS, **truth.get("thresholds", {}))
    for k in GATED:
        if res.get(k) is not None and k in th and res[k] + 1e-9 < th[k]:
            res["problems"].append("%s %.3f < threshold %.3f" % (k, res[k], th[k]))
    if res["problems"]:
        res["status"] = "FAIL"
    return res


def align(truth, rows):
    """Truth event i -> output event, keyed by (timestamp, first line of statement 0);
    equal keys are taken in event order. Returns (list of output events or None, problems)."""
    events = group_events(rows)
    pool = collections.defaultdict(collections.deque)
    for ev in events:
        pool[(ev[0].get("timestamp"), (ev[0].get("raw_summary") or "").split("\n")[0].strip())].append(ev)
    out, probs = [], []
    for ei, te in enumerate(truth["events"]):
        key = (te["ts"], te["statements"][0]["head"].strip())
        out.append(pool[key].popleft() if pool[key] else None)
        if out[-1] is None:
            probs.append("event %d (%s | %s) not found in output" % (ei, key[0], key[1][:50]))
    extra = sum(len(q) for q in pool.values())
    if extra:
        probs.append("%d output event(s) not in truth" % extra)
    return out, probs


def score(truth, rows, verbose):
    aligned, probs = align(truth, rows)
    n = cat_ok = sub_n = sub_ok = op_ok = val_n = val_ok = act_n = act_ok = 0
    confusion = collections.Counter()
    misses = []
    for ei, te in enumerate(truth["events"]):
        got = aligned[ei] or []
        by_si = {(r.get("statement_index") or 0): r for r in got}
        if got and len(got) != len(te["statements"]):
            probs.append("event %d: %d statements (truth %d)" % (ei, len(got), len(te["statements"])))
        for k in ("actor_type", "account_name"):
            if k in te and got:
                act_n += k == "actor_type"
                if got[0].get(k) == te[k]:
                    act_ok += k == "actor_type"
                else:
                    misses.append("e%d %s %r -> %r" % (ei, k, te[k], got[0].get(k)))
                    if k == "account_name":
                        probs.append("event %d account_name %r (truth %r)" % (ei, got[0].get(k), te[k]))
        for si, ts in enumerate(te["statements"]):
            n += 1
            r = by_si.get(si)
            if r is None:
                confusion[(ts["category"], "<missing>")] += 1
                continue
            if r.get("category") == ts["category"]:
                cat_ok += 1
            else:
                confusion[(ts["category"], r.get("category"))] += 1
                misses.append("e%d.s%d category %s/%s -> %s/%s | %s" % (ei, si, ts["category"], ts.get("subcategory"), r.get("category"), r.get("subcategory"), ts["head"][:60]))
            if "subcategory" in ts:
                sub_n += 1
                sub_ok += r.get("subcategory") == ts["subcategory"]
                if r.get("subcategory") != ts["subcategory"] and r.get("category") == ts["category"]:
                    misses.append("e%d.s%d subcategory %s -> %s | %s" % (ei, si, ts["subcategory"], r.get("subcategory"), ts["head"][:60]))
            if r.get("operation") == ts["operation"]:
                op_ok += 1
            else:
                misses.append("e%d.s%d operation %s -> %s | %s" % (ei, si, ts["operation"], r.get("operation"), ts["head"][:60]))
            if "old" in ts or "new" in ts:
                val_n += 1
                ok = all(num_eq(r.get(k + "_value_num"), ts[k]) for k in ("old", "new") if k in ts)
                val_ok += ok
                if not ok:
                    misses.append("e%d.s%d values old=%r new=%r -> old=%r new=%r | %s" % (ei, si, ts.get("old"), ts.get("new"), r.get("old_value_num"), r.get("new_value_num"), ts["head"][:60]))
    return {
        "statements": n,
        "category_accuracy": cat_ok / n if n else None,
        "subcategory_accuracy": sub_ok / sub_n if sub_n else None,
        "operation_accuracy": op_ok / n if n else None,
        "value_match_rate": val_ok / val_n if val_n else None,
        "actor_accuracy": act_ok / act_n if act_n else None,
        "value_statements": val_n,
        "confusion": {"%s -> %s" % k: v for k, v in sorted(confusion.items(), key=lambda kv: -kv[1])},
        "misses": misses if verbose else misses[:8],
        "align_problems": probs,
        "_aligned": aligned,
    }


def check_before_after(spec, ch, aligned):
    """spec: {"<event>.<statement>": {"level":..., "before": {...}, "after": {...}}}; null-valued metrics must stay null."""
    probs = []
    ba = ch.get("before_after")
    if not isinstance(ba, dict):
        return ["change_history.json has no before_after object"]
    for key, want in spec.items():
        ei, si = (int(x) for x in key.split("."))
        try:
            cid = [r for r in (aligned[ei] or []) if (r.get("statement_index") or 0) == si][0]["change_id"]
        except (IndexError, KeyError, TypeError):
            probs.append("before_after %s: no such row" % key)
            continue
        got = ba.get(cid)
        if want is None:
            if got:
                probs.append("before_after %s: expected no window, got one" % key)
            continue
        if not got:
            probs.append("before_after %s: missing" % key)
            continue
        for k in ("level", "window_days"):
            if k in want and got.get(k) != want[k]:
                probs.append("before_after %s.%s = %r (expected %r)" % (key, k, got.get(k), want[k]))
        for side in ("before", "after"):
            for m, v in want.get(side, {}).items():
                if not num_eq(got.get(side, {}).get(m), v):
                    probs.append("before_after %s.%s.%s = %r (expected %r)" % (key, side, m, got.get(side, {}).get(m), v))
        if "overlapping_count" in want and (got.get("overlapping") or {}).get("count") != want["overlapping_count"]:
            probs.append("before_after %s.overlapping.count = %r (expected %r)" % (key, (got.get("overlapping") or {}).get("count"), want["overlapping_count"]))
    return probs


def _flag_matches(f, spec, ref_of):
    """spec keys: changes (truth "event.statement" refs that must all be among the flag's
    change_ids), campaign, ad_group, account, user (equal to the flag's field)."""
    if "changes" in spec:
        have = {ref_of.get(c) for c in f.get("change_ids") or []}
        if not set(spec["changes"]) <= have:
            return False
    for k in ("campaign", "ad_group", "account", "user"):
        if k in spec and (f.get(k) or None) != spec[k]:
            return False
    return True


def check_flags(exp, ch, rows, aligned):
    """2026-11-19. exp["flags"]: [{rule, count, unit: "flags" (default) | "events", match: [spec, ...],
    events: [truth event index, ...]}]. Per rule: precision = matched / raised, recall = matched /
    expected; the case scores the lowest of each. No flags list in the output is a failure."""
    probs, misses, per_rule = [], [], {}
    res = {"flag_precision": 0.0, "flag_recall": 0.0, "flag_rules": per_rule, "flag_misses": misses, "flag_problems": probs}
    flags = ch.get("flags")
    if not isinstance(flags, list):
        probs.append("change_history.json has no flags list (truth expects flags)")
        return res
    ref_of, ev_of, ev_of_change = {}, {}, {}
    for ei, ev in enumerate(aligned):
        for r in ev or []:
            ref_of[r.get("change_id")] = "%d.%d" % (ei, r.get("statement_index") or 0)
            ev_of[r.get("event_id")] = ei
            ev_of_change[r.get("change_id")] = ei
    row_ids = {r.get("change_id") for r in rows}
    tiers = dict(RULE_TIERS)
    # The data contract's shape: known rule, its tier, both texts, change_ids that exist, sorted by tier.
    for f in flags:
        tag = "flag %s (%s)" % (f.get("id"), f.get("rule"))
        if f.get("rule") not in tiers:
            probs.append("%s: unknown rule" % tag)
            continue
        if f.get("tier") != tiers[f["rule"]]:
            probs.append("%s: tier %r (contract: %s)" % (tag, f.get("tier"), tiers[f["rule"]]))
        text = f.get("text")
        if not (isinstance(text, dict) and text.get("tr") and text.get("en")):
            probs.append("%s: text.tr / text.en missing" % tag)
        if not isinstance(f.get("change_ids"), list) or not isinstance(f.get("fact"), dict):
            probs.append("%s: change_ids must be a list and fact an object" % tag)
        elif any(c not in row_ids for c in f["change_ids"]):
            probs.append("%s: change_ids not found in changes.jsonl" % tag)
    rank = [0 if f.get("tier") == "attention" else 1 for f in flags]
    if rank != sorted(rank):
        probs.append("flags are not sorted attention first, then info")
    rc_list = [r for r in ch.get("rules_config") or [] if isinstance(r, dict)]
    rc = {r.get("rule"): r for r in rc_list}
    if [r.get("rule") for r in rc_list] != [r for r, _ in RULE_TIERS]:
        probs.append("rules_config lists %s (expected the 12 contract rules in order)" % [r.get("rule") for r in rc_list])
    rules_exp = exp.get("rules") or {}
    for name, params in rules_exp.get("params", {}).items():
        got = (rc.get(name) or {}).get("params") or {}
        for k, v in params.items():
            if not (num_eq(got.get(k), v) if isinstance(v, (int, float)) and not isinstance(v, bool) else got.get(k) == v):
                probs.append("rules_config %s.params.%s = %r (expected %r)" % (name, k, got.get(k), v))
    for name in rules_exp.get("disabled", []):
        r = rc.get(name) or {}
        reason = r.get("disabled_reason") or {}
        if r.get("enabled") is not False or not (isinstance(reason, dict) and reason.get("tr") and reason.get("en")):
            probs.append("rules_config %s: expected disabled with a tr/en disabled_reason" % name)
    by_rule = collections.defaultdict(list)
    for f in flags:
        by_rule[f.get("rule")].append(f)
    for spec in exp["flags"]:
        rule, got = spec["rule"], by_rule.get(spec["rule"], [])
        if spec.get("count", 0) > 0 and rule in rc and rc[rule].get("enabled") is False:
            probs.append("rules_config %s is disabled but the truth expects hits" % rule)
        if spec.get("unit") == "events":
            want, have = set(spec.get("events") or []), set()
            for f in got:
                eids = f.get("event_ids") or []
                have |= {ev_of.get(e, "?") for e in eids} if eids else {ev_of_change.get(c, "?") for c in f.get("change_ids") or []}
            tp = len(want & have)
            p, r = (tp / len(have) if have else 1.0), (tp / len(want) if want else 1.0)
            n_got, n_want = len(have), len(want)
            if p < 1 or r < 1:
                misses.append("%s events raised %s, expected %s" % (rule, sorted(map(str, have)), sorted(map(str, want))))
        else:
            n_want, matchers, free, tp = spec.get("count", 0), spec.get("match") or [], list(got), 0
            for m in matchers:
                hit = next((f for f in free if _flag_matches(f, m, ref_of)), None)
                if hit is None:
                    misses.append("%s expected, not raised: %s" % (rule, json.dumps(m, ensure_ascii=False)))
                    continue
                free.remove(hit)
                tp += 1
            tp += min(max(n_want - len(matchers), 0), len(free))
            n_got = len(got)
            p, r = (tp / n_got if n_got else 1.0), (tp / n_want if n_want else 1.0)
            if p < 1 and matchers:
                for f in free:
                    misses.append("%s raised, not expected: %s %s changes %s" % (rule, f.get("id"), f.get("ts_first"), sorted(ref_of.get(c, "?") for c in f.get("change_ids") or [])))
        per_rule[rule] = {"raised": n_got, "expected": n_want, "precision": p, "recall": r}
        if p < 1 or r < 1:
            probs.append("flags %s: raised %d, expected %d (precision %.2f, recall %.2f)" % (rule, n_got, n_want, p, r))
    if per_rule:
        res["flag_precision"] = min(v["precision"] for v in per_rule.values())
        res["flag_recall"] = min(v["recall"] for v in per_rule.values())
    es = ch.get("exec_summary")
    if isinstance(es, dict):
        totals = es.get("totals") or {}
        for tier in ("attention", "info"):
            n = sum(1 for f in flags if f.get("tier") == tier)
            if totals.get(tier + "_flags") != n:
                probs.append("exec_summary.totals.%s_flags = %r, flags list has %d" % (tier, totals.get(tier + "_flags"), n))
    for section, want in (exp.get("exec_summary") or {}).items():
        got = (es or {}).get(section) if isinstance(es, dict) else None
        if not isinstance(got, dict):
            probs.append("exec_summary.%s missing" % section)
            continue
        for k, v in want.items():
            g = got.get(k)
            if k == "decrease_total" and isinstance(g, (int, float)):
                g = abs(g)
            if not num_eq(g, v):
                probs.append("exec_summary.%s.%s = %r (expected %r)" % (section, k, got.get(k), v))
    return res


def pct(x):
    return "  n/a " if x is None else "%5.1f%%" % (100 * x)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--private-dir", default=None, help="extra private cases (same layout, never committed); default %s" % default_private_dir())
    ap.add_argument("--synthetic-only", action="store_true")
    ap.add_argument("--case", action="append", help="run only this case name (repeatable)")
    ap.add_argument("--verbose", action="store_true", help="list every mismatch")
    ap.add_argument("--json", help="also write the full results to this file")
    ap.add_argument("--allow-skip", action="store_true", help="a SKIPPED case does not fail the run (default: it does)")
    ap.add_argument("--skill", default=str(SKILL), help="ads_change_history.py to evaluate (default: this repo's)")
    a = ap.parse_args()
    dirs = [("synthetic", d) for d in sorted(CASES.iterdir()) if (d / "truth.json").exists()]
    priv = Path(a.private_dir) if a.private_dir else default_private_dir()
    if not a.synthetic_only and priv.is_dir():
        dirs += [("private", d) for d in sorted(priv.iterdir()) if (d / "truth.json").exists() and (d / "args.json").exists()]
    if a.case:
        dirs = [x for x in dirs if x[1].name in a.case]
    results = []
    print("%-24s %-9s %-7s %6s %7s %7s %7s %7s %7s %7s %7s" % ("case", "kind", "status", "stmts", "cat", "subcat", "op", "value", "actor", "flagP", "flagR"))
    for kind, d in dirs:
        r = run_case(d, a.verbose, Path(a.skill).resolve())
        r["kind"] = kind
        results.append(r)
        print("%-24s %-9s %-7s %6s %7s %7s %7s %7s %7s %7s %7s" % (r["case"], kind, r["status"], r.get("statements", "-"),
              pct(r.get("category_accuracy")), pct(r.get("subcategory_accuracy")), pct(r.get("operation_accuracy")), pct(r.get("value_match_rate")), pct(r.get("actor_accuracy")),
              pct(r.get("flag_precision")), pct(r.get("flag_recall"))))
        misses = r.get("misses", []) + r.get("flag_misses", [])
        show = r["problems"] + (misses if a.verbose or r["status"] == "FAIL" else [])
        if kind == "private" and not a.verbose:
            show = [x for x in r["problems"] if not x.startswith("judgement")] + ["(%d mismatches; --verbose to list)" % len(misses)]
        for x in show:
            print("    - " + x)
        if r.get("confusion") and r["status"] == "FAIL":
            print("    confusion (truth -> output): " + json.dumps(r["confusion"]))
    scored = [r for r in results if r["status"] != "SKIPPED" and r.get("statements")]
    tot = sum(r["statements"] for r in scored)
    if tot:
        def w(k):
            num = den = 0
            for r in scored:
                if r.get(k) is None:
                    continue
                base = r["value_statements"] if k == "value_match_rate" else r["statements"]
                num += r[k] * base
                den += base
            return num / den if den else None
        print("%-24s %-9s %-7s %6d %7s %7s %7s %7s" % ("OVERALL", "", "", tot, pct(w("category_accuracy")), pct(w("subcategory_accuracy")), pct(w("operation_accuracy")), pct(w("value_match_rate"))))
    counts = collections.Counter(r["status"] for r in results)
    print("passed %d, failed %d, skipped %d" % (counts["PASS"], counts["FAIL"], counts["SKIPPED"]))
    if any(r.get("digest_checked") is False for r in results):
        print("note: digest.md not produced by this skill version; its wording check was skipped")
    if a.json:
        Path(a.json).write_text(json.dumps(results, indent=1, ensure_ascii=False), encoding="utf-8")
    # 2026-11-15: a skipped case is a failure unless --allow-skip.
    return 1 if counts["FAIL"] or (counts["SKIPPED"] and not a.allow_skip) else 0


if __name__ == "__main__":
    sys.exit(main())
