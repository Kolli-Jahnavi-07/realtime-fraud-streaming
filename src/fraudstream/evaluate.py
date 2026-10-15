"""Score the stream's alerts against the hidden answer key.

An alert *matches* an attack when it is for the same card and its trigger time
(window_end) falls inside the attack, with a small allowance for the detection
window. From that:

* recall per attack type   share of attacks caught by any rule
* precision per rule       share of a rule's alerts that point at a real attack
* false alarms             alerts that hit a benign shopping spree or ordinary traffic
* latency                  detected_at - sent_at of the transaction that completed the
                           evidence (meaningful in live runs; batch replays queue data)
* integrity                bronze holds each transaction exactly once (no loss, no
                           duplicates), and no alert was written twice

    python -m fraudstream.evaluate --lake data/lake --truth data/ground_truth/attacks.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd
from deltalake import DeltaTable

RULES = ["card_testing", "impossible_travel", "account_takeover"]
SLACK = pd.Timedelta("2min")


def load_alerts(lake: str) -> pd.DataFrame:
    frames = []
    for rule in RULES:
        path = os.path.join(lake, f"alerts_{rule}")
        if DeltaTable.is_deltatable(path):
            frames.append(DeltaTable(path).to_pandas())
    if not frames:
        return pd.DataFrame(columns=["card_id", "rule", "window_start", "window_end", "detected_at", "trigger_sent_at", "alert_id"])
    a = pd.concat(frames, ignore_index=True)
    for c in ("window_start", "window_end", "detected_at", "trigger_sent_at"):
        a[c] = pd.to_datetime(a[c], utc=True)
    return a


def load_truth(path: str) -> pd.DataFrame:
    t = pd.DataFrame(json.load(open(path)))
    t["start"] = pd.to_datetime(t["start"], utc=True)
    t["end"] = pd.to_datetime(t["end"], utc=True)
    return t


def match(alerts: pd.DataFrame, truth: pd.DataFrame) -> pd.DataFrame:
    """Attach the attack (if any) each alert points at."""
    m = alerts.merge(truth[["attack_id", "kind", "card_id", "start", "end"]], on="card_id", how="left")
    hit = (m["window_end"] >= m["start"] - SLACK) & (m["window_end"] <= m["end"] + SLACK)
    m.loc[~hit, ["attack_id", "kind"]] = None
    m = m.sort_values("attack_id", na_position="last").drop_duplicates("alert_id")
    return m


def score(lake: str, truth_path: str, landing: str | None = None) -> dict:
    alerts = load_alerts(lake)
    truth = load_truth(truth_path)
    m = match(alerts, truth)
    fraud = truth[truth.kind != "benign_spree"]

    recall = {}
    for kind in RULES:
        ids = set(fraud.loc[fraud.kind == kind, "attack_id"])
        caught = set(m.loc[m.kind == kind, "attack_id"].dropna())
        recall[kind] = {"attacks": len(ids), "caught": len(ids & caught)}

    precision = {}
    for rule in RULES:
        r = m[m.rule == rule]
        real = r["kind"].notna() & (r["kind"] != "benign_spree")
        precision[rule] = {"alerts": int(len(r)), "true": int(real.sum()),
                           "benign_spree": int((r["kind"] == "benign_spree").sum()),
                           "ordinary_traffic": int(r["kind"].isna().sum())}

    lat = (alerts["detected_at"] - alerts["trigger_sent_at"]).dt.total_seconds()
    sprees = truth[truth.kind == "benign_spree"]
    flagged_sprees = set(m.loc[m.kind == "benign_spree", "attack_id"])

    integrity: dict = {"duplicate_alert_ids": int(alerts["alert_id"].duplicated().sum())}
    bronze = os.path.join(lake, "bronze_transactions")
    if DeltaTable.is_deltatable(bronze):
        ids = DeltaTable(bronze).to_pandas(columns=["txn_id"])["txn_id"]
        integrity["bronze_rows"] = int(len(ids))
        integrity["bronze_duplicate_txn_ids"] = int(ids.duplicated().sum())
        if landing and os.path.isdir(landing):
            sent = set()
            n_lines = 0
            for f in sorted(os.listdir(landing)):
                if f.endswith(".json"):
                    for line in open(os.path.join(landing, f)):
                        if line.strip():
                            n_lines += 1
                            sent.add(json.loads(line)["txn_id"])
            integrity["events_delivered"] = n_lines
            integrity["unique_transactions_sent"] = len(sent)
            integrity["missing_from_bronze"] = len(sent - set(ids))

    total_attacks = sum(v["attacks"] for v in recall.values())
    total_caught = sum(v["caught"] for v in recall.values())
    total_alerts = sum(v["alerts"] for v in precision.values())
    total_true = sum(v["true"] for v in precision.values())
    return {
        "summary": {
            "attacks": total_attacks,
            "caught": total_caught,
            "recall": round(total_caught / total_attacks, 3) if total_attacks else None,
            "alerts": total_alerts,
            "precision": round(total_true / total_alerts, 3) if total_alerts else None,
            "benign_sprees": int(len(sprees)),
            "benign_sprees_flagged": len(flagged_sprees),
            "latency_p50_s": round(float(lat.median()), 2) if len(lat) else None,
            "latency_p95_s": round(float(lat.quantile(0.95)), 2) if len(lat) else None,
        },
        "recall_by_attack": recall,
        "precision_by_rule": precision,
        "integrity": integrity,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--truth", default="data/ground_truth/attacks.json")
    ap.add_argument("--landing", default="data/landing")
    ap.add_argument("--out", help="write the report as JSON")
    ap.add_argument("--min-recall", type=float, default=0.0, help="exit 1 if overall recall is below this")
    ap.add_argument("--min-precision", type=float, default=0.0, help="exit 1 if overall precision is below this")
    a = ap.parse_args()
    report = score(a.lake, a.truth, a.landing)
    print(json.dumps(report, indent=2))
    if a.out:
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        json.dump(report, open(a.out, "w"), indent=2)
    s, i = report["summary"], report["integrity"]
    ok = (s["recall"] or 0) >= a.min_recall and (s["precision"] or 0) >= a.min_precision
    ok = ok and i.get("duplicate_alert_ids", 0) == 0 and i.get("bronze_duplicate_txn_ids", 0) == 0 \
        and i.get("missing_from_bronze", 0) == 0
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
