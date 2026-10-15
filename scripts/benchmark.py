"""Score the detectors on several unseen seeds and write docs/benchmark.json.

Each seed: simulate traffic -> run the streaming job over it -> score against the
hidden answer key. Seeds here were never used while developing the thresholds.

    python scripts/benchmark.py --seeds 101 202 303 --minutes 30
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = {**os.environ, "PYTHONPATH": os.path.join(ROOT, "src")}


def sh(*args: str) -> None:
    subprocess.run([sys.executable, *args], check=True, cwd=ROOT, env=ENV,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, nargs="+", default=[101, 202, 303])
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--attacks-per-min", type=float, default=3.0)
    ap.add_argument("--out", default="docs/benchmark.json")
    a = ap.parse_args()

    runs = []
    for seed in a.seeds:
        base = os.path.join(ROOT, "data", "bench", str(seed))
        shutil.rmtree(base, ignore_errors=True)
        land, lake, ckpt, truth = (os.path.join(base, x) for x in ("landing", "lake", "checkpoints", "truth.json"))
        sh("-m", "fraudstream.simulator", "--seed", str(seed), "--minutes", str(a.minutes),
           "--attacks-per-min", str(a.attacks_per_min), "--out", land, "--truth", truth)
        sh("-m", "fraudstream.job", "--available-now", "--max-files-per-trigger", "4",
           "--input", land, "--lake", lake, "--checkpoints", ckpt)
        from fraudstream.evaluate import score  # noqa: E402

        r = score(lake, truth, land)
        r["seed"] = seed
        runs.append(r)
        s = r["summary"]
        print(f"seed {seed}: caught {s['caught']}/{s['attacks']}  precision {s['precision']}  "
              f"sprees flagged {s['benign_sprees_flagged']}/{s['benign_sprees']}")

    agg = {"attacks": 0, "caught": 0, "alerts": 0, "true_alerts": 0, "benign_sprees": 0, "benign_sprees_flagged": 0}
    by_kind: dict = {}
    for r in runs:
        agg["attacks"] += r["summary"]["attacks"]
        agg["caught"] += r["summary"]["caught"]
        agg["benign_sprees"] += r["summary"]["benign_sprees"]
        agg["benign_sprees_flagged"] += r["summary"]["benign_sprees_flagged"]
        for rule, p in r["precision_by_rule"].items():
            agg["alerts"] += p["alerts"]
            agg["true_alerts"] += p["true"]
            k = by_kind.setdefault(rule, {"attacks": 0, "caught": 0, "alerts": 0, "true": 0})
            k["alerts"] += p["alerts"]
            k["true"] += p["true"]
        for kind, v in r["recall_by_attack"].items():
            by_kind[kind]["attacks"] += v["attacks"]
            by_kind[kind]["caught"] += v["caught"]
    agg["recall"] = round(agg["caught"] / agg["attacks"], 3)
    agg["precision"] = round(agg["true_alerts"] / agg["alerts"], 3)
    out = {"seeds": a.seeds, "minutes_per_seed": a.minutes, "overall": agg, "by_rule": by_kind,
           "integrity_ok": all(r["integrity"].get("missing_from_bronze", 0) == 0
                               and r["integrity"]["bronze_duplicate_txn_ids"] == 0
                               and r["integrity"]["duplicate_alert_ids"] == 0 for r in runs),
           "runs": runs}
    os.makedirs(os.path.dirname(os.path.join(ROOT, a.out)), exist_ok=True)
    json.dump(out, open(os.path.join(ROOT, a.out), "w"), indent=2, default=str)
    print(json.dumps({k: out[k] for k in ("overall", "by_rule", "integrity_ok")}, indent=2))


if __name__ == "__main__":
    main()
