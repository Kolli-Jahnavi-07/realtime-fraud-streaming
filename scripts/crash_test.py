"""Crash test: kill the streaming job hard (SIGKILL) in the middle of the stream,
restart it, and prove that nothing was lost or written twice.

Spark replays the batch that was in flight when the process died. The Delta sinks
recognise the replayed batch id (a Delta application transaction) and skip it, so
the final tables are identical to an uninterrupted run. Writes docs/crash_test.json.

    python scripts/crash_test.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = {**os.environ, "PYTHONPATH": os.path.join(ROOT, "src")}


def commits(ckpt: str) -> int:
    return sum(len(os.listdir(os.path.join(ckpt, q, "commits")))
               for q in os.listdir(ckpt) if os.path.isdir(os.path.join(ckpt, q, "commits"))) if os.path.isdir(ckpt) else 0


def run_job(land: str, lake: str, ckpt: str, kill_after_commits: int | None = None) -> int:
    """Run the job over all files; optionally SIGKILL it after N committed batches."""
    p = subprocess.Popen(
        [sys.executable, "-m", "fraudstream.job", "--available-now", "--max-files-per-trigger", "1",
         "--input", land, "--lake", lake, "--checkpoints", ckpt],
        cwd=ROOT, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )
    if kill_after_commits is None:
        return p.wait(timeout=900)
    while p.poll() is None and commits(ckpt) < kill_after_commits:
        time.sleep(0.2)
    os.killpg(p.pid, signal.SIGKILL)  # no shutdown hooks, no flush: a real crash
    p.wait()
    return -9


def _count_lines(path: str) -> int:
    return sum(1 for _ in open(path)) if os.path.exists(path) else 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=505)
    ap.add_argument("--minutes", type=int, default=15)
    ap.add_argument("--out", default="docs/crash_test.json")
    a = ap.parse_args()

    base = os.path.join(ROOT, "data", "crash")
    shutil.rmtree(base, ignore_errors=True)
    land, truth = os.path.join(base, "landing"), os.path.join(base, "truth.json")
    subprocess.run([sys.executable, "-m", "fraudstream.simulator", "--seed", str(a.seed), "--minutes", str(a.minutes),
                    "--attacks-per-min", "3", "--batch-size", "1500", "--out", land, "--truth", truth],
                   cwd=ROOT, env=ENV, check=True, stdout=subprocess.DEVNULL)
    files = len([f for f in os.listdir(land) if f.endswith(".json")])

    # reference: one uninterrupted run
    ref_lake, ref_ckpt = os.path.join(base, "ref", "lake"), os.path.join(base, "ref", "ckpt")
    run_job(land, ref_lake, ref_ckpt)

    # crashed run: kill twice part-way, then let it finish
    lake, ckpt = os.path.join(base, "crash", "lake"), os.path.join(base, "crash", "ckpt")
    kills = 0
    for at in (files * 5 // 3, files * 10 // 3):  # five queries commit once per file
        run_job(land, lake, ckpt, kill_after_commits=at)
        kills += 1
    run_job(land, lake, ckpt)

    from fraudstream.evaluate import load_alerts, score  # noqa: E402
    from deltalake import DeltaTable  # noqa: E402

    ref, got = score(ref_lake, truth, land), score(lake, truth, land)

    def alert_keys(path: str) -> list[str]:
        return sorted(load_alerts(path)["alert_id"])

    def bronze_ids(path: str) -> list[str]:
        return sorted(DeltaTable(os.path.join(path, "bronze_transactions")).to_pandas(columns=["txn_id"])["txn_id"])

    result = {
        "files": files,
        "hard_kills": kills,
        "uninterrupted": {"bronze_rows": ref["integrity"]["bronze_rows"], "alerts": ref["summary"]["alerts"]},
        "after_crashes": {"bronze_rows": got["integrity"]["bronze_rows"], "alerts": got["summary"]["alerts"]},
        "identical_bronze": bronze_ids(ref_lake) == bronze_ids(lake),
        "identical_alerts": alert_keys(ref_lake) == alert_keys(lake),
        "duplicates_after_crashes": got["integrity"]["bronze_duplicate_txn_ids"] + got["integrity"]["duplicate_alert_ids"],
        "missing_after_crashes": got["integrity"]["missing_from_bronze"],
    }
    os.makedirs(os.path.dirname(os.path.join(ROOT, a.out)), exist_ok=True)
    json.dump(result, open(os.path.join(ROOT, a.out), "w"), indent=2)
    print(json.dumps(result, indent=2))
    ok = result["identical_bronze"] and result["identical_alerts"] and not result["duplicates_after_crashes"] \
        and not result["missing_after_crashes"]
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
