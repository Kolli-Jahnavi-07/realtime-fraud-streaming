"""Live run: the streaming job is already running while transactions arrive.

Starts the job with a 1-second trigger, then streams 30 minutes of simulated traffic
into it (replayed about 5x faster than real time), waits for the job to catch up,
and scores detection quality and end-to-end latency (transaction sent -> alert
written). Writes docs/live_run.json.

    python scripts/live_run.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV = {**os.environ, "PYTHONPATH": os.path.join(ROOT, "src")}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=404)
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--batch-size", type=int, default=400, help="events per landing file (~10 s of traffic)")
    ap.add_argument("--pause", type=float, default=2.0, help="seconds between files")
    ap.add_argument("--out", default="docs/live_run.json")
    a = ap.parse_args()

    base = os.path.join(ROOT, "data", "live")
    shutil.rmtree(base, ignore_errors=True)
    land, lake, ckpt, truth = (os.path.join(base, x) for x in ("landing", "lake", "checkpoints", "truth.json"))
    os.makedirs(land)

    job = subprocess.Popen(
        [sys.executable, "-m", "fraudstream.job", "--input", land, "--lake", lake, "--checkpoints", ckpt,
         "--trigger", "1 second", "--max-files-per-trigger", "5"],
        cwd=ROOT, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(25)  # let Spark start all queries before traffic begins
        t0 = time.time()
        subprocess.run(
            [sys.executable, "-m", "fraudstream.simulator", "--seed", str(a.seed), "--minutes", str(a.minutes),
             "--attacks-per-min", "3", "--out", land, "--truth", truth,
             "--batch-size", str(a.batch_size), "--pause", str(a.pause)],
            cwd=ROOT, env=ENV, check=True, stdout=subprocess.DEVNULL,
        )
        streamed = time.time() - t0
        files = len([f for f in os.listdir(land) if f.endswith(".json")])
        # the job has caught up once no query has committed anything new for 20 s
        def commit_count() -> int:
            return sum(len(os.listdir(os.path.join(ckpt, q, "commits")))
                       for q in os.listdir(ckpt) if os.path.isdir(os.path.join(ckpt, q, "commits")))

        last, quiet_since, deadline = -1, time.time(), time.time() + 600
        while time.time() < deadline:
            n = commit_count()
            if n != last:
                last, quiet_since = n, time.time()
            elif time.time() - quiet_since > 20:
                break
            time.sleep(1)
    finally:
        job.terminate()
        try:
            job.wait(timeout=60)
        except subprocess.TimeoutExpired:
            job.kill()

    from fraudstream.evaluate import score  # noqa: E402

    r = score(lake, truth, land)
    r["run"] = {"seed": a.seed, "event_minutes": a.minutes, "wall_seconds_streaming": round(streamed, 1),
                "speedup_vs_real_time": round(a.minutes * 60 / streamed, 1), "files": files, "trigger": "1 second"}
    os.makedirs(os.path.dirname(os.path.join(ROOT, a.out)), exist_ok=True)
    json.dump(r, open(os.path.join(ROOT, a.out), "w"), indent=2, default=str)
    print(json.dumps({k: r[k] for k in ("summary", "integrity", "run")}, indent=2))


if __name__ == "__main__":
    main()
