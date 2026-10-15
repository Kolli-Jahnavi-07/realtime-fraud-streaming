"""End-to-end run on a real Kafka broker (docker-compose locally, a service container in CI).

1. publish simulated transactions to the `card-transactions` topic, keyed by card
2. run the streaming job against Kafka until it has consumed everything
3. read the `fraud-alerts` topic and check it matches the Delta alert tables
4. score detection against the hidden answer key

    docker compose up -d kafka
    python scripts/kafka_e2e.py --bootstrap localhost:9092
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


def create_topics(bootstrap: str, topics: list[str]) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    for t in topics:
        try:
            admin.create_topics([NewTopic(t, num_partitions=4, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    admin.close()


def read_alert_topic(bootstrap: str, topic: str) -> list[dict]:
    from kafka import KafkaConsumer

    c = KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset="earliest",
                      consumer_timeout_ms=10_000, value_deserializer=lambda v: json.loads(v))
    out = [m.value for m in c]
    c.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--seed", type=int, default=606)
    ap.add_argument("--minutes", type=int, default=10)
    ap.add_argument("--min-recall", type=float, default=0.9)
    a = ap.parse_args()

    base = os.path.join(ROOT, "data", "kafka_e2e")
    shutil.rmtree(base, ignore_errors=True)
    lake, ckpt, truth = (os.path.join(base, x) for x in ("lake", "checkpoints", "truth.json"))

    create_topics(a.bootstrap, ["card-transactions", "fraud-alerts"])
    subprocess.run([sys.executable, "-m", "fraudstream.simulator", "--sink", "kafka", "--bootstrap", a.bootstrap,
                    "--seed", str(a.seed), "--minutes", str(a.minutes), "--attacks-per-min", "3", "--truth", truth],
                   cwd=ROOT, env=ENV, check=True)
    subprocess.run([sys.executable, "-m", "fraudstream.job", "--source", "kafka", "--bootstrap", a.bootstrap,
                    "--available-now", "--lake", lake, "--checkpoints", ckpt],
                   cwd=ROOT, env=ENV, check=True, timeout=1200)

    from fraudstream.evaluate import load_alerts, score  # noqa: E402

    report = score(lake, truth)
    delta_ids = set(load_alerts(lake)["alert_id"])
    topic_ids = {m["alert_id"] for m in read_alert_topic(a.bootstrap, "fraud-alerts")}
    result = {"summary": report["summary"], "alerts_in_delta": len(delta_ids), "alerts_on_topic": len(topic_ids),
              "topic_matches_delta": delta_ids == topic_ids}
    print(json.dumps(result, indent=2))
    ok = result["topic_matches_delta"] and (report["summary"]["recall"] or 0) >= a.min_recall \
        and report["integrity"]["bronze_duplicate_txn_ids"] == 0 and report["integrity"]["duplicate_alert_ids"] == 0
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
