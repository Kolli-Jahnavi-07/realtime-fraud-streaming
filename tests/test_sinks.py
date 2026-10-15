"""Exactly-once sink behaviour: replays are skipped, repeat evidence is one alert."""

from datetime import datetime

import pandas as pd
from deltalake import DeltaTable

from fraudstream import sinks


def alert(spark, card="C1", end_min=10, rule="card_testing"):
    row = {"card_id": card, "rule": rule, "window_start": datetime(2026, 10, 1, 12, end_min - 3),
           "window_end": datetime(2026, 10, 1, 12, end_min), "evidence": "x", "txn_count": 6,
           "amount_total": 9.0, "trigger_sent_at": datetime(2026, 10, 1, 12, end_min)}
    return spark.createDataFrame(pd.DataFrame([row]))


def rows(path):
    return len(DeltaTable(path).to_pandas()) if DeltaTable.is_deltatable(path) else 0


def test_replayed_batch_is_skipped(spark, tmp_path):
    path = str(tmp_path / "lake" / "alerts_card_testing")
    sink = sinks.alert_sink(path, "alerts_card_testing")
    sink(alert(spark), 0)
    sink(alert(spark, card="C2"), 0)  # same batch id again: what Spark does after a crash
    assert rows(path) == 1
    assert (tmp_path / "lake" / "_ops" / "skipped_replays.jsonl").exists()


def test_repeat_evidence_within_cooldown_is_one_alert(spark, tmp_path):
    path = str(tmp_path / "alerts_card_testing")
    sink = sinks.alert_sink(path, "alerts_card_testing")
    sink(alert(spark, end_min=10), 0)
    sink(alert(spark, end_min=11), 1)  # sliding window updated a minute later
    sink(alert(spark, end_min=30), 2)  # a new incident after the cooldown
    assert rows(path) == 2


def test_new_alerts_are_published_once(spark, tmp_path):
    published = []
    sink = sinks.alert_sink(str(tmp_path / "a"), "a", publish=lambda df: published.extend(df["card_id"]))
    sink(alert(spark, card="C1"), 0)
    sink(alert(spark, card="C1"), 0)  # replay
    sink(alert(spark, card="C1", end_min=12), 1)  # repeat within cooldown
    assert published == ["C1"]


def test_append_sink_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "bronze")
    sink = sinks.delta_append(path, "bronze")
    df = spark.createDataFrame(pd.DataFrame({"txn_id": ["a", "b"]}))
    sink(df, 0)
    sink(df, 0)
    sink(df, 1)
    assert rows(path) == 4
