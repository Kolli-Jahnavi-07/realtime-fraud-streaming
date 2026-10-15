"""Exactly-once sinks for micro-batches.

Spark guarantees each micro-batch is delivered at least once; after a crash the
last batch can be replayed. These sinks make the replay harmless:

* Delta writes carry a Delta *application transaction* (app_id = query name,
  version = batch id). If the table already has that version, the batch is skipped.
* Alerts are upserted by alert_id and suppressed by a per-card cooldown, so a
  windowed rule that keeps updating, or overlapping sliding windows, still yield
  one alert per incident.
* New alerts (and only new ones) are published downstream, e.g. to a Kafka topic.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable

import pandas as pd
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from pyspark.sql import DataFrame

# After a card is flagged, further evidence of the same incident is not a new alert.
# Impossible travel pairs every transaction in the last hour, so its cooldown matches
# that lookback (the cardholder's next purchase at home is the same incident).
COOLDOWN = {"card_testing": pd.Timedelta("10min"), "account_takeover": pd.Timedelta("10min"),
            "impossible_travel": pd.Timedelta("60min")}


def _already_committed(path: str, app_id: str, batch_id: int) -> bool:
    """True if this batch id was already committed to the table (a replay after a crash)."""
    if not DeltaTable.is_deltatable(path):
        return False
    v = DeltaTable(path).transaction_version(app_id)
    if v is not None and v >= batch_id:
        _log_skipped_replay(path, app_id, batch_id)
        return True
    return False


def _log_skipped_replay(path: str, app_id: str, batch_id: int) -> None:
    """Record skipped replays next to the lake, so the crash test can show they happened."""
    log = os.path.join(os.path.dirname(path), "_ops", "skipped_replays.jsonl")
    os.makedirs(os.path.dirname(log), exist_ok=True)
    with open(log, "a") as f:
        f.write(json.dumps({"query": app_id, "batch_id": batch_id, "at": pd.Timestamp.now(tz="UTC").isoformat()}) + "\n")


def _write(path: str, table: pa.Table, app_id: str, batch_id: int, mode: str = "append") -> None:
    write_deltalake(path, table, mode=mode,
                    commit_properties=CommitProperties(app_transactions=[Transaction(app_id, batch_id)]))


def delta_append(path: str, app_id: str) -> Callable[[DataFrame, int], None]:
    def sink(df: DataFrame, batch_id: int) -> None:
        if _already_committed(path, app_id, batch_id):
            return
        pdf = df.toPandas()
        if pdf.empty:
            return
        _write(path, pa.Table.from_pandas(pdf, preserve_index=False), app_id, batch_id)
    return sink


def alert_sink(path: str, app_id: str, publish: Callable[[pd.DataFrame], None] | None = None) -> Callable[[DataFrame, int], None]:
    """Upsert alerts for one rule; apply cooldown; publish only brand-new alerts."""

    def sink(df: DataFrame, batch_id: int) -> None:
        if _already_committed(path, app_id, batch_id):
            return
        pdf = df.toPandas()
        if pdf.empty:
            return
        # wall-clock time the alert is ready to publish: the end of the latency measurement
        pdf["detected_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
        # window_end is when the evidence was complete (the triggering transaction), so it
        # is the right clock for de-duplication: overlapping sliding windows and multiple
        # join pairs for the same trigger all collapse into one alert.
        pdf["alert_id"] = pdf["card_id"] + "|" + pdf["rule"] + "|" + pdf["window_end"].astype(str)

        existing = DeltaTable(path).to_pandas(columns=["card_id", "window_end"]) if DeltaTable.is_deltatable(path) else None
        new_rows = []
        for card_id, grp in pdf.sort_values("window_end").groupby("card_id"):
            last = existing.loc[existing.card_id == card_id, "window_end"].max() if existing is not None else pd.NaT
            for r in grp.itertuples(index=False):
                if pd.isna(last) or abs(pd.Timestamp(r.window_end) - pd.Timestamp(last)) > COOLDOWN[r.rule]:
                    new_rows.append(r)
                    last = r.window_end
        if not new_rows:  # every candidate is a repeat of an alert already raised
            return
        new = pd.DataFrame(new_rows)
        _write(path, pa.Table.from_pandas(new, preserve_index=False), app_id, batch_id)
        if publish:
            publish(new)

    return sink


def kafka_publisher(bootstrap: str, topic: str) -> Callable[[pd.DataFrame], None]:
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: v.encode(), key_serializer=lambda k: k.encode())

    def publish(alerts: pd.DataFrame) -> None:
        for r in alerts.itertuples(index=False):
            producer.send(topic, key=r.card_id, value=pd.Series(r._asdict()).to_json(date_format="iso"))
        producer.flush()

    return publish


def table_path(root: str, name: str) -> str:
    return os.path.join(root, name)
