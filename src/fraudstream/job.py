"""The streaming job: transactions in, fraud alerts and metrics out.

    source (Kafka topic or JSON files)
      -> parse + schema check
      -> de-duplicate within the watermark
      -> 3 detectors (2 windowed aggregations, 1 stream-stream join) -> alerts_<rule> Delta tables (+ alerts topic)
      -> per-minute metrics                       -> metrics_minute Delta table
      -> raw transactions                         -> bronze_transactions Delta table

Every query has its own checkpoint, so the job can be stopped and restarted at any
point and resume where it left off without losing or double-counting data.

    python -m fraudstream.job --source files --input data/landing            # local
    python -m fraudstream.job --source kafka --bootstrap localhost:9092      # with Kafka
    python -m fraudstream.job ... --available-now                            # process what's there, then stop
"""

from __future__ import annotations

import argparse
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import detectors as D
from . import sinks

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:{version}"


def spark_session(kafka: bool) -> SparkSession:
    b = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[4]"))
        .appName("fraud-stream")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    )
    if kafka:
        import pyspark

        b = b.config("spark.jars.packages", KAFKA_PACKAGE.format(version=pyspark.__version__))
    s = b.getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def read_source(spark: SparkSession, a: argparse.Namespace) -> DataFrame:
    if a.source == "kafka":
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", a.bootstrap)
            .option("subscribe", a.topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", a.max_per_trigger)
            .load()
            .select(F.col("value").cast("string").alias("value"))
        )
    return (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", a.max_files_per_trigger)
        .load(a.input)
    )


def start(a: argparse.Namespace):
    spark = spark_session(a.source == "kafka")
    txns = D.deduplicate(D.parse(read_source(spark, a)))
    publish = sinks.kafka_publisher(a.bootstrap, a.alerts_topic) if a.source == "kafka" else None

    def run(df: DataFrame, name: str, sink, mode: str):
        w = (df.writeStream.queryName(name).outputMode(mode)
             .option("checkpointLocation", os.path.join(a.checkpoints, name))
             .foreachBatch(sink))
        w = w.trigger(availableNow=True) if a.available_now else w.trigger(processingTime=a.trigger)
        return w.start()

    queries = []
    for rule, fn in D.DETECTORS.items():
        mode = "append" if rule == "impossible_travel" else "update"
        queries.append(run(fn(txns), f"alerts_{rule}",
                           sinks.alert_sink(sinks.table_path(a.lake, f"alerts_{rule}"), f"alerts_{rule}", publish), mode))
    queries.append(run(D.minute_metrics(txns), "metrics_minute",
                       sinks.delta_append(sinks.table_path(a.lake, "metrics_minute"), "metrics_minute"), "append"))
    queries.append(run(txns.withColumn("ingested_at", F.current_timestamp()), "bronze_transactions",
                       sinks.delta_append(sinks.table_path(a.lake, "bronze_transactions"), "bronze_transactions"), "append"))
    return spark, queries


def main() -> None:
    ap = argparse.ArgumentParser(description="Real-time fraud detection stream")
    ap.add_argument("--source", choices=["files", "kafka"], default="files")
    ap.add_argument("--input", default="data/landing")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="card-transactions")
    ap.add_argument("--alerts-topic", default="fraud-alerts")
    ap.add_argument("--lake", default="data/lake")
    ap.add_argument("--checkpoints", default="data/checkpoints")
    ap.add_argument("--trigger", default="2 seconds")
    ap.add_argument("--max-files-per-trigger", type=int, default=1)
    ap.add_argument("--max-per-trigger", type=int, default=20000)
    ap.add_argument("--available-now", action="store_true", help="process everything available, then stop")
    ap.add_argument("--run-seconds", type=int, default=0, help="stop after N seconds (0 = run until stopped)")
    a = ap.parse_args()

    spark, queries = start(a)
    t0 = time.time()
    try:
        if a.available_now:
            for q in queries:
                q.awaitTermination()
        else:
            while all(q.isActive for q in queries):
                if a.run_seconds and time.time() - t0 > a.run_seconds:
                    break
                time.sleep(1)
            for q in queries:
                if q.exception():
                    raise q.exception()
    finally:
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
