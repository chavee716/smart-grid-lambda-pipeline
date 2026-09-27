"""
Speed layer: Apache Spark Structured Streaming job.

Reads raw meter-reading events from Kafka, validates/cleans them, computes
1-minute tumbling-window aggregates per grid zone (total consumption, total
solar generation, renewable share, active meter count), and:

  1. Upserts the latest windows into Postgres `realtime_grid_metrics`
     (foreach-batch sink) - this is what the /realtime API endpoint serves.
  2. ALSO archives every raw (validated) event as Parquet, partitioned by
     simulated date, into the data lake (`RAW_READINGS_PATH`). This Parquet
     archive is the immutable master dataset that the BATCH layer re-reads
     from scratch every night - never the speed layer's approximate table.
     This dual-write is the crux of the Lambda pattern: the speed layer
     serves *fast* answers, the batch layer later recomputes the *correct*
     answer over the same raw log.
  3. Tracks malformed-event counts into `ingestion_quality_stats` for the
     observability "error rate" alert rule.

Run with:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      stream_processing/spark_streaming_job.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

from common import config

RAW_SCHEMA = StructType([
    StructField("meter_id", StringType()),
    StructField("household_id", StringType()),
    StructField("power_consumption_kwh", DoubleType()),
    StructField("solar_generation_kwh", DoubleType()),
    StructField("grid_zone", StringType()),
    StructField("timestamp", StringType()),
])

WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "2 minutes"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SmartGridSpeedLayer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.TOPIC_METER_READINGS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_validate(raw_df):
    """
    Parses the Kafka value as JSON against RAW_SCHEMA (permissive mode - bad
    JSON or type mismatches yield NULLs rather than crashing the query), then
    splits into (valid_df, malformed_df) so both can be handled explicitly.
    This is the pipeline's data-quality / dead-letter boundary.
    """
    parsed = (
        raw_df
        .select(F.col("value").cast("string").alias("raw_value"))
        .withColumn("event", F.from_json(F.col("raw_value"), RAW_SCHEMA))
        .select("raw_value", "event.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
    )

    is_valid = (
        F.col("household_id").isNotNull()
        & F.col("grid_zone").isNotNull()
        & F.col("power_consumption_kwh").isNotNull()
        & (F.col("power_consumption_kwh") >= 0)
        & F.col("event_ts").isNotNull()
    )

    classified = parsed.withColumn("is_malformed", ~is_valid)
    valid_df = classified.filter(~F.col("is_malformed"))
    malformed_df = classified.filter(F.col("is_malformed"))
    quality_df = classified.select("is_malformed")
    return valid_df, malformed_df, quality_df


def compute_windowed_aggregates(valid_df):
    return (
        valid_df
        .withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", WINDOW_DURATION).alias("w"),
            F.col("grid_zone").alias("zone"),
        )
        .agg(
            F.sum("power_consumption_kwh").alias("total_consumption_kwh"),
            F.sum("solar_generation_kwh").alias("total_solar_kwh"),
            F.approx_count_distinct("household_id").alias("active_meters"),
            F.avg("power_consumption_kwh").alias("avg_load_kwh"),
        )
        .withColumn(
            "renewable_share",
            F.when(F.col("total_consumption_kwh") > 0,
                   F.col("total_solar_kwh") / F.col("total_consumption_kwh"))
             .otherwise(F.lit(0.0)),
        )
        .select(
            F.col("zone"),
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "total_consumption_kwh", "total_solar_kwh",
            "renewable_share", "active_meters", "avg_load_kwh",
        )
    )


def upsert_to_postgres(batch_df, batch_id: int):
    """foreachBatch sink: UPSERT windowed metrics into realtime_grid_metrics."""
    if batch_df.rdd.isEmpty():
        return
    rows = batch_df.collect()
    import psycopg2
    from observability.logging_config import get_logger
    log = get_logger("stream_processing.spark_streaming_job")

    conn = psycopg2.connect(config.POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO realtime_grid_metrics
                        (zone, window_start, window_end, total_consumption_kwh,
                         total_solar_kwh, renewable_share, active_meters, avg_load_kwh)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (zone, window_start) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                        total_solar_kwh = EXCLUDED.total_solar_kwh,
                        renewable_share = EXCLUDED.renewable_share,
                        active_meters = EXCLUDED.active_meters,
                        avg_load_kwh = EXCLUDED.avg_load_kwh;
                """, (r["zone"], r["window_start"], r["window_end"],
                      float(r["total_consumption_kwh"]), float(r["total_solar_kwh"]),
                      float(r["renewable_share"]), int(r["active_meters"]), float(r["avg_load_kwh"])))
        conn.commit()
        log.info("windowed_metrics_upserted", extra={
            "stage": "speed_layer", "batch_id": batch_id, "rows": len(rows),
        })
    finally:
        conn.close()


def record_quality_stats(batch_df, batch_id: int):
    total_count = batch_df.count()
    malformed_count = batch_df.filter(F.col("is_malformed")).count()
    if total_count == 0:
        return
    import psycopg2
    conn = psycopg2.connect(config.POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingestion_quality_stats (total_count, malformed_count) VALUES (%s, %s);",
                (total_count, malformed_count),
            )
        conn.commit()
    finally:
        conn.close()


def main():
    spark = build_spark()
    raw_stream = read_kafka_stream(spark)
    valid_df, malformed_df, quality_df = parse_and_validate(raw_stream)
    windowed = compute_windowed_aggregates(valid_df)

    # Sink 1: real-time serving table (speed layer output)
    metrics_query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(upsert_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoints/realtime_metrics")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Sink 2: immutable raw archive (batch layer's master dataset)
    archive_query = (
        valid_df.withColumn("event_date", F.to_date("event_ts"))
        .writeStream
        .format("parquet")
        .option("path", config.RAW_READINGS_PATH)
        .option("checkpointLocation", "/tmp/checkpoints/raw_archive")
        .partitionBy("event_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Sink 3: data-quality stats for the error-rate alert rule
    quality_query = (
        quality_df.writeStream
        .foreachBatch(record_quality_stats)
        .option("checkpointLocation", "/tmp/checkpoints/quality_stats")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
