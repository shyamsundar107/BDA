"""
STEP 3: Historical trend value (blocking — Member 1 request #2).

"Is this lane's traffic getting worse over time" as one number per
lane, 0-1. Method: compare average congestion over the most recent
window (e.g. last 7 days) to a longer baseline window (e.g. last 30
days). A lane trending worse gets a value near 1; flat or improving
gets a value near 0.

This is a batch job — run it daily (cron / Oozie / Airflow) and it
refreshes the historical_trend Hive table. The optimizer reads that
table's latest row per lane at decision time.

Run:
    spark-submit 03_historical_trend.py --intersection_id J1 --as_of_date 2026-09-23
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RECENT_WINDOW_DAYS = 7
BASELINE_WINDOW_DAYS = 30


def compute_trend(spark, intersection_id, as_of_date):
    events = spark.table("raw_traffic_events").filter(
        F.col("intersection_id") == intersection_id
    )

    # per-day, per-lane average waiting time as our congestion proxy
    daily = (
        events.withColumn("event_date", F.to_date("event_ts"))
        .groupBy("lane", "event_date")
        .agg(F.avg("waiting_time").alias("avg_wait"))
    )

    recent = (
        daily.filter(F.col("event_date") > F.date_sub(F.lit(as_of_date), RECENT_WINDOW_DAYS))
        .groupBy("lane")
        .agg(F.avg("avg_wait").alias("recent_avg_wait"))
    )

    baseline = (
        daily.filter(F.col("event_date") > F.date_sub(F.lit(as_of_date), BASELINE_WINDOW_DAYS))
        .groupBy("lane")
        .agg(F.avg("avg_wait").alias("baseline_avg_wait"))
    )

    joined = recent.join(baseline, on="lane", how="inner")

    # ratio-based normalization: 1.0x baseline -> 0.5, 2x baseline -> ~1.0,
    # 0.5x baseline -> ~0.0. Clamped to [0, 1].
    result = joined.withColumn(
        "raw_ratio", F.col("recent_avg_wait") / F.col("baseline_avg_wait")
    ).withColumn(
        "historical_trend",
        F.greatest(F.lit(0.0), F.least(F.lit(1.0), (F.col("raw_ratio") - 0.5)))
    ).withColumn("intersection_id", F.lit(intersection_id)) \
     .withColumn("trend_date", F.lit(as_of_date)) \
     .select("intersection_id", "lane", "trend_date",
             F.round("historical_trend", 3).alias("historical_trend"))

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--intersection_id", required=True)
    parser.add_argument("--as_of_date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("historical_trend").enableHiveSupport().getOrCreate()

    trend_df = compute_trend(spark, args.intersection_id, args.as_of_date)
    trend_df.show(truncate=False)

    # Write back to the Hive table so the optimizer can read it
    trend_df.write.mode("append").insertInto("historical_trend")

    # This is the table/shape to paste to Member 1:
    #
    # lane, historical_trend
    # North, 0.55
    # South, 0.20
