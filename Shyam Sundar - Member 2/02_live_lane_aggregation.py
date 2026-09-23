"""
STEP 2: Live per-cycle lane aggregation.

This is the function whose output goes straight to Member 1 — it answers
his request #1. Run it once per signal cycle (e.g. every 30-60s in the
demo) for a given intersection; it reads raw_traffic_events for just
that cycle window and produces one JSON object per lane.

Run:
    spark-submit 02_live_lane_aggregation.py --intersection_id J1 \
        --cycle_start "2026-09-23 10:00:00" --cycle_end "2026-09-23 10:00:30"
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MAX_REASONABLE_VEHICLES = 60   # for density normalization, tune to your data
MAX_REASONABLE_WAIT = 90       # seconds, for congestion normalization


def aggregate_cycle(spark, intersection_id, cycle_start, cycle_end):
    df = (
        spark.table("raw_traffic_events")
        .filter(F.col("intersection_id") == intersection_id)
        .filter(F.col("event_ts") >= cycle_start)
        .filter(F.col("event_ts") < cycle_end)
    )

    agg = (
        df.groupBy("lane")
        .agg(
            F.count("vehicle_id").alias("vehicle_count"),
            F.avg("waiting_time").alias("avg_waiting"),
            F.max(F.col("emergency_flag").cast("int")).alias("emergency_flag_int"),
        )
    )

    rows = agg.collect()
    output = []
    for r in rows:
        vehicle_count = r["vehicle_count"] or 0
        avg_waiting = round(float(r["avg_waiting"] or 0.0), 1)

        # congestion: blended 0-1 score from density + waiting, same
        # spirit as the "Congestion" column already on Member 1's slide 7
        density_norm = min(vehicle_count / MAX_REASONABLE_VEHICLES, 1.0)
        wait_norm = min(avg_waiting / MAX_REASONABLE_WAIT, 1.0)
        congestion = round(0.6 * density_norm + 0.4 * wait_norm, 3)

        output.append({
            "intersection_id": intersection_id,
            "lane": r["lane"],
            "vehicle_count": int(vehicle_count),
            "avg_waiting": avg_waiting,
            "congestion": congestion,
            "emergency_flag": bool(r["emergency_flag_int"] or 0),
            "cycle_start": str(cycle_start),
            "cycle_end": str(cycle_end),
        })
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--intersection_id", required=True)
    parser.add_argument("--cycle_start", required=True)
    parser.add_argument("--cycle_end", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("live_lane_aggregation").enableHiveSupport().getOrCreate()

    result = aggregate_cycle(spark, args.intersection_id, args.cycle_start, args.cycle_end)
    print(json.dumps(result, indent=2))

    # Sample of what this prints, per lane — THIS is the block to paste
    # to Member 1:
    #
    # {
    #   "intersection_id": "J1",
    #   "lane": "North",
    #   "vehicle_count": 42,
    #   "avg_waiting": 51.0,
    #   "congestion": 0.85,
    #   "emergency_flag": false,
    #   "cycle_start": "2026-09-23 10:00:00",
    #   "cycle_end": "2026-09-23 10:00:30"
    # }
