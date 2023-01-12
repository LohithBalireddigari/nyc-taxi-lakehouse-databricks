"""Near-real-time zone demand with Structured Streaming.

The transformation is written once and works on a streaming DataFrame
(``readStream`` from the Silver Delta table) or a batch DataFrame (tests,
backfills). ``withWatermark`` is a no-op in batch mode.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def zone_demand_windows(trips: DataFrame, window: str = "15 minutes", watermark: str = "30 minutes") -> DataFrame:
    """Trips, revenue and average duration per pickup zone per tumbling window."""
    return (
        trips.withWatermark("pickup_ts", watermark)
        .groupBy(F.window("pickup_ts", window).alias("w"), "PULocationID")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "PULocationID",
            "trips",
            "revenue",
            "avg_duration_min",
        )
    )


def surge_candidates(windows: DataFrame, baseline: DataFrame, ratio: float = 1.5) -> DataFrame:
    """Flag zone-windows whose demand is ``ratio``x the zone's typical demand for that hour.

    ``baseline`` has columns PULocationID, hour, avg_trips (from history).
    """
    return (
        windows.withColumn("hour", F.hour("window_start"))
        .join(baseline, ["PULocationID", "hour"], "left")
        .withColumn("demand_ratio", F.round(F.col("trips") / F.col("avg_trips"), 2))
        .withColumn("is_surge", F.col("demand_ratio") >= ratio)
    )


def hourly_baseline(trips: DataFrame, window: str = "15 minutes") -> DataFrame:
    w = zone_demand_windows(trips, window=window)
    return (
        w.withColumn("hour", F.hour("window_start"))
        .groupBy("PULocationID", "hour")
        .agg(F.round(F.avg("trips"), 3).alias("avg_trips"))
    )
