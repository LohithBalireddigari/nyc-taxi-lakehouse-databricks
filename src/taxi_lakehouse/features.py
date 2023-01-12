"""Feature engineering for hourly pickup-demand forecasting per zone."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import HOLIDAYS

LAGS = {"lag_1h": 1, "lag_2h": 2, "lag_24h": 24, "lag_168h": 168}
FEATURE_COLS = [
    "hour", "day_of_week", "is_weekend", "is_holiday",
    "lag_1h", "lag_2h", "lag_24h", "lag_168h", "rolling_3h_mean", "rolling_24h_mean",
]


def hourly_demand_grid(trips: DataFrame, zone_ids: DataFrame) -> DataFrame:
    """Trips per (zone, hour), including explicit zero-demand hours.

    Run with ``spark.sql.session.timeZone = UTC`` so every hour exists exactly
    once (no DST gaps/overlaps).
    """
    counts = (
        trips.withColumn("hour_ts", F.date_trunc("hour", "pickup_ts"))
        .groupBy(F.col("PULocationID").alias("zone_id"), "hour_ts")
        .agg(F.count("*").alias("trips"))
    )
    bounds = counts.agg(F.min("hour_ts").alias("lo"), F.max("hour_ts").alias("hi")).collect()[0]
    spark = trips.sparkSession
    lo, hi = int(bounds.lo.timestamp()), int(bounds.hi.timestamp())
    hours = spark.range(lo, hi + 1, 3600).select(F.timestamp_seconds("id").alias("hour_ts"))
    grid = zone_ids.select(F.col(zone_ids.columns[0]).alias("zone_id")).distinct().crossJoin(hours)
    return grid.join(counts, ["zone_id", "hour_ts"], "left").fillna(0, ["trips"])


def add_features(grid: DataFrame) -> DataFrame:
    w = Window.partitionBy("zone_id").orderBy("hour_ts")
    df = grid
    for name, k in LAGS.items():
        df = df.withColumn(name, F.lag("trips", k).over(w))
    return (
        df.withColumn("rolling_3h_mean", F.avg("trips").over(w.rowsBetween(-3, -1)))
        .withColumn("rolling_24h_mean", F.avg("trips").over(w.rowsBetween(-24, -1)))
        .withColumn("hour", F.hour("hour_ts"))
        .withColumn("day_of_week", F.dayofweek("hour_ts"))
        .withColumn("is_weekend", F.dayofweek("hour_ts").isin(1, 7).cast("int"))
        .withColumn("is_holiday", F.to_date("hour_ts").cast("string").isin(*HOLIDAYS).cast("int"))
        .dropna(subset=["lag_168h"])
    )


def build_training_set(trips: DataFrame, zone_ids: DataFrame) -> DataFrame:
    return add_features(hourly_demand_grid(trips, zone_ids))
