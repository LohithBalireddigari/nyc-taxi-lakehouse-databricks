"""Gold: star schema (fact_trips + dimensions) and analytics marts."""

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import HOLIDAYS
from .scd2 import point_in_time_join


def dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    days = (end - start).days + 1
    return (
        spark.range(days)
        .select(F.date_add(F.lit(start), F.col("id").cast("int")).alias("date"))
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.dayofmonth("date").alias("day"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
            F.col("date").cast("string").isin(*HOLIDAYS).alias("is_holiday"),
        )
    )


def dim_zone(zones: DataFrame) -> DataFrame:
    return zones.select(
        F.col("LocationID").alias("zone_key"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
        F.col("service_zone").isin("Airports", "EWR").alias("is_airport"),
    )


def fact_trips(trips: DataFrame, dim_driver: DataFrame) -> DataFrame:
    """One row per valid trip with surrogate/foreign keys and measures."""
    with_driver = point_in_time_join(trips, dim_driver, "driver_id", "pickup_ts", ["base_name"])
    return with_driver.select(
        "trip_id",
        F.date_format("pickup_ts", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        "pickup_ts",
        "dropoff_ts",
        "pickup_hour",
        F.col("PULocationID").alias("pickup_zone_key"),
        F.col("DOLocationID").alias("dropoff_zone_key"),
        "driver_sk",
        "driver_id",
        F.col("base_name").alias("driver_base"),
        "vendor_id",
        "ratecode_id",
        "payment_type",
        "payment_desc",
        "passenger_count",
        "trip_distance",
        "duration_min",
        "avg_speed_mph",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "tip_pct",
        "is_airport_trip",
        "pickup_date",
    )


def hourly_zone_demand(fact: DataFrame, zones: DataFrame) -> DataFrame:
    z = dim_zone(zones)
    return (
        fact.groupBy("pickup_date", "pickup_hour", "pickup_zone_key")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
        )
        .join(z, F.col("pickup_zone_key") == z.zone_key)
        .drop("zone_key")
    )


def daily_borough_revenue(fact: DataFrame, zones: DataFrame) -> DataFrame:
    z = dim_zone(zones).select("zone_key", "borough")
    return (
        fact.join(z, fact.pickup_zone_key == z.zone_key)
        .groupBy("pickup_date", "borough")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.sum("tip_amount"), 2).alias("tips"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .orderBy("pickup_date", "borough")
    )


def top_routes(fact: DataFrame, zones: DataFrame, n: int = 25) -> DataFrame:
    z = dim_zone(zones)
    pu = z.select(F.col("zone_key").alias("pickup_zone_key"), F.col("zone").alias("pickup_zone"))
    do = z.select(F.col("zone_key").alias("dropoff_zone_key"), F.col("zone").alias("dropoff_zone"))
    routes = (
        fact.groupBy("pickup_zone_key", "dropoff_zone_key")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
            F.round(F.expr("percentile_approx(duration_min, 0.9)"), 1).alias("p90_duration_min"),
        )
        .withColumn("route_rank", F.row_number().over(Window.orderBy(F.desc("trips"), "pickup_zone_key",
                                                                     "dropoff_zone_key")))
        .filter(F.col("route_rank") <= n)
    )
    return routes.join(pu, "pickup_zone_key").join(do, "dropoff_zone_key").orderBy("route_rank")


def tip_behaviour(fact: DataFrame) -> DataFrame:
    """Average tip % by hour for card payments (cash tips aren't recorded)."""
    return (
        fact.filter(F.col("payment_type") == 1)
        .groupBy("pickup_hour")
        .agg(
            F.count("*").alias("card_trips"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg(F.when(F.col("tip_amount") == 0, 1).otherwise(0)) * 100, 2).alias("no_tip_pct"),
        )
        .orderBy("pickup_hour")
    )


def driver_performance(fact: DataFrame) -> DataFrame:
    w = Window.partitionBy("driver_base").orderBy(F.desc("revenue"))
    return (
        fact.filter(F.col("driver_id").isNotNull())
        .groupBy("driver_id", "driver_base")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("total_amount"), 2).alias("revenue"),
            F.round(F.sum("duration_min") / 60, 1).alias("hours_driven"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
        .withColumn("revenue_per_hour", F.round(F.col("revenue") / F.col("hours_driven"), 2))
        .withColumn("rank_in_base", F.dense_rank().over(w))
    )
