"""Silver: typed, de-duplicated, validated and zone-enriched trips."""

from itertools import chain
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import quality
from .config import AIRPORT_ZONES, PAYMENT_TYPES

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
MONEY_COLS = ("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
              "total_amount", "congestion_surcharge", "airport_fee")
BUSINESS_COLS = ("vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance", "ratecode_id",
                 "store_and_fwd", "PULocationID", "DOLocationID", "payment_type") + MONEY_COLS + ("driver_id",)


def _payment_map():
    return F.create_map(*[F.lit(x) for x in chain(*PAYMENT_TYPES.items())])


def type_trips(bronze: DataFrame) -> DataFrame:
    """Cast raw strings to proper types and derive trip metrics."""
    df = bronze.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime", TS_FORMAT).alias("pickup_ts"),
        F.to_timestamp("tpep_dropoff_datetime", TS_FORMAT).alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("ratecode_id"),
        (F.col("store_and_fwd_flag") == "Y").alias("store_and_fwd"),
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        F.col("payment_type").cast("int").alias("payment_type"),
        *[F.col(c).cast("decimal(10,2)").alias(c) for c in MONEY_COLS],
        F.trim("driver_id").alias("driver_id"),
        "_ingested_at",
        "_source_file",
    )
    duration = (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    return (
        # Exact re-sent records hash to the same id; any differing field gives a new id.
        df.withColumn("trip_id", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit(""))
                                                          for c in BUSINESS_COLS]), 256))
        .withColumn("duration_min", F.round(duration, 2))
        .withColumn("avg_speed_mph",
                    F.when(duration > 0, F.round(F.col("trip_distance") / (duration / 60.0), 2)))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("day_of_week", F.date_format("pickup_ts", "E"))
        .withColumn("is_weekend", F.dayofweek("pickup_ts").isin(1, 7))
        .withColumn("payment_desc", F.coalesce(_payment_map()[F.col("payment_type")], F.lit("Unknown")))
        .withColumn("tip_pct",
                    F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                           F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)))
        .withColumn("is_airport_trip",
                    F.col("PULocationID").isin(*AIRPORT_ZONES) | F.col("DOLocationID").isin(*AIRPORT_ZONES))
    )


def clean_trips(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Type, drop exact re-sent duplicates, then validate. Returns ``(valid, quarantine)``."""
    typed = type_trips(bronze).dropDuplicates(["trip_id"])
    return quality.split_by_rules(typed, quality.TRIP_RULES)


def enrich_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    do = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )
    return trips.join(F.broadcast(pu), "PULocationID", "left").join(F.broadcast(do), "DOLocationID", "left")


def clean_drivers(bronze: DataFrame) -> Tuple[DataFrame, DataFrame]:
    typed = bronze.select(
        F.when(F.trim("driver_id") != "", F.trim("driver_id")).alias("driver_id"),
        F.initcap(F.trim("driver_name")).alias("driver_name"),
        F.trim("vehicle_type").alias("vehicle_type"),
        F.trim("base_name").alias("base_name"),
        F.upper(F.trim("status")).alias("status"),
        F.col("rating"),
        F.to_date("snapshot_date").alias("snapshot_date"),
    )
    return quality.split_by_rules(typed, quality.DRIVER_RULES)
