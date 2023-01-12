"""Schemas for raw landing data (modelled on the NYC TLC yellow-taxi trip records)."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Raw trips are read as strings so bad values survive into Bronze and get
# handled explicitly in Silver.
TRIPS_RAW = StructType(
    [
        StructField(name, StringType(), True)
        for name in (
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "RatecodeID",
            "store_and_fwd_flag",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "driver_id",
        )
    ]
)

ZONES = StructType(
    [
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ]
)

DRIVERS_RAW = StructType(
    [
        StructField("driver_id", StringType(), True),
        StructField("driver_name", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("base_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rating", DoubleType(), True),
        StructField("snapshot_date", StringType(), True),
    ]
)
