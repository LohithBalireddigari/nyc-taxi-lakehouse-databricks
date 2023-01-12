import os
import sys
import time

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Run Python and the JVM in UTC so naive datetimes in tests mean exactly what they say.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


RAW_TRIP_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID",
    "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
    "driver_id",
]


def raw_trip(pickup="2022-11-01 08:00:00", dropoff="2022-11-01 08:20:00", passengers="1", distance="3.2",
             pu="161", do="236", payment="1", fare="15.50", tip="3.10", total="22.40", driver="D0001", **kw):
    row = {
        "VendorID": "2", "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff,
        "passenger_count": passengers, "trip_distance": distance, "RatecodeID": "1", "store_and_fwd_flag": "N",
        "PULocationID": pu, "DOLocationID": do, "payment_type": payment, "fare_amount": fare, "extra": "0.0",
        "mta_tax": "0.5", "tip_amount": tip, "tolls_amount": "0.0", "improvement_surcharge": "0.3",
        "total_amount": total, "congestion_surcharge": "2.5", "airport_fee": "0.0", "driver_id": driver,
    }
    row.update(kw)
    return tuple(row[c] for c in RAW_TRIP_COLS)


@pytest.fixture
def make_bronze_trips(spark):
    from taxi_lakehouse.bronze import with_metadata
    from taxi_lakehouse.schemas import TRIPS_RAW

    def _make(rows):
        return with_metadata(spark.createDataFrame(rows, TRIPS_RAW))

    return _make


@pytest.fixture
def zones(spark):
    from taxi_lakehouse.schemas import ZONES

    return spark.createDataFrame(
        [
            (132, "Queens", "JFK Airport", "Airports"),
            (161, "Manhattan", "Midtown Center", "Yellow Zone"),
            (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
        ],
        ZONES,
    )
