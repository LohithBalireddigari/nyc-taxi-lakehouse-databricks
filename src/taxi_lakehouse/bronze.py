"""Bronze: land raw files unchanged, plus lineage metadata."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import DRIVERS_RAW, TRIPS_RAW, ZONES


def with_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.input_file_name())


def read_trips(spark: SparkSession, path: str) -> DataFrame:
    return with_metadata(spark.read.option("header", "true").schema(TRIPS_RAW).csv(path))


def read_trips_stream(spark: SparkSession, path: str, schema_location: str) -> DataFrame:
    """Auto Loader (Databricks only): picks up each new monthly file exactly once."""
    return with_metadata(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")
        .schema(TRIPS_RAW)
        .load(path)
    )


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema(ZONES).csv(path)


def read_drivers(spark: SparkSession, path: str) -> DataFrame:
    return with_metadata(spark.read.option("header", "true").schema(DRIVERS_RAW).csv(path))
