# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables version of the trips pipeline
# MAGIC Same logic as notebooks 01-04, declared as a DLT pipeline. Expectations come from
# MAGIC `quality.TRIP_RULES`, so the batch quarantine and the DLT expectations never drift apart.
# MAGIC
# MAGIC Pipeline settings: see `conf/dlt_pipeline.json` (target database `nyc_taxi_dlt`).

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

import dlt
from pyspark.sql import functions as F

from taxi_lakehouse import gold, silver
from taxi_lakehouse.quality import TRIP_RULES
from taxi_lakehouse.schemas import TRIPS_RAW, ZONES

RAW = spark.conf.get("taxi.raw_path", "dbfs:/FileStore/nyc_taxi/raw")

# COMMAND ----------

@dlt.table(comment="Raw yellow-taxi trips loaded incrementally with Auto Loader",
           table_properties={"quality": "bronze"})
def bronze_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(TRIPS_RAW)
        .load(f"{RAW}/trips")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


@dlt.table(comment="Taxi zone lookup")
def zones():
    return spark.read.option("header", "true").schema(ZONES).csv(f"{RAW}/zones")

# COMMAND ----------

@dlt.table(comment="Typed, de-duplicated trips that pass every expectation",
           partition_cols=["pickup_date"], table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(TRIP_RULES)
def silver_trips():
    typed = silver.type_trips(dlt.read_stream("bronze_trips")).dropDuplicates(["trip_id"])
    return silver.enrich_with_zones(typed, dlt.read("zones"))


@dlt.table(comment="Rows that violated at least one expectation")
def silver_trips_quarantine():
    typed = silver.type_trips(dlt.read_stream("bronze_trips"))
    all_ok = " AND ".join(f"({r})" for r in TRIP_RULES.values())
    return typed.filter(f"NOT coalesce({all_ok}, false)")

# COMMAND ----------

@dlt.table(comment="Hourly demand and revenue per pickup zone", table_properties={"quality": "gold"})
def gold_hourly_zone_demand():
    fact = dlt.read("silver_trips").withColumnRenamed("PULocationID", "pickup_zone_key")
    return gold.hourly_zone_demand(fact, dlt.read("zones"))


@dlt.table(comment="Daily revenue per borough", table_properties={"quality": "gold"})
def gold_daily_borough_revenue():
    fact = dlt.read("silver_trips").withColumnRenamed("PULocationID", "pickup_zone_key")
    return gold.daily_borough_revenue(fact, dlt.read("zones"))
