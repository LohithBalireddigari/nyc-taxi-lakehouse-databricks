# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest (Auto Loader)
# MAGIC Each monthly trip file is picked up exactly once with Auto Loader and appended to
# MAGIC `nyc_taxi_bronze.trips`. Zones and driver snapshots are small reference files loaded in batch.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")

import sys, os
sys.path.append(os.path.abspath("../src"))

from taxi_lakehouse import bronze
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))

# COMMAND ----------

query = (
    bronze.read_trips_stream(spark, f"{cfg.raw_path}/trips", f"{cfg.checkpoint_root}/bronze_trips/schema")
    .writeStream.format("delta")
    .option("checkpointLocation", f"{cfg.checkpoint_root}/bronze_trips")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(cfg.table("bronze", "trips"))
)
query.awaitTermination()
query.lastProgress

# COMMAND ----------

(bronze.read_zones(spark, f"{cfg.raw_path}/zones")
    .write.format("delta").mode("overwrite").saveAsTable(cfg.table("bronze", "zones")))

(bronze.read_drivers(spark, f"{cfg.raw_path}/drivers")
    .write.format("delta").mode("overwrite").saveAsTable(cfg.table("bronze", "driver_snapshots")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _source_file, COUNT(*) AS rows, MIN(_ingested_at) AS ingested_at
# MAGIC FROM nyc_taxi_bronze.trips GROUP BY _source_file ORDER BY _source_file
