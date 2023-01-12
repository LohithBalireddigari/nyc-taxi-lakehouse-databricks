# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver trips
# MAGIC Types and de-duplicates trips, applies the quality rules in `quality.TRIP_RULES`,
# MAGIC enriches with pickup/dropoff zones and upserts into `nyc_taxi_silver.trips` with `MERGE`
# MAGIC (idempotent: re-running never creates duplicates). Failing rows go to `trips_quarantine`.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")

import sys, os
sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable
from taxi_lakehouse import quality, silver
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))
target = cfg.table("silver", "trips")

# COMMAND ----------

valid, quarantine = silver.clean_trips(spark.table(cfg.table("bronze", "trips")))
valid = silver.enrich_with_zones(valid, spark.table(cfg.table("bronze", "zones")))

# COMMAND ----------

if not spark.catalog.tableExists(target):
    (valid.write.format("delta").partitionBy("pickup_date").saveAsTable(target))
else:
    (DeltaTable.forName(spark, target).alias("t")
        .merge(valid.alias("s"), "t.trip_id = s.trip_id AND t.pickup_date = s.pickup_date")
        .whenNotMatchedInsertAll()
        .execute())

(quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(cfg.table("silver", "trips_quarantine")))

# COMMAND ----------

spark.sql(f"OPTIMIZE {target} ZORDER BY (PULocationID, pickup_ts)")

# COMMAND ----------

display(quality.failure_counts(spark.table(cfg.table("silver", "trips_quarantine"))))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyc_taxi_silver.trips
