# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Driver dimension (SCD Type 2)
# MAGIC Applies each driver snapshot in date order with a single Delta `MERGE` per snapshot:
# MAGIC changed rows are closed (`effective_to`, `is_current = false`) and a new version is
# MAGIC inserted, using the *staged updates / null merge key* pattern.
# MAGIC Tracked columns: `vehicle_type`, `base_name`, `status`. Rating changes are type 1.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")

import sys, os
sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from taxi_lakehouse import scd2, silver
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))
TRACKED = ("vehicle_type", "base_name", "status")
target = cfg.table("gold", "dim_driver")

valid, quarantine = silver.clean_drivers(spark.table(cfg.table("bronze", "driver_snapshots")))
quarantine.write.format("delta").mode("overwrite").saveAsTable(cfg.table("silver", "drivers_quarantine"))

# COMMAND ----------

snapshots = [r[0] for r in valid.select("snapshot_date").distinct().orderBy("snapshot_date").collect()]

for snap in snapshots:
    incoming = scd2.prepare_incoming(valid.filter(F.col("snapshot_date") == snap), "driver_id", TRACKED,
                                     "snapshot_date")
    if not spark.catalog.tableExists(target):
        incoming.write.format("delta").saveAsTable(target)
        continue

    dim = DeltaTable.forName(spark, target)
    current = dim.toDF().filter("is_current")
    staged = scd2.staged_updates_for_merge(current, incoming, "driver_id")

    (dim.alias("t")
        .merge(staged.alias("s"), "t.driver_id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={"is_current": "false", "effective_to": "s.effective_from"})
        .whenMatchedUpdate(                       # unchanged tracked cols -> refresh type-1 attributes
            set={"rating": "s.rating", "driver_name": "s.driver_name"})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in incoming.columns})
        .execute())
    print(f"applied snapshot {snap}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_current, COUNT(*) AS versions FROM nyc_taxi_gold.dim_driver GROUP BY is_current

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drivers with history
# MAGIC SELECT driver_id, vehicle_type, base_name, status, effective_from, effective_to, is_current
# MAGIC FROM nyc_taxi_gold.dim_driver
# MAGIC WHERE driver_id IN (SELECT driver_id FROM nyc_taxi_gold.dim_driver GROUP BY driver_id HAVING COUNT(*) > 1)
# MAGIC ORDER BY driver_id, effective_from
