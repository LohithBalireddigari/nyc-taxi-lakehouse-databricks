# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Data quality gate
# MAGIC Appends a run record to `gold.data_quality` and fails the workflow when a dataset's
# MAGIC pass rate drops below the threshold.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")
dbutils.widgets.text("min_pass_rate", "0.95")

import sys, os
sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F
from taxi_lakehouse import quality, silver
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))
threshold = float(dbutils.widgets.get("min_pass_rate"))

# COMMAND ----------

drivers_valid, _ = silver.clean_drivers(spark.table(cfg.table("bronze", "driver_snapshots")))
reports = [
    quality.report("trips", spark.table(cfg.table("silver", "trips")),
                   spark.table(cfg.table("silver", "trips_quarantine"))).as_dict(),
    quality.report("drivers", drivers_valid, spark.table(cfg.table("silver", "drivers_quarantine"))).as_dict(),
]
run = spark.createDataFrame(reports).withColumn("checked_at", F.current_timestamp())
run.write.format("delta").mode("append").saveAsTable(cfg.table("gold", "data_quality"))
display(run)

# COMMAND ----------

display(quality.failure_counts(spark.table(cfg.table("silver", "trips_quarantine"))))

# COMMAND ----------

failing = [r for r in reports if r["pass_rate"] < threshold]
assert not failing, f"Data quality below {threshold:.0%}: {failing}"
print("Data quality gate passed.")
