# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Streaming zone demand + surge detection
# MAGIC Reads the Silver Delta table as a stream, aggregates trips per pickup zone in
# MAGIC 15-minute tumbling windows (30-minute watermark for late data) and flags windows
# MAGIC whose demand is ≥ 1.5× the zone's normal level for that hour.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")
dbutils.widgets.dropdown("trigger", "availableNow", ["availableNow", "continuous_1min"])

import sys, os
sys.path.append(os.path.abspath("../src"))

from taxi_lakehouse import streaming
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))

# COMMAND ----------

# Baseline: typical 15-min demand per zone and hour, from history (batch).
baseline = streaming.hourly_baseline(spark.table(cfg.table("silver", "trips")))
baseline.write.format("delta").mode("overwrite").saveAsTable(cfg.table("gold", "zone_hour_baseline"))

# COMMAND ----------

windows = streaming.zone_demand_windows(spark.readStream.table(cfg.table("silver", "trips")))

writer = (
    windows.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{cfg.checkpoint_root}/zone_demand_15min")
)
writer = (writer.trigger(availableNow=True) if dbutils.widgets.get("trigger") == "availableNow"
          else writer.trigger(processingTime="1 minute"))
query = writer.toTable(cfg.table("gold", "zone_demand_15min"))
if dbutils.widgets.get("trigger") == "availableNow":
    query.awaitTermination()

# COMMAND ----------

surges = streaming.surge_candidates(
    spark.table(cfg.table("gold", "zone_demand_15min")),
    spark.table(cfg.table("gold", "zone_hour_baseline")),
)
surges.write.format("delta").mode("overwrite").saveAsTable(cfg.table("gold", "surge_windows"))
display(surges.filter("is_surge").orderBy("window_start", ascending=False).limit(50))
