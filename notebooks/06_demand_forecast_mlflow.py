# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Hourly demand forecast (MLlib GBT + MLflow)
# MAGIC Predicts next-hour pickups per zone. Features: calendar, lags (1h, 2h, 24h, 168h),
# MAGIC rolling means and a target-encoded zone level computed on the training window only.
# MAGIC Time-based split (no leakage). Compared against two naive baselines, logged to MLflow
# MAGIC and registered as `nyc_taxi_zone_demand`.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")
dbutils.widgets.text("cutoff", "2022-12-10 00:00:00")
dbutils.widgets.text("max_iter", "40")
dbutils.widgets.text("max_depth", "5")

import sys, os
sys.path.append(os.path.abspath("../src"))

import mlflow
from taxi_lakehouse import features, ml
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))
mlflow.set_experiment("/Shared/nyc_taxi_zone_demand")

# COMMAND ----------

zone_ids = spark.table(cfg.table("gold", "dim_zone")).filter("zone_key <= 263").select("zone_key")
feats = features.build_training_set(spark.table(cfg.table("silver", "trips")), zone_ids)
feats.write.format("delta").mode("overwrite").saveAsTable(cfg.table("gold", "demand_features"))
feats = spark.table(cfg.table("gold", "demand_features"))

# COMMAND ----------

model, metrics, predictions = ml.train_demand_model(
    feats,
    dbutils.widgets.get("cutoff"),
    max_iter=int(dbutils.widgets.get("max_iter")),
    max_depth=int(dbutils.widgets.get("max_depth")),
    log_to_mlflow=True,
)
metrics

# COMMAND ----------

ml.feature_importance(model)

# COMMAND ----------

(predictions.select("zone_id", "hour_ts", "trips", "prediction", "lag_168h")
    .write.format("delta").mode("overwrite").saveAsTable(cfg.table("gold", "demand_predictions")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT hour_ts, SUM(trips) AS actual, ROUND(SUM(prediction), 1) AS predicted
# MAGIC FROM nyc_taxi_gold.demand_predictions GROUP BY hour_ts ORDER BY hour_ts
