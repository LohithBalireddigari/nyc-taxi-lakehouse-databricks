# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold star schema + marts
# MAGIC `fact_trips` joins each trip to the driver version valid at pickup time (point-in-time
# MAGIC join on the SCD2 dimension). Marts feed the Databricks SQL dashboard.

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")

import sys, os
sys.path.append(os.path.abspath("../src"))

from datetime import date
from taxi_lakehouse import gold
from taxi_lakehouse.config import Config

cfg = Config(root=dbutils.widgets.get("root"))
trips = spark.table(cfg.table("silver", "trips"))
zones = spark.table(cfg.table("bronze", "zones"))
dim_driver = spark.table(cfg.table("gold", "dim_driver"))


def save(df, name, partition=None):
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition:
        w = w.partitionBy(partition)
    w.saveAsTable(cfg.table("gold", name))
    print(f"gold.{name}: {spark.table(cfg.table('gold', name)).count():,}")

# COMMAND ----------

save(gold.dim_zone(zones), "dim_zone")
save(gold.dim_date(spark, date(2022, 10, 1), date(2022, 12, 31)), "dim_date")
save(gold.fact_trips(trips, dim_driver), "fact_trips", partition="pickup_date")

fact = spark.table(cfg.table("gold", "fact_trips"))

# COMMAND ----------

save(gold.hourly_zone_demand(fact, zones), "hourly_zone_demand")
save(gold.daily_borough_revenue(fact, zones), "daily_borough_revenue")
save(gold.top_routes(fact, zones), "top_routes")
save(gold.tip_behaviour(fact), "tip_behaviour")
save(gold.driver_performance(fact), "driver_performance")

# COMMAND ----------

spark.sql(f"OPTIMIZE {cfg.table('gold', 'fact_trips')} ZORDER BY (pickup_zone_key)")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT z.borough, COUNT(*) trips, ROUND(SUM(f.total_amount), 2) revenue
# MAGIC FROM nyc_taxi_gold.fact_trips f JOIN nyc_taxi_gold.dim_zone z ON f.pickup_zone_key = z.zone_key
# MAGIC GROUP BY z.borough ORDER BY revenue DESC
