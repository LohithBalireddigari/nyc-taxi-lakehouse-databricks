# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the bronze / silver / gold databases and lands three months of synthetic
# MAGIC NYC yellow-taxi data (Oct-Dec 2022) in DBFS.
# MAGIC
# MAGIC Tested on **DBR 12.0** (Spark 3.3.1, Delta 2.2).

# COMMAND ----------

dbutils.widgets.text("root", "dbfs:/FileStore/nyc_taxi")
dbutils.widgets.text("trips", "60000")

import sys, os
sys.path.append(os.path.abspath("../src"))

from taxi_lakehouse.config import LAYERS, Config
from taxi_lakehouse.data_generator import generate

cfg = Config(root=dbutils.widgets.get("root"))

# COMMAND ----------

for layer in LAYERS:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.database(layer)}")
display(spark.sql(f"SHOW DATABASES LIKE '{cfg.db_prefix}*'"))

# COMMAND ----------

summary = generate(cfg.raw_path_local, n_trips=int(dbutils.widgets.get("trips")))
summary

# COMMAND ----------

display(dbutils.fs.ls(f"{cfg.raw_path}/trips"))
