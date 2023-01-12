"""End-to-end runner.

    python -m taxi_lakehouse.pipeline --local       # local Spark, Parquet output, prints a summary

On Databricks the notebooks in ``notebooks/`` run the same functions and
write Delta tables; see ``conf/deployment.yml`` for the workflow.
"""

import argparse
import json
import os
import sys
import time
from datetime import date
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import bronze, features, gold, ml, quality, scd2, silver, streaming

DRIVER_TRACKED = ("vehicle_type", "base_name", "status")


def build_tables(spark: SparkSession, raw: str) -> Dict[str, DataFrame]:
    t: Dict[str, DataFrame] = {}

    # Bronze
    trips_b = bronze.read_trips(spark, f"{raw}/trips")
    zones = bronze.read_zones(spark, f"{raw}/zones")
    drivers_b = bronze.read_drivers(spark, f"{raw}/drivers")
    t["bronze_trips"] = trips_b

    # Silver: trips
    trips_ok, trips_bad = silver.clean_trips(trips_b)
    trips_ok = silver.enrich_with_zones(trips_ok, zones).cache()
    t["silver_trips"], t["silver_trips_quarantine"] = trips_ok, trips_bad

    # Silver: drivers SCD2 (apply snapshots in order)
    drivers_ok, drivers_bad = silver.clean_drivers(drivers_b)
    t["silver_drivers_quarantine"] = drivers_bad
    dim_driver = None
    for (snap,) in drivers_ok.select("snapshot_date").distinct().orderBy("snapshot_date").collect():
        batch = drivers_ok.filter(F.col("snapshot_date") == snap)
        dim_driver = scd2.apply_scd2(dim_driver, batch, "driver_id", DRIVER_TRACKED, "snapshot_date").cache()
    t["gold_dim_driver"] = dim_driver

    # Gold: star schema + marts
    fact = gold.fact_trips(trips_ok, dim_driver).cache()
    t["gold_fact_trips"] = fact
    t["gold_dim_zone"] = gold.dim_zone(zones)
    t["gold_dim_date"] = gold.dim_date(spark, date(2022, 10, 1), date(2022, 12, 31))
    t["gold_hourly_zone_demand"] = gold.hourly_zone_demand(fact, zones)
    t["gold_daily_borough_revenue"] = gold.daily_borough_revenue(fact, zones)
    t["gold_top_routes"] = gold.top_routes(fact, zones)
    t["gold_tip_behaviour"] = gold.tip_behaviour(fact)
    t["gold_driver_performance"] = gold.driver_performance(fact)
    t["gold_zone_demand_15min"] = streaming.zone_demand_windows(trips_ok)

    t["gold_data_quality"] = spark.createDataFrame([
        quality.report("trips", trips_ok, trips_bad).as_dict(),
        quality.report("drivers", drivers_ok, drivers_bad).as_dict(),
    ])
    return t


def local_spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ["TZ"] = "UTC"
    time.tzset()
    spark = (
        SparkSession.builder.master("local[*]").appName("nyc-taxi-lakehouse")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--local", action="store_true")
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--out", default="output")
    p.add_argument("--cutoff", default="2022-12-10 00:00:00", help="train/test split for the forecast model")
    a = p.parse_args()
    if not a.local:
        sys.exit("On Databricks run the notebooks / workflow; use --local here.")

    spark = local_spark()
    t0 = time.time()
    tables = build_tables(spark, a.raw)
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(f"{a.out}/{name}")

    dim = tables["gold_dim_driver"]
    summary = {
        "data_quality": [r.asDict() for r in tables["gold_data_quality"].collect()],
        "trip_rule_failures": {r.rule: r["count"] for r in
                               quality.failure_counts(tables["silver_trips_quarantine"]).collect()},
        "dim_driver_rows": dim.count(),
        "dim_driver_current": dim.filter("is_current").count(),
        "dim_driver_closed_versions": dim.filter("NOT is_current").count(),
        "fact_trips": tables["gold_fact_trips"].count(),
        "fact_trips_without_driver": tables["gold_fact_trips"].filter("driver_sk IS NULL").count(),
    }

    feats = features.build_training_set(tables["silver_trips"], tables["gold_dim_zone"]
                                        .filter("zone_key <= 263").select("zone_key"))
    feats.cache()
    model, metrics, _ = ml.train_demand_model(feats, a.cutoff)
    summary["forecast_rows"] = feats.count()
    summary["forecast_metrics"] = metrics
    summary["feature_importance"] = ml.feature_importance(model)
    summary["runtime_sec"] = round(time.time() - t0, 1)
    print(json.dumps(summary, indent=2, default=str))

    tables["gold_top_routes"].select("route_rank", "pickup_zone", "dropoff_zone", "trips", "avg_total",
                                     "avg_duration_min").show(10, truncate=False)
    tables["gold_daily_borough_revenue"].groupBy("borough").agg(
        F.sum("trips").alias("trips"), F.round(F.sum("revenue"), 2).alias("revenue")
    ).orderBy(F.desc("revenue")).show()
    tables["gold_tip_behaviour"].show(24)


if __name__ == "__main__":
    main()
