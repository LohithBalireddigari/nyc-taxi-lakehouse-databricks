"""Train and evaluate a demand-forecasting model (Spark MLlib GBT), tracked in MLflow."""

from typing import Dict, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .features import FEATURE_COLS

MODEL_FEATURES = FEATURE_COLS + ["zone_avg_demand"]


def time_split(df: DataFrame, cutoff: str) -> Tuple[DataFrame, DataFrame]:
    """Train on everything before ``cutoff``, test on everything after (no leakage)."""
    ts = F.to_timestamp(F.lit(cutoff))  # interpreted in the session time zone (UTC)
    return df.filter(F.col("hour_ts") < ts), df.filter(F.col("hour_ts") >= ts)


def add_zone_encoding(train: DataFrame, test: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Target-encode the zone using the *training* period only."""
    enc = train.groupBy("zone_id").agg(F.avg("trips").alias("zone_avg_demand"))
    return train.join(enc, "zone_id"), test.join(enc, "zone_id", "left").fillna(0.0, ["zone_avg_demand"])


def _metrics(pred: DataFrame, col: str) -> Dict[str, float]:
    ev = RegressionEvaluator(labelCol="trips", predictionCol=col)
    return {m: round(ev.setMetricName(m).evaluate(pred), 4) for m in ("rmse", "mae", "r2")}


def train_demand_model(features: DataFrame, cutoff: str, max_iter: int = 40, max_depth: int = 5,
                       seed: int = 42, log_to_mlflow: bool = False) -> Tuple[PipelineModel, Dict[str, Dict[str, float]], DataFrame]:
    train, test = add_zone_encoding(*time_split(features, cutoff))
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=MODEL_FEATURES, outputCol="features"),
        GBTRegressor(labelCol="trips", featuresCol="features", maxIter=max_iter, maxDepth=max_depth,
                     stepSize=0.1, seed=seed),
    ])
    model = pipeline.fit(train)
    pred = model.transform(test).withColumn("lag_168h_d", F.col("lag_168h").cast("double")) \
                                .withColumn("lag_24h_d", F.col("lag_24h").cast("double"))
    metrics = {
        "gbt": _metrics(pred, "prediction"),
        "baseline_same_hour_last_week": _metrics(pred, "lag_168h_d"),
        "baseline_same_hour_yesterday": _metrics(pred, "lag_24h_d"),
    }

    if log_to_mlflow:
        import mlflow
        import mlflow.spark

        with mlflow.start_run(run_name="gbt_zone_demand"):
            mlflow.log_params({"max_iter": max_iter, "max_depth": max_depth, "cutoff": cutoff,
                               "features": ",".join(MODEL_FEATURES)})
            for name, vals in metrics.items():
                mlflow.log_metrics({f"{name}_{k}": v for k, v in vals.items()})
            mlflow.spark.log_model(model, "model", registered_model_name="nyc_taxi_zone_demand")

    return model, metrics, pred


def feature_importance(model: PipelineModel) -> Dict[str, float]:
    gbt = model.stages[-1]
    return dict(sorted(zip(MODEL_FEATURES, [round(float(x), 4) for x in gbt.featureImportances.toArray()]),
                       key=lambda kv: -kv[1]))
