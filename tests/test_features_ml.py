import datetime as dt
import math

import pytest

from taxi_lakehouse import features, ml


@pytest.fixture(scope="module")
def synthetic_trips(spark):
    """3 weeks of trips for 2 zones with a clear daily pattern (more trips at 8am/6pm)."""
    rows = []
    start = dt.datetime(2022, 11, 1)
    for h in range(24 * 21):
        ts = start + dt.timedelta(hours=h)
        base = 6 if ts.hour in (8, 18) else 2 if 7 <= ts.hour <= 22 else 0
        for zone, mult in ((161, 2), (236, 1)):
            for k in range(base * mult):
                rows.append((zone, ts + dt.timedelta(minutes=k)))
    return spark.createDataFrame(rows, "PULocationID INT, pickup_ts TIMESTAMP")


def test_grid_fills_zero_hours(spark, synthetic_trips):
    zones = spark.createDataFrame([(161,), (236,), (999,)], "zone_key INT")
    grid = features.hourly_demand_grid(synthetic_trips, zones)
    # first trip Nov 1 07:00, last trip Nov 21 22:00 -> 20 days + 15 h -> 496 hourly slots
    assert grid.select("hour_ts").distinct().count() == 496
    assert grid.count() == 3 * 496  # 3 zones incl. one with no trips at all
    assert grid.filter("zone_id = 999").agg({"trips": "sum"}).collect()[0][0] == 0
    assert grid.filter("zone_id = 161 AND hour(hour_ts) = 3").agg({"trips": "max"}).collect()[0][0] == 0


def test_lag_features(spark, synthetic_trips):
    zones = spark.createDataFrame([(161,)], "zone_key INT")
    f = features.build_training_set(synthetic_trips, zones)
    row = f.filter("hour = 8").orderBy("hour_ts").first()
    assert row.lag_24h == row.trips == 12          # same hour yesterday, same pattern
    assert row.lag_168h == 12
    assert row.lag_1h == 4                         # 7am
    assert f.filter("lag_168h IS NULL").count() == 0
    assert set(features.FEATURE_COLS) <= set(f.columns)


def test_train_demand_model_beats_nothing_and_logs_metrics(spark, synthetic_trips):
    zones = spark.createDataFrame([(161,), (236,)], "zone_key INT")
    f = features.build_training_set(synthetic_trips, zones)
    model, metrics, pred = ml.train_demand_model(f, "2022-11-18 00:00:00", max_iter=10, max_depth=3)
    assert pred.count() > 0
    assert set(metrics) == {"gbt", "baseline_same_hour_last_week", "baseline_same_hour_yesterday"}
    assert metrics["gbt"]["rmse"] < 1.0           # perfectly periodic signal -> near-exact fit
    assert not math.isnan(metrics["gbt"]["r2"])
    imp = ml.feature_importance(model)
    assert set(imp) == set(ml.MODEL_FEATURES)
    assert abs(sum(imp.values()) - 1.0) < 1e-3


def test_time_split_has_no_overlap(spark, synthetic_trips):
    zones = spark.createDataFrame([(161,)], "zone_key INT")
    f = features.build_training_set(synthetic_trips, zones)
    train, test = ml.time_split(f, "2022-11-18 00:00:00")
    assert train.agg({"hour_ts": "max"}).collect()[0][0] < test.agg({"hour_ts": "min"}).collect()[0][0]
