import datetime as dt

import pytest

from taxi_lakehouse import scd2

TRACKED = ("vehicle_type", "base_name", "status")
SCHEMA = "driver_id STRING, driver_name STRING, vehicle_type STRING, base_name STRING, status STRING, " \
         "rating DOUBLE, snapshot_date DATE"


@pytest.fixture
def snapshots(spark):
    oct1, nov15 = dt.date(2022, 10, 1), dt.date(2022, 11, 15)
    first = spark.createDataFrame([
        ("D1", "Ana", "Camry", "Midtown", "ACTIVE", 4.8, oct1),
        ("D2", "Ben", "RAV4", "LIC", "ACTIVE", 4.6, oct1),
        ("D3", "Cai", "NV200", "LIC", "ACTIVE", 4.9, oct1),
    ], SCHEMA)
    second = spark.createDataFrame([
        ("D1", "Ana", "Model Y", "Midtown", "ACTIVE", 4.7, nov15),   # vehicle changed -> new version
        ("D2", "Ben", "RAV4", "LIC", "ACTIVE", 4.5, nov15),          # only rating changed -> type-1 refresh
        ("D4", "Dee", "Camry", "Bronx", "ACTIVE", 5.0, nov15),       # new driver
    ], SCHEMA)
    return first, second


def _apply(first, second):
    dim = scd2.apply_scd2(None, first, "driver_id", TRACKED, "snapshot_date")
    return scd2.apply_scd2(dim, second, "driver_id", TRACKED, "snapshot_date")


def test_initial_load_is_all_current(snapshots):
    dim = scd2.apply_scd2(None, snapshots[0], "driver_id", TRACKED, "snapshot_date")
    rows = dim.collect()
    assert len(rows) == 3
    assert all(r.is_current and r.effective_to is None and r.effective_from == dt.date(2022, 10, 1) for r in rows)


def test_changed_driver_gets_new_version(snapshots):
    dim = _apply(*snapshots)
    d1 = sorted(dim.filter("driver_id = 'D1'").collect(), key=lambda r: r.effective_from)
    assert [(r.vehicle_type, r.is_current) for r in d1] == [("Camry", False), ("Model Y", True)]
    assert d1[0].effective_to == dt.date(2022, 11, 15)
    assert d1[1].effective_to is None
    assert d1[0].driver_sk != d1[1].driver_sk


def test_untracked_change_is_type1(snapshots):
    d2 = _apply(*snapshots).filter("driver_id = 'D2'").collect()
    assert len(d2) == 1
    assert d2[0].rating == 4.5 and d2[0].effective_from == dt.date(2022, 10, 1)


def test_new_and_missing_drivers(snapshots):
    dim = _apply(*snapshots)
    assert dim.filter("driver_id = 'D4' AND is_current").count() == 1
    assert dim.filter("driver_id = 'D3' AND is_current").count() == 1  # absent from snapshot -> kept
    assert dim.count() == 5
    assert dim.filter("is_current").count() == 4


def test_reapplying_same_snapshot_is_idempotent(snapshots):
    first, second = snapshots
    once = _apply(first, second)
    twice = scd2.apply_scd2(once, second, "driver_id", TRACKED, "snapshot_date")
    assert sorted(map(tuple, once.collect())) == sorted(map(tuple, twice.collect()))


def test_staged_updates_for_merge(snapshots, spark):
    first, second = snapshots
    current = scd2.prepare_incoming(first, "driver_id", TRACKED, "snapshot_date")
    incoming = scd2.prepare_incoming(second, "driver_id", TRACKED, "snapshot_date")
    staged = scd2.staged_updates_for_merge(current, incoming, "driver_id").collect()
    keys = sorted((r.driver_id, r.merge_key is None) for r in staged)
    # D1 changed -> twice (close + insert), D2 unchanged and D4 new -> once each
    assert keys == [("D1", False), ("D1", True), ("D2", False), ("D4", False)]


def test_point_in_time_join(snapshots, spark):
    dim = _apply(*snapshots)
    facts = spark.createDataFrame([
        ("T1", "D1", dt.datetime(2022, 10, 20, 9)),
        ("T2", "D1", dt.datetime(2022, 12, 1, 9)),
        ("T3", "D9", dt.datetime(2022, 12, 1, 9)),
    ], "trip_id STRING, driver_id STRING, pickup_ts TIMESTAMP")
    out = {r.trip_id: r for r in scd2.point_in_time_join(facts, dim, "driver_id", "pickup_ts",
                                                         ["vehicle_type"]).collect()}
    assert out["T1"].vehicle_type == "Camry"
    assert out["T2"].vehicle_type == "Model Y"
    assert out["T3"].driver_sk is None
