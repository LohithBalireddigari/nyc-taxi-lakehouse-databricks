import datetime as dt

from conftest import raw_trip
from taxi_lakehouse import gold, scd2, silver, streaming


def _fact(spark, make_bronze_trips, zones, rows):
    trips, _ = silver.clean_trips(make_bronze_trips(rows))
    trips = silver.enrich_with_zones(trips, zones)
    drivers = spark.createDataFrame(
        [("D0001", "Ana", "Camry", "Midtown", "ACTIVE", 4.8, dt.date(2022, 10, 1))],
        "driver_id STRING, driver_name STRING, vehicle_type STRING, base_name STRING, status STRING, "
        "rating DOUBLE, snapshot_date DATE",
    )
    dim = scd2.apply_scd2(None, drivers, "driver_id", ("vehicle_type", "base_name", "status"), "snapshot_date")
    return trips, gold.fact_trips(trips, dim)


ROWS = [
    raw_trip(pickup="2022-11-01 08:01:00", dropoff="2022-11-01 08:21:00"),
    raw_trip(pickup="2022-11-01 08:07:00", dropoff="2022-11-01 08:30:00", total="30.00", tip="5.00"),
    raw_trip(pickup="2022-11-01 08:20:00", dropoff="2022-11-01 08:45:00", pu="132", do="161", fare="52.00",
             total="70.00", tip="0", payment="2"),
    raw_trip(pickup="2022-11-02 18:00:00", dropoff="2022-11-02 18:30:00", driver="D0404"),
]


def test_fact_trips_keys_and_driver(spark, make_bronze_trips, zones):
    _, fact = _fact(spark, make_bronze_trips, zones, ROWS)
    rows = fact.collect()
    assert len(rows) == 4
    assert {r.pickup_date_key for r in rows} == {20221101, 20221102}
    known = [r for r in rows if r.driver_id == "D0001"]
    assert all(r.driver_sk is not None and r.driver_base == "Midtown" for r in known)
    assert [r.driver_sk for r in rows if r.driver_id == "D0404"] == [None]


def test_dim_date(spark):
    d = gold.dim_date(spark, dt.date(2022, 11, 23), dt.date(2022, 11, 27)).collect()
    assert [r.date_key for r in d] == [20221123, 20221124, 20221125, 20221126, 20221127]
    thanksgiving = d[1]
    assert thanksgiving.is_holiday and thanksgiving.day_name == "Thursday"
    assert [r.is_weekend for r in d] == [False, False, False, True, True]


def test_hourly_zone_demand_and_borough_revenue(spark, make_bronze_trips, zones):
    _, fact = _fact(spark, make_bronze_trips, zones, ROWS)
    hz = {(r.pickup_date.isoformat(), r.pickup_hour, r.pickup_zone_key): r
          for r in gold.hourly_zone_demand(fact, zones).collect()}
    midtown_8am = hz[("2022-11-01", 8, 161)]
    assert midtown_8am.trips == 2 and float(midtown_8am.revenue) == 52.40 and midtown_8am.borough == "Manhattan"
    rev = {(r.pickup_date.isoformat(), r.borough): r for r in gold.daily_borough_revenue(fact, zones).collect()}
    assert rev[("2022-11-01", "Queens")].trips == 1


def test_top_routes_and_tips(spark, make_bronze_trips, zones):
    _, fact = _fact(spark, make_bronze_trips, zones, ROWS)
    routes = gold.top_routes(fact, zones, n=1).collect()
    assert len(routes) == 1
    assert (routes[0].pickup_zone, routes[0].dropoff_zone, routes[0].trips) == ("Midtown Center",
                                                                               "Upper East Side North", 3)
    tips = {r.pickup_hour: r for r in gold.tip_behaviour(fact).collect()}
    assert tips[8].card_trips == 2  # cash JFK trip excluded


def test_zone_demand_windows(spark, make_bronze_trips, zones):
    trips, _ = _fact(spark, make_bronze_trips, zones, ROWS)
    w = {(r.window_start.strftime("%H:%M"), r.PULocationID): r.trips
         for r in streaming.zone_demand_windows(trips, "15 minutes").collect()}
    assert w[("08:00", 161)] == 2
    assert w[("08:15", 132)] == 1


def test_surge_candidates(spark):
    windows = spark.createDataFrame(
        [(dt.datetime(2022, 11, 1, 8, 0), 161, 9), (dt.datetime(2022, 11, 1, 8, 0), 236, 2)],
        "window_start TIMESTAMP, PULocationID INT, trips LONG",
    )
    baseline = spark.createDataFrame([(161, 8, 4.0), (236, 8, 2.0)], "PULocationID INT, hour INT, avg_trips DOUBLE")
    out = {r.PULocationID: r for r in streaming.surge_candidates(windows, baseline).collect()}
    assert out[161].is_surge and out[161].demand_ratio == 2.25
    assert not out[236].is_surge
