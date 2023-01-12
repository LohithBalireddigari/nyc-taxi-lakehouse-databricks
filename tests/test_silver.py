from decimal import Decimal

from conftest import raw_trip
from taxi_lakehouse import silver


def test_valid_trip_is_typed_and_enriched(make_bronze_trips, zones):
    valid, quarantine = silver.clean_trips(make_bronze_trips([raw_trip()]))
    assert quarantine.count() == 0
    row = silver.enrich_with_zones(valid, zones).collect()[0]
    assert row.duration_min == 20.0
    assert row.avg_speed_mph == 9.6
    assert row.fare_amount == Decimal("15.50")
    assert row.tip_pct == 20.0
    assert row.payment_desc == "Credit card"
    assert row.pickup_hour == 8 and row.day_of_week == "Tue" and row.is_weekend is False
    assert (row.pickup_borough, row.pickup_zone, row.dropoff_zone) == ("Manhattan", "Midtown Center",
                                                                       "Upper East Side North")
    assert row.is_airport_trip is False


def test_exact_duplicates_are_removed(make_bronze_trips):
    valid, _ = silver.clean_trips(make_bronze_trips([raw_trip(), raw_trip()]))
    assert valid.count() == 1


def test_bad_trips_are_quarantined_with_reasons(make_bronze_trips):
    rows = [
        raw_trip(fare="-15.50", total="-22.40"),                                       # negative money
        raw_trip(distance="0"),                                                         # zero distance
        raw_trip(dropoff="2022-11-01 07:55:00"),                                        # dropoff before pickup
        raw_trip(passengers="0"),                                                       # no passengers
        raw_trip(pu="265"),                                                             # unknown zone
        raw_trip(distance="900"),                                                       # absurd distance
        raw_trip(pickup="not-a-date"),                                                  # unparseable
    ]
    valid, quarantine = silver.clean_trips(make_bronze_trips(rows))
    assert valid.count() == 0
    reasons = sorted(tuple(sorted(r._failed_rules)) for r in quarantine.collect())
    assert ("fare_non_negative",) in reasons
    assert ("distance_positive",) in reasons
    assert ("passenger_count_1_to_6",) in reasons
    assert ("known_pickup_zone",) in reasons
    assert ("distance_positive", "plausible_speed") in reasons
    assert any("dropoff_after_pickup" in r and "duration_1min_to_4h" in r for r in reasons)
    assert any("valid_pickup_ts" in r for r in reasons)


def test_cash_trip_has_no_tip_pct_and_airport_flag(make_bronze_trips):
    valid, _ = silver.clean_trips(make_bronze_trips([raw_trip(payment="2", tip="0", pu="132")]))
    row = valid.collect()[0]
    assert row.tip_pct is None
    assert row.payment_desc == "Cash"
    assert row.is_airport_trip is True


def test_clean_drivers(spark):
    from taxi_lakehouse.bronze import with_metadata
    from taxi_lakehouse.schemas import DRIVERS_RAW

    df = with_metadata(spark.createDataFrame([
        ("D1", "ana lopez", "Tesla Model Y", "LIC Fleet", "active", 4.9, "2022-10-01"),
        ("", "No Id", "Tesla Model Y", "LIC Fleet", "ACTIVE", 4.9, "2022-10-01"),
        ("D2", "Bad", "Tesla Model Y", "LIC Fleet", "ACTIVE", 7.0, "2022-10-01"),
    ], DRIVERS_RAW))
    valid, quarantine = silver.clean_drivers(df)
    v = valid.collect()
    assert len(v) == 1 and v[0].driver_name == "Ana Lopez" and v[0].status == "ACTIVE"
    assert sorted(r._failed_rules[0] for r in quarantine.collect()) == ["driver_id_present", "rating_1_to_5"]
