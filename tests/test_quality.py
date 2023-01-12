from taxi_lakehouse import quality


def test_split_and_failure_counts(spark):
    df = spark.createDataFrame([(1, 5), (2, -1), (None, -2)], "driver_id INT, rating INT")
    rules = {"driver_id_present": "driver_id IS NOT NULL", "rating_positive": "rating > 0"}
    valid, quarantine = quality.split_by_rules(df, rules)
    assert valid.count() == 1
    counts = {r.rule: r["count"] for r in quality.failure_counts(quarantine).collect()}
    assert counts == {"rating_positive": 2, "driver_id_present": 1}


def test_null_counts_as_failure(spark):
    df = spark.createDataFrame([(None,)], "rating INT")
    valid, quarantine = quality.split_by_rules(df, {"rating_positive": "rating > 0"})
    assert (valid.count(), quarantine.count()) == (0, 1)


def test_report(spark):
    df = spark.createDataFrame([(1,), (2,), (-3,), (4,)], "x INT")
    valid, quarantine = quality.split_by_rules(df, {"pos": "x > 0"})
    r = quality.report("t", valid, quarantine)
    assert r.as_dict() == {"dataset": "t", "total": 4, "valid": 3, "quarantined": 1, "pass_rate": 0.75}


def test_rules_are_valid_dlt_expectation_strings():
    # Every rule must be a plain SQL boolean so it can be passed to @dlt.expect_all_or_drop.
    for rules in (quality.TRIP_RULES, quality.DRIVER_RULES):
        for name, sql in rules.items():
            assert name.replace("_", "").isalnum()
            assert isinstance(sql, str) and sql.strip()
