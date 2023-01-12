"""Declarative data-quality rules.

The same rule definitions are used by the batch pipeline (rows that fail go to
a quarantine table) and by the Delta Live Tables pipeline (``expect_or_drop``),
so expectations only live in one place.
"""

from dataclasses import dataclass
from typing import Dict, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# SQL expressions -> reusable as Column (batch) and as DLT expectation strings.
TRIP_RULES: Dict[str, str] = {
    "valid_pickup_ts": "pickup_ts IS NOT NULL",
    "dropoff_after_pickup": "dropoff_ts > pickup_ts",
    "duration_1min_to_4h": "duration_min BETWEEN 1 AND 240",
    "distance_positive": "trip_distance > 0 AND trip_distance <= 100",
    "fare_non_negative": "fare_amount >= 0 AND total_amount >= 0",
    "passenger_count_1_to_6": "passenger_count BETWEEN 1 AND 6",
    "plausible_speed": "avg_speed_mph <= 80",
    "known_pickup_zone": "PULocationID BETWEEN 1 AND 263",
    "known_dropoff_zone": "DOLocationID BETWEEN 1 AND 263",
}

DRIVER_RULES: Dict[str, str] = {
    "driver_id_present": "driver_id IS NOT NULL",
    "rating_1_to_5": "rating BETWEEN 1 AND 5",
    "status_known": "status IN ('ACTIVE', 'SUSPENDED', 'INACTIVE')",
}


def as_columns(rules: Dict[str, str]) -> Dict[str, Column]:
    return {name: F.expr(sql) for name, sql in rules.items()}


def split_by_rules(df: DataFrame, rules: Dict[str, str]) -> Tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``; null rule results count as failures."""
    checks = [
        F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in as_columns(rules).items()
    ]
    tagged = df.withColumn("_failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))
    valid = tagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantine = tagged.filter(F.size("_failed_rules") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, quarantine


def failure_counts(quarantine: DataFrame) -> DataFrame:
    return (
        quarantine.select(F.explode("_failed_rules").alias("rule"))
        .groupBy("rule")
        .count()
        .orderBy(F.desc("count"), "rule")
    )


@dataclass
class Report:
    dataset: str
    total: int
    valid: int
    quarantined: int

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total == 0 else round(self.valid / self.total, 4)

    def as_dict(self) -> dict:
        return {**self.__dict__, "pass_rate": self.pass_rate}


def report(dataset: str, valid: DataFrame, quarantine: DataFrame) -> Report:
    v, q = valid.count(), quarantine.count()
    return Report(dataset, v + q, v, q)
