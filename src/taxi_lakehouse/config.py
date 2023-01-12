"""Paths and table names.

Targets Databricks Runtime 12.0 (Spark 3.3.1, Delta Lake 2.2) with the
workspace Hive metastore: one database per medallion layer.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    root: str = "dbfs:/FileStore/nyc_taxi"
    db_prefix: str = "nyc_taxi"

    @property
    def raw_path(self) -> str:
        return f"{self.root}/raw"

    @property
    def raw_path_local(self) -> str:
        """FUSE path for plain-Python writers (the data generator) on a cluster."""
        return self.raw_path.replace("dbfs:/", "/dbfs/")

    @property
    def checkpoint_root(self) -> str:
        return f"{self.root}/_checkpoints"

    def database(self, layer: str) -> str:
        return f"{self.db_prefix}_{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.database(layer)}.{name}"


LAYERS = ("bronze", "silver", "gold")

# Trips priced at the JFK flat fare.
JFK_FLAT_FARE = 52.0
AIRPORT_ZONES = (1, 132, 138)  # EWR, JFK, LaGuardia

PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}

# US federal holidays in the data window (Oct-Dec 2022) + Jan 2023.
HOLIDAYS = (
    "2022-10-10",  # Columbus Day
    "2022-11-11",  # Veterans Day
    "2022-11-24",  # Thanksgiving
    "2022-12-26",  # Christmas (observed)
    "2023-01-02",  # New Year (observed)
)
