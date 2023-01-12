"""Slowly Changing Dimension Type 2.

``apply_scd2`` is a pure-DataFrame implementation (used in tests and for
small dimensions). The notebook ``03_scd2_drivers`` uses the equivalent
Delta ``MERGE`` with the classic "staged updates / null merge key" pattern
built by :func:`staged_updates_for_merge`.
"""

from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SCD_COLS = ("driver_sk", "effective_from", "effective_to", "is_current", "row_hash")


def _hash(cols: Sequence[str]):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def prepare_incoming(snapshot: DataFrame, key: str, tracked: Sequence[str], effective_col: str) -> DataFrame:
    """Add hash + validity columns to a snapshot so it matches the dimension layout."""
    return (
        snapshot.withColumn("row_hash", _hash(tracked))
        .withColumn("effective_from", F.col(effective_col).cast("date"))
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("driver_sk", F.sha2(F.concat_ws("|", F.col(key), F.col("effective_from").cast("string")), 256))
        .drop(effective_col)
    )


def apply_scd2(dim: DataFrame, snapshot: DataFrame, key: str, tracked: Sequence[str], effective_col: str) -> DataFrame:
    """Return the new dimension after applying ``snapshot``.

    * key not in dim                    -> insert as current
    * key present, tracked cols changed -> close old row (effective_to, is_current=false), insert new row
    * key present, unchanged            -> keep old row; untracked attributes are refreshed (type 1)
    """
    incoming = prepare_incoming(snapshot, key, tracked, effective_col)
    cols = incoming.columns
    if dim is None or dim.rdd.isEmpty():
        return incoming.select(cols)

    history = dim.filter(~F.col("is_current")).select(cols)
    current = dim.filter(F.col("is_current")).select(cols)

    joined = current.alias("c").join(incoming.alias("n"), key, "full_outer")
    c_hash, n_hash = F.col("c.row_hash"), F.col("n.row_hash")

    untouched = joined.filter(n_hash.isNull()).select([F.col(f"c.{x}").alias(x) if x != key else F.col(key)
                                                       for x in cols])
    unchanged = joined.filter(c_hash.isNotNull() & (c_hash == n_hash)).select(
        [F.col(key)]
        + [F.col(f"c.{x}").alias(x) if x in SCD_COLS or x in tracked else F.col(f"n.{x}").alias(x)
           for x in cols if x != key]
    )
    changed = joined.filter(c_hash.isNotNull() & n_hash.isNotNull() & (c_hash != n_hash))
    closed = changed.select(
        [F.col(key)]
        + [
            F.col("n.effective_from").alias(x) if x == "effective_to"
            else F.lit(False).alias(x) if x == "is_current"
            else F.col(f"c.{x}").alias(x)
            for x in cols if x != key
        ]
    )
    new_versions = joined.filter(n_hash.isNotNull() & (c_hash.isNull() | (c_hash != n_hash))).select(
        [F.col(key)] + [F.col(f"n.{x}").alias(x) for x in cols if x != key]
    )
    return (
        history.unionByName(untouched).unionByName(unchanged).unionByName(closed).unionByName(new_versions)
        .select(cols)
    )


def staged_updates_for_merge(current: DataFrame, incoming: DataFrame, key: str) -> DataFrame:
    """Build the source for a single Delta MERGE that both closes and inserts.

    Changed keys appear twice: once with ``merge_key = key`` (matches -> close
    the old row) and once with ``merge_key = NULL`` (never matches -> insert
    the new version). New keys appear once with ``merge_key = key``.
    """
    changed = (
        incoming.alias("n")
        .join(current.alias("c"), key)
        .filter(F.col("c.row_hash") != F.col("n.row_hash"))
        .select(F.lit(None).cast("string").alias("merge_key"), "n.*")
    )
    return incoming.select(F.col(key).alias("merge_key"), "*").unionByName(changed)


def point_in_time_join(facts: DataFrame, dim: DataFrame, key: str, ts_col: str, attrs: Sequence[str]) -> DataFrame:
    """Attach the dimension version that was valid at each fact's timestamp."""
    d = dim.select(key, "driver_sk", "effective_from", "effective_to", *attrs).alias("d")
    cond = (
        (F.col(f"f.{key}") == F.col(f"d.{key}"))
        & (F.to_date(F.col(f"f.{ts_col}")) >= F.col("d.effective_from"))
        & (F.col("d.effective_to").isNull() | (F.to_date(F.col(f"f.{ts_col}")) < F.col("d.effective_to")))
    )
    return facts.alias("f").join(d, cond, "left").select("f.*", "d.driver_sk", *[F.col(f"d.{a}") for a in attrs])
