"""
Gold-layer cleaning for the Olist orders fact table.

This module prepares the fact-order dataset so it is ready for validation and
final gold-layer consumption.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def _standardize_columns(df: DataFrame) -> DataFrame:
    """Trim spaces and lowercase all column names."""
    return df.toDF(*[c.strip().lower() for c in df.columns])


def _check_required_columns(df: DataFrame) -> None:
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required column(s): {sorted(missing)}")


def clean_fact_orders_gold(df: DataFrame) -> DataFrame:
    """Clean a raw or silver fact-order DataFrame for the gold layer."""
    df = _standardize_columns(df)
    _check_required_columns(df)

    df = df.select(*REQUIRED_COLUMNS)

    df = (
        df.withColumn("order_id", F.trim(F.col("order_id").cast(StringType())))
        .withColumn("customer_id", F.trim(F.col("customer_id").cast(StringType())))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status").cast(StringType()))))
        .withColumn("order_purchase_timestamp", F.to_timestamp(F.col("order_purchase_timestamp")))
        .withColumn("order_approved_at", F.to_timestamp(F.col("order_approved_at")))
        .withColumn("order_delivered_carrier_date", F.to_timestamp(F.col("order_delivered_carrier_date")))
        .withColumn("order_delivered_customer_date", F.to_timestamp(F.col("order_delivered_customer_date")))
        .withColumn("order_estimated_delivery_date", F.to_timestamp(F.col("order_estimated_delivery_date")))
    )

    df = (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_status").isNotNull())
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .filter(F.col("order_purchase_timestamp") <= F.current_timestamp())
    )

    df = df.dropDuplicates()

    ordering_col = "__row_order__"
    df = df.withColumn(ordering_col, F.monotonically_increasing_id())
    window = Window.partitionBy("order_id").orderBy(F.col(ordering_col).asc())
    df = (
        df.withColumn("__rn__", F.row_number().over(window))
        .filter(F.col("__rn__") == 1)
        .drop("__rn__", ordering_col)
    )

    return df.select(*REQUIRED_COLUMNS)


# Common alias names used across the project.
clean_orders_gold = clean_fact_orders_gold
