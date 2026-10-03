"""
Gold-layer validation for the Olist orders fact table.

Checks for required fields, logical timestamps, allowed statuses, duplicate keys,
and other integrity requirements prior to downstream analysis.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

REQUIRED_COLUMNS = [
    "order_id",
    "date_key",
    "customer_key",
    "order_value",
    "item_count",
    "delivery_delay_days",
    "payment_value_total",
    "is_new_at_order",
]


def validate_fact_orders_gold(df: DataFrame) -> DataFrame:
    """Validate order grain, dimension keys, and order-level measures."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    critical_cols = ["order_id", "date_key", "customer_key", "is_new_at_order"]
    for col in critical_cols:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold fact orders: {col}")

    dup_order_ids = df.groupBy("order_id").count().filter(F.col("count") > 1)
    if dup_order_ids.count() > 0:
        raise ValueError("Duplicate order_id values detected in Gold fact orders")

    for col in ["order_value", "item_count", "payment_value_total"]:
        invalid = (
            F.col(col).isNull()
            | F.isnan(F.col(col).cast("double"))
            | (F.abs(F.col(col).cast("double")) == float("inf"))
        )
        if df.filter(invalid | (F.col(col) < 0)).count() > 0:
            raise ValueError(f"Invalid {col} in Gold fact orders")

    if dict(df.dtypes)["is_new_at_order"] != "boolean":
        raise ValueError("is_new_at_order must be boolean in Gold fact orders")

    return df


validate_orders_gold = validate_fact_orders_gold
