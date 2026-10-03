""" validation_fact_order_item_gold
This module contains functions to validate the Gold fact order-item table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "order_item_id",
    "order_id",
    "date_key",
    "customer_key",
    "product_key",
    "seller_key",
    "price",
    "freight_value",
    "price_vs_baseline_pct",
]


def validate_fact_order_item_gold(
    df: DataFrame, fact_orders: DataFrame | None = None
) -> DataFrame:
    """Validate Gold order-item rows."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    key_cols = ["order_item_id", "order_id"]
    for col in key_cols + [
        "date_key",
        "customer_key",
        "product_key",
        "seller_key",
        "price",
        "freight_value",
    ]:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold fact order item: {col}")

    dup = df.groupBy("order_id", "order_item_id").count().filter(F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate order_id/order_item_id pair detected")

    if df.filter(F.col("order_item_id") <= 0).count() > 0:
        raise ValueError("Invalid order_item_id: values must be positive")

    if (
        df.filter(
            F.isnan("price")
            | (F.abs(F.col("price")) == float("inf"))
            | (F.col("price") <= 0)
        ).count()
        > 0
    ):
        raise ValueError("Invalid price detected")

    if (
        df.filter(
            F.isnan("freight_value")
            | (F.abs(F.col("freight_value")) == float("inf"))
            | (F.col("freight_value") < 0)
        ).count()
        > 0
    ):
        raise ValueError("Invalid freight_value detected")

    if (
        df.filter(
            F.col("price_vs_baseline_pct").isNotNull()
            & (
                F.isnan("price_vs_baseline_pct")
                | (F.abs(F.col("price_vs_baseline_pct")) == float("inf"))
            )
        ).count()
        > 0
    ):
        raise ValueError("Invalid price_vs_baseline_pct detected")

    if fact_orders is not None:
        expected_orders = fact_orders.select("order_id", "date_key", "customer_key")
        orphans = df.join(
            expected_orders,
            ["order_id", "date_key", "customer_key"],
            "left_anti",
        )
        if orphans.limit(1).count() > 0:
            raise ValueError("Orphan order/date/customer key in Gold fact order item")

    return df


validate_order_item_gold = validate_fact_order_item_gold
