"""
validation_fact_order_item_gold
Validation for the Gold fact_order_item table.
"""

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
    """Validate Gold order-item fact rows."""
    # Schema check
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in gold_fact_order_item: {missing}")

    # Null checks
    critical_cols = [
        "order_item_id", "order_id", "date_key", "customer_key",
        "product_key", "seller_key", "price", "freight_value"
    ]
    for col in critical_cols:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(
                f"Null critical field in gold_fact_order_item: {col}")

    # Duplicate order-item pairs
    dup = df.groupBy("order_id", "order_item_id").count().filter(
        F.col("count") > 1)
    if dup.count() > 0:
        raise ValueError("Duplicate (order_id, order_item_id) pair detected")

    # order_item_id must be positive
    if df.filter(F.col("order_item_id") <= 0).count() > 0:
        raise ValueError("order_item_id must be positive")

    # Price validation
    invalid_price = (
        F.isnan("price")
        | (F.abs(F.col("price")) == float("inf"))
        | (F.col("price") <= 0)
    )
    if df.filter(invalid_price).count() > 0:
        raise ValueError("Invalid price detected in gold_fact_order_item")

    # Freight validation
    invalid_freight = (
        F.isnan("freight_value")
        | (F.abs(F.col("freight_value")) == float("inf"))
        | (F.col("freight_value") < 0)
    )
    if df.filter(invalid_freight).count() > 0:
        raise ValueError(
            "Invalid freight_value detected in gold_fact_order_item")

    # Baseline % validation
    invalid_pct = (
        F.col("price_vs_baseline_pct").isNotNull()
        & (
            F.isnan("price_vs_baseline_pct")
            | (F.abs(F.col("price_vs_baseline_pct")) == float("inf"))
        )
    )
    if df.filter(invalid_pct).count() > 0:
        raise ValueError("Invalid price_vs_baseline_pct detected")

    # Orphan FK check
    if fact_orders is not None:
        expected = fact_orders.select("order_id", "date_key", "customer_key")
        orphans = df.join(
            expected,
            ["order_id", "date_key", "customer_key"],
            "left_anti",
        )
        if orphans.limit(1).count() > 0:
            raise ValueError("Orphan FK detected in gold_fact_order_item")

    return df


validate_order_item_gold = validate_fact_order_item_gold
