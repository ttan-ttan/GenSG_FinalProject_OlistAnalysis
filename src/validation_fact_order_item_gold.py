""" validation_fact_order_item_gold
This module contains functions to validate the Gold fact order-item table."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]

REQUIRED_TYPES = {
    "order_item_id": "int",
    "shipping_limit_date": "timestamp",
    "price": "double",
    "freight_value": "double",
}


def validate_fact_order_item_gold(df: DataFrame) -> DataFrame:
    """Validate Gold order-item rows."""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    actual_types = dict(df.dtypes)
    for col, expected_type in REQUIRED_TYPES.items():
        if actual_types[col] != expected_type:
            raise ValueError(
                f"Invalid type for {col}: expected {expected_type}, got {actual_types[col]}"
            )

    key_cols = ["order_id", "order_item_id"]
    for col in key_cols + ["product_id", "seller_id", "price", "freight_value"]:
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

    if df.filter(F.col("shipping_limit_date").isNull()).count() > 0:
        raise ValueError("shipping_limit_date contains null values")

    return df


validate_order_item_gold = validate_fact_order_item_gold
