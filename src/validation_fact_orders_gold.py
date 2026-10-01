"""
Gold-layer validation for the Olist orders fact table.

Checks for required fields, logical timestamps, allowed statuses, duplicate keys,
and other integrity requirements prior to downstream analysis.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

VALID_STATUSES = {
    "delivered",
    "shipped",
    "canceled",
    "cancelled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved",
}

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


def validate_fact_orders_gold(df: DataFrame) -> DataFrame:
    """Return the DataFrame if it passes the gold fact-order checks."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    critical_cols = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    ]
    for col in critical_cols:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"Null critical field in Gold fact orders: {col}")

    dup_order_ids = df.groupBy("order_id").count().filter(F.col("count") > 1)
    if dup_order_ids.count() > 0:
        raise ValueError("Duplicate order_id values detected in Gold fact orders")

    invalid_statuses = df.filter(
        F.col("order_status").isin(list(VALID_STATUSES)).__invert__()
    )
    if invalid_statuses.count() > 0:
        raise ValueError("Invalid order_status detected in Gold fact orders")

    impossible_time_rules = [
        (
            "order_approved_at occurs before order_purchase_timestamp",
            F.col("order_approved_at").isNotNull()
            & (F.col("order_approved_at") < F.col("order_purchase_timestamp")),
        ),
        (
            "order_delivered_carrier_date occurs before order_approved_at",
            F.col("order_delivered_carrier_date").isNotNull()
            & (F.col("order_delivered_carrier_date") < F.col("order_approved_at")),
        ),
        (
            "order_delivered_customer_date occurs before order_delivered_carrier_date",
            F.col("order_delivered_customer_date").isNotNull()
            & (
                F.col("order_delivered_customer_date")
                < F.col("order_delivered_carrier_date")
            ),
        ),
        (
            "order_estimated_delivery_date occurs before order_purchase_timestamp",
            F.col("order_estimated_delivery_date").isNotNull()
            & (
                F.col("order_estimated_delivery_date")
                < F.col("order_purchase_timestamp")
            ),
        ),
    ]

    for message, condition in impossible_time_rules:
        if df.filter(condition).count() > 0:
            raise ValueError(message)

    return df


validate_orders_gold = validate_fact_orders_gold
