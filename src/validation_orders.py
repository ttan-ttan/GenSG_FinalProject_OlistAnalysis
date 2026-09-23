# pylint: disable=invalid-unary-operand-type
# pylint: disable=no-member

"""
Validation rules for the Silver Orders dataset.

Purpose:
    Check that cleaned order data is complete, consistent, and suitable
    for downstream analysis.

Validation handled here:
    1. Ensure all required columns exist.
    2. Ensure key identifiers are not null.
    3. Ensure order_id values are unique.
    4. Ensure order_status contains only accepted values.
    5. Ensure approval timestamps follow a logical order.

Invalid records are not silently corrected because doing so could
introduce assumptions into the dataset.
"""

import pyspark.sql.functions as F


def validate_orders(df):
    """
    Validate the cleaned Olist Orders DataFrame.

    Parameters:
        df: PySpark DataFrame containing cleaned order records.

    Returns:
        The validated PySpark DataFrame.

    Raises:
        ValueError: If any validation rule fails.
    """

    # Required fields needed for order-level analysis.
    required_columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Scenario 1:
    # An order without an order_id cannot be uniquely identified.
    if df.filter(F.col("order_id").isNull()).count() > 0:
        raise ValueError("order_id contains null values")

    # Scenario 2:
    # A missing customer_id prevents the order from being linked
    # to a customer for customer-level behaviour analysis.
    if df.filter(F.col("customer_id").isNull()).count() > 0:
        raise ValueError("customer_id contains null values")

    # Each row in the Orders dataset should represent one unique order.
    dup_count = (
        df.groupBy("order_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if dup_count > 0:
        raise ValueError("Duplicate order_id values detected")

    # Cleaning handles casing and spaces.
    # Validation catches unknown or misspelled statuses such as "deliverd".
    valid_statuses = [
        "delivered",
        "shipped",
        "canceled",
        "unavailable",
        "invoiced",
        "processing",
        "created",
        "approved",
    ]

    invalid_statuses = df.filter(
        ~F.col("order_status").isin(valid_statuses)
    ).count()

    if invalid_statuses > 0:
        raise ValueError("Invalid order_status detected")

    # Approval should never occur before the order was purchased.
    # An impossible timestamp sequence indicates a dq issue.
    invalid_approval_time = df.filter(
        F.col("order_approved_at").isNotNull()
        & (F.col("order_approved_at") < F.col("order_purchase_timestamp"))
    ).count()

    if invalid_approval_time > 0:
        raise ValueError(
            "order_approved_at occurs before order_purchase_timestamp"
        )

    return df