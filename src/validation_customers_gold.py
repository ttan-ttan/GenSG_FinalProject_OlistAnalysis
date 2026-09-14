# pylint: disable=no-member
# pylint: disable=invalid-unary-operand-type

"""
Validation rules for the Gold Customers dataset.

Purpose:
    Ensure the enriched Gold customers table contains valid,
    consistent, and business‑correct metrics before BI consumption.

Gold Layer Columns:
    - customer_id
    - customer_unique_id
    - customer_first_purchase_date
    - customer_total_orders
    - customer_lifetime_value
    - customer_recency_days
    - customer_city
    - customer_state
"""

import pyspark.sql.functions as F


def validate_customers_gold(df):
    """
    Validate the Gold customers dataframe.

    Checks:
        1. Required columns exist
        2. customer_id is unique and non-null
        3. Derived metrics are non-null and non-negative
        4. customer_first_purchase_date is valid
        5. customer_state is valid (2-letter uppercase)

    Raises:
        ValueError: if any validation rule fails
    """

    #  1. Required columns
    required_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_first_purchase_date",
        "customer_total_orders",
        "customer_lifetime_value",
        "customer_recency_days",
        "customer_city",
        "customer_state"
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required Gold columns: {missing}")

    #  2. customer_id must be unique and non-null
    if df.filter(F.col("customer_id").isNull()).count() > 0:
        raise ValueError("customer_id contains null values")

    dup_count = df.groupBy("customer_id").count().filter(
        F.col("count") > 1).count()
    if dup_count > 0:
        raise ValueError("Duplicate customer_id values detected in Gold")

    # 3. Derived metrics must be non-null and non-negative
    numeric_cols = [
        "customer_total_orders",
        "customer_lifetime_value",
        "customer_recency_days"
    ]

    for col in numeric_cols:
        if df.filter(F.col(col).isNull()).count() > 0:
            raise ValueError(f"{col} contains null values")

        if df.filter(F.col(col) < 0).count() > 0:
            raise ValueError(f"{col} contains negative values")

    #  4. Validate first purchase date
    if df.filter(F.col("customer_first_purchase_date").isNull()).count() > 0:
        raise ValueError("customer_first_purchase_date contains null values")

    # Must be a valid date and not in the future
    invalid_dates = df.filter(
        F.col("customer_first_purchase_date") > F.current_date()
    ).count()

    if invalid_dates > 0:
        raise ValueError("customer_first_purchase_date contains future dates")

    # 5. Validate state format
    invalid_states = df.filter(
        ~F.col("customer_state").rlike("^[A-Z]{2}$")).count()
    if invalid_states > 0:
        raise ValueError("Invalid customer_state format detected in Gold")

    return df
