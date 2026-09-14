# pylint: disable=invalid-unary-operand-type
# pylint: disable=no-member

"""
Validation rules for the Silver Customers dataset.
"""

import pyspark.sql.functions as F


def validate_customers(df):
    """Validate the Silver customers dataframe."""

    # Required columns
    required_columns = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # customer_id must be non-null
    if df.filter(F.col("customer_id").isNull()).count() > 0:
        raise ValueError("customer_id contains null values")

    # customer_id must be unique
    dup_count = df.groupBy("customer_id").count().filter(
        F.col("count") > 1).count()
    if dup_count > 0:
        raise ValueError("Duplicate customer_id values detected")

    # Validate state format (two uppercase letters)
    invalid_states = df.filter(
        ~F.col("customer_state").rlike("^[A-Z]{2}$")).count()
    if invalid_states > 0:
        raise ValueError("Invalid customer_state format detected")

    # Validate ZIP code range (avoid unary ~)
    invalid_zip = df.filter(
        F.col("customer_zip_code_prefix").between(1000, 99999) == False
    ).count()

    if invalid_zip > 0:
        raise ValueError("Invalid customer_zip_code_prefix detected")

    return df
