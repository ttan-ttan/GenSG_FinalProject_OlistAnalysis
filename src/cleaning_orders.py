"""
Cleaning logic for the Olist Orders dataset.

Purpose:
    Prepare raw order data for validation and the Silver layer.

Cleaning handled here:
    1. Convert order date/time columns into timestamp format.
    2. Standardise order_status by trimming spaces and converting to lowercase.

Important:
    Business-rule issues such as missing customer_id values or invalid
    timestamp sequences are handled during validation, not silently fixed here.
"""

from pyspark.sql import functions as F


def clean_orders(df):
    """
    Parameters:
        df: PySpark DataFrame containing raw order records.

    Returns:
        PySpark DataFrame with standardised timestamps and order status.
    """

    # Scenario 1:
    # Raw date fields may arrive as strings.
    # Convert to timestamps so later comparisons can be performed reliably.
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for column in timestamp_columns:
        df = df.withColumn(column, F.to_timestamp(F.col(column)))

    # Scenario 2:
    # Status values may contain inconsistent casing or extra spaces,
    # e.g. " Delivered " and "DELIVERED".
    # Standardising them prevents the same status being treated as different values.
    df = df.withColumn("order_status", F.lower(F.trim(F.col("order_status"))))

    return df
