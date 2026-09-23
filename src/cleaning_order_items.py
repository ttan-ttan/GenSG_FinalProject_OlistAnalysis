"""
Cleaning logic for the Olist Order Items dataset.

Purpose:
    Prepare raw order-item data for validation and the Silver layer.

Cleaning handled here:
    1. Convert numeric fields to the correct data types.
    2. Convert shipping_limit_date to timestamp format.
    3. Trim extra spaces from identifier fields.

Important:
    Business-rule issues such as missing IDs, invalid prices, or negative
    freight values are handled during validation rather than silently fixed.
"""

from pyspark.sql import functions as F


def clean_order_items(df):
    """Parameters:
        df: PySpark DataFrame containing raw order-item records.

    Returns:
        PySpark DataFrame with standardised data types and identifiers.
    """

    # Scenario 1:
    # order_item_id may be read as text from the raw CSV.
    # Convert it to integer so it can be validated and analysed correctly.
    df = df.withColumn("order_item_id", F.col("order_item_id").cast("int"))

    # Convert the shipping deadline to timestamp format
    # for reliable date and time comparisons.
    df = df.withColumn(
        "shipping_limit_date", F.to_timestamp(F.col("shipping_limit_date"))
    )

    # Scenario 2:
    # Price and freight values may be stored as text in the raw source.
    # Convert them to numeric values for calculations and validation.
    df = df.withColumn("price", F.col("price").cast("double"))

    df = df.withColumn("freight_value", F.col("freight_value").cast("double"))

    # Remove accidental leading or trailing spaces from identifier fields
    # so matching and joins are more reliable.
    id_columns = ["order_id", "product_id", "seller_id"]

    for column in id_columns:
        df = df.withColumn(column, F.trim(F.col(column)))

    return df
