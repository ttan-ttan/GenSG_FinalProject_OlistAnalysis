"""
Customer Validation Module
Validates the cleaned customers dataset before moving to the Gold layer.

Validation:
    1. Ensure customer_state is a valid Brazilian state code
    2. Ensure zip code prefix is within valid range (1000–99999)
    3. Ensure customer_id is unique
    4. Ensure customer_first_purchase_date exists, is a timestamp,
       is not null, and not in the future
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType


def validate_customers(df: DataFrame) -> DataFrame:
    """
    Validate the customers dataset loaded from Silver and return a validated
    dataframe ready for Gold layer.

    Raises ValueError for:
        - missing or invalid customer_first_purchase_date
        - invalid state codes
        - invalid zip codes
        - duplicate customer_id
    """

    valid_states = [
        "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
        "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
        "RS", "RO", "RR", "SC", "SP", "SE", "TO"
    ]

    # --- 1. Validate state codes ---
    df_val = df.filter(F.col("customer_state").isin(valid_states))

    # --- 2. Validate zip code ---
    df_val = df_val.filter(
        F.col("customer_zip_code_prefix").between(1000, 99999))

    # --- 3. Validate first purchase date ---
    if "customer_first_purchase_date" not in df_val.columns:
        raise ValueError(
            "Missing required column: customer_first_purchase_date")

    if not isinstance(df_val.schema["customer_first_purchase_date"].dataType, TimestampType):
        raise ValueError("customer_first_purchase_date must be a timestamp")

    if df_val.filter(F.col("customer_first_purchase_date").isNull()).count() > 0:
        raise ValueError("Null values found in customer_first_purchase_date")

    if df_val.filter(F.col("customer_first_purchase_date") > F.current_timestamp()).count() > 0:
        raise ValueError(
            "customer_first_purchase_date contains future timestamps")

    # --- 4. Validate uniqueness of customer_id ---
    duplicates = (
        df_val.groupBy("customer_id")
              .count()
              .filter(F.col("count") > 1)
    )
    if duplicates.count() > 0:
        raise ValueError("Duplicate customer_id found in customers dataset")

    return df_val
