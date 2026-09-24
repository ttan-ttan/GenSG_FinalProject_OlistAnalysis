"""
Gold‑layer cleaning for customers dimension.
Ensures business‑ready fields and removes unusable customer records.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_customers_gold(df: DataFrame) -> DataFrame:
    """
    Gold cleaning rules:
    - Ensure customer_id is present
    - Ensure first_purchase_date exists and is not in the future
    - Remove customers with no orders (if business requires)
    - Standardize city/state formatting again
    """

    df_clean = (
        df.filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_first_purchase_date").isNotNull())
        .filter(F.col("customer_first_purchase_date") <= F.current_timestamp())
        .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    )

    return df_clean
