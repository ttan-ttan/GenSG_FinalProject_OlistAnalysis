# pylint: disable=no-member

"""
Gold Cleaning: Seller Dimension
Purpose:
    Apply business-level cleaning rules before Gold modeling.
Notes:
    - Standardize city/state
    - Ensure critical fields exist
    - Enforce business ZIP rules
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_sellers_gold(df: DataFrame) -> DataFrame:
    """
    Gold cleaning rules:
    - seller_id must exist
    - seller_zip_code_prefix must be valid (>= 1000)
    - seller_city lowercase
    - seller_state uppercase
    """

    df_clean = (
        df.filter(F.col("seller_id").isNotNull())
          .filter(F.col("seller_zip_code_prefix").isNotNull())
          .filter(F.col("seller_zip_code_prefix") >= 1000)
          .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
          .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    )

    return df_clean
