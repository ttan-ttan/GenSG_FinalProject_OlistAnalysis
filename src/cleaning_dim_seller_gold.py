# pylint: disable=no-member

"""
cleaning_dim_seller_gold
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
from pyspark.sql import Window

from validation_dim_seller_gold import validate_dim_seller_gold


def clean_dim_seller_gold(df: DataFrame) -> DataFrame:
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
          .filter(F.col("seller_zip_code_prefix").between(1000, 99999))
          .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
          .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    )

    return (
        df_clean.withColumn("seller_key", F.row_number().over(
            Window.orderBy("seller_id")))
        .select("seller_key", "seller_id", "seller_state", "seller_city")
    )


def run_clean(spark):
    """Run Gold seller dimension build."""
    silver_df = spark.read.table("silver_sellers")
    gold_df = clean_dim_seller_gold(silver_df)
    gold_df = validate_dim_seller_gold(gold_df)

    gold_df.write.format("delta").mode(
        "overwrite").saveAsTable("gold_dim_seller")
    print("Gold dimension table 'gold_dim_seller' created successfully.")
    return gold_df


clean_sellers_gold = clean_dim_seller_gold
