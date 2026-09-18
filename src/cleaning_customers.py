"""
Silver-layer cleaning for the customers dataset.
Handles type casting, normalization, and removal of invalid records.
"""

# testing deployment

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType


def clean_customers(df: DataFrame) -> DataFrame:
    """Normalize customer fields and remove invalid customer records."""

    df_clean = (
        df.withColumn("customer_id", F.col("customer_id").cast(StringType()))
          .withColumn("customer_unique_id", F.col("customer_unique_id").cast(StringType()))
          .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast(IntegerType()))
          .withColumn("customer_city", F.lower(F.trim(F.col("customer_city").cast(StringType()))))
          .withColumn("customer_state", F.upper(F.trim(F.col("customer_state").cast(StringType()))))
          .filter(F.col("customer_id").isNotNull())
          .filter(F.col("customer_unique_id").isNotNull())
          .filter(F.col("customer_city").isNotNull())
          .filter(F.col("customer_state").isNotNull())
          .filter(F.col("customer_zip_code_prefix").between(1000, 99999))
    )

    return df_clean
