# pylint: disable=invalid-unary-operand-type
# pylint: disable=no-member

"""
Silver-layer cleaning for the sellers dataset.
Handles type casting, normalization, and removal of invalid records.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType


def clean_sellers(df: DataFrame) -> DataFrame:
    """Normalize seller fields and remove invalid seller records."""

    df_clean = (
        df.withColumn("seller_id", F.col("seller_id").cast(StringType()))
          .withColumn("seller_zip_code_prefix",
                      F.col("seller_zip_code_prefix").cast(IntegerType()))
          .withColumn("seller_city",
                      F.lower(F.trim(F.col("seller_city").cast(StringType()))))
          .withColumn("seller_state",
                      F.upper(F.trim(F.col("seller_state").cast(StringType()))))

        # Critical non-null fields
          .filter(F.col("seller_id").isNotNull())
          .filter(F.col("seller_city").isNotNull())
          .filter(F.col("seller_state").isNotNull())

        # ZIP code must be within valid numeric range
          .filter(F.col("seller_zip_code_prefix").between(1000, 99999))
    )

    return df_clean
