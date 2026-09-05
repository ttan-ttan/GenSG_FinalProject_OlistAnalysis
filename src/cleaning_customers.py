""" 
Customer dataset cleaning module - prepare raw Bronze data for Silver layer validation. 
    Cleaning steps: 
        1. Type casting
        2. city/state normalization
        3. zip code validation
        4. null removal
        5. basic formatting cleanup
"""
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType


def clean_customers(df: DataFrame) -> DataFrame:
    """
    Clean the customers dataset loaded from Bronze (data/raw/customers.csv)
    , return as cleaned dataframe for validation. 
        Rules applied:
            - cast all columns to correct types
            - trim whitespace and normalize text fields
            - convert city names to lowercase
            - convert state codes to uppercase
            - ensure zip code prefix is a valid 5‑digit number 
            - Drop rows with null customer_id
    """
    df_clean = (
        #  cast all columns to correct types
        df.withColumn("customer_id", F.col("customer_id").cast(StringType()))
          .withColumn("customer_unique_id", F.col("customer_unique_id").cast(StringType()))
          .withColumn(
              "customer_zip_code_prefix",
              F.col("customer_zip_code_prefix").cast(IntegerType()))
          .withColumn("customer_city", F.lower(F.trim(F.col("customer_city").cast(StringType()))))
          .withColumn("customer_state", F.upper(F.trim(F.col("customer_state").cast(StringType()))))
        # normalize the city/state (precheck in SQL query all data valid)
          .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
          .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        # ensure zip code prefix is within valid range
          .filter(F.col("customer_zip_code_prefix").between(1000, 99999))
        # remove row with null customer_id
          .filter(F.col("customer_id").isNotNull())
    )
    return df_clean
