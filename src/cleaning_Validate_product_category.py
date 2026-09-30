"""
VS Code Module: cleaning_Validate_product_category.py
Handles string sanitization and validation for product category translation dataset using PySpark.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_category_translation(df: DataFrame) -> DataFrame:
    """Sanitizes text, removes special characters, standardizes whitespace, and drops duplicates."""
    return df \
        .dropna(subset=["product_category_name"]) \
        .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name")))) \
        .withColumn("product_category_name", F.regexp_replace(F.col("product_category_name"), r"[\_\-]+", " ")) \
        .withColumn("product_category_name_english", F.initcap(F.trim(F.col("product_category_name_english")))) \
        .dropDuplicates(["product_category_name"])


def validate_category_translation(df: DataFrame) -> None:
    """Data quality assertions prior to Silver layer write."""
    row_count = df.count()
    assert row_count > 0, "Validation Error: silver_category_translation resulted in 0 rows!"

    null_count = df.filter(F.col("product_category_name").isNull()).count()
    assert null_count == 0, f"Validation Error: Found {null_count} NULL category keys!"

    print(f"Validation successful: {row_count} rows ready for Silver layer.")
