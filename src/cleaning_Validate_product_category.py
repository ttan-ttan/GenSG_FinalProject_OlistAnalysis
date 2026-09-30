"""
VS Code Module: cleaning_Validate_product_category.py
Handles sanitization, manual missing translations, deduplication, and validation for product categories.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_category_translation(df: DataFrame) -> DataFrame:
    """Sanitizes text, applies manual translation patches, and drops duplicates."""

    # 1. Standardize string formatting
    cleaned_df = df \
        .dropna(subset=["product_category_name"]) \
        .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name")))) \
        .withColumn("product_category_name", F.regexp_replace(F.col("product_category_name"), r"[\_\-]+", " ")) \
        .withColumn("product_category_name_english", F.initcap(F.trim(F.col("product_category_name_english")))) \
        .dropDuplicates(["product_category_name"])

    # 2. Patch known missing categories in Olist dataset (pc_gamer & portateis_cozinha_e_preparadores_de_alimentos)
    missing_categories_data = [
        ("pc gamer", "PC Gamer"),
        ("portateis cozinha e preparadores de alimentos",
         "Food Processors And Portable Kitchen Appliances")
    ]
    patch_df = df.sparkSession.createDataFrame(
        missing_categories_data,
        ["product_category_name", "product_category_name_english"]
    )

    # 3. Combine and remove any potential duplicates introduced by patching
    final_df = cleaned_df.union(patch_df).dropDuplicates([
        "product_category_name"])

    return final_df


def validate_category_translation(df: DataFrame) -> None:
    """Data quality assertions prior to Silver layer write."""
    row_count = df.count()
    assert row_count > 0, "Validation Error: Output resulted in 0 rows!"

    null_count = df.filter(F.col("product_category_name").isNull()).count()
    assert null_count == 0, f"Validation Error: Found {null_count} NULL category keys!"

    print(f"Validation successful: {row_count} rows ready for Silver layer.")
