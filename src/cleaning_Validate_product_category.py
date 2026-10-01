"""Cleaning and validation helpers for product-category translations."""

import re

import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def sanitize_category_name(value: object) -> str:
    """Return the normalized category key used by the translation table."""
    if value is None or not str(value).strip():
        return "unknown"
    return re.sub(r"[_-]+", " ", str(value).strip().lower())


def clean_category_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and deduplicate a Pandas category-translation DataFrame."""
    required = {"product_category_name", "product_category_name_english"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    cleaned = df.copy()
    cleaned["product_category_name"] = cleaned["product_category_name"].map(
        sanitize_category_name
    )
    cleaned["product_category_name_english"] = (
        cleaned["product_category_name_english"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.title()
    )
    return cleaned.drop_duplicates(subset=["product_category_name"]).reset_index(
        drop=True
    )


def validate_category_dataframe(df: pd.DataFrame) -> list[str]:
    """Return data-quality errors for a Pandas category-translation DataFrame."""
    errors = []
    required = {"product_category_name", "product_category_name_english"}
    missing = sorted(required - set(df.columns))
    if missing:
        return [f"Missing required columns: {missing}"]

    if df["product_category_name"].isna().any():
        errors.append("product_category_name contains null values")
    if df["product_category_name"].duplicated().any():
        errors.append("product_category_name contains duplicates")
    return errors


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
