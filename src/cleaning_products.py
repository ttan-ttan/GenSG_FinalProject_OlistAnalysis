"""
Silver-layer cleaning for the products dataset
Casts types, fixes source column typos and normalises text.

Rules:
    - product_category_name uses the same key format as
      clean_category_translation (cleaning_Validate_product_category):
      trim -> lowercase -> "_"/"-" runs become one space
      ("cama_mesa_banho" -> "cama mesa banho"), so the Gold join matches.
    - Missing category -> stays NULL.
    - Only exact duplicate rows are dropped. Conflicting rows sharing a
      product_id are left for validation to reject.
"""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegerType, StringType

# Source column typos -> fixed names
RENAMES = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}

SOURCE_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

INT_COLS = [
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]

OUTPUT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]


# Helpers
def _strip_bom_and_whitespace(df: DataFrame) -> DataFrame:
    """Clean header names (guards against a UTF-8 BOM / stray spaces)."""
    return df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])


def _check_required_columns(df: DataFrame, required: list, name: str) -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"{name}: missing required column(s): {sorted(missing)}")


def _clean_str(col_name: str) -> Column:
    """Trim; empty string -> null."""
    trimmed = F.trim(F.col(col_name).cast(StringType()))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def normalise_category(col_name: str) -> Column:
    """Mirror of the translation key format; blank -> NULL.

    Must stay identical to clean_category_translation, otherwise the Gold
    join silently returns NULL English categories.
    """
    trimmed = F.lower(_clean_str(col_name))
    return F.regexp_replace(trimmed, r"[_\-]+", " ")


# Products transformations
def rename_columns(df: DataFrame) -> DataFrame:
    for old, new in RENAMES.items():
        df = df.withColumnRenamed(old, new)
    return df


def cast_types(df: DataFrame) -> DataFrame:
    """Unparseable numbers become null (ANSI off, Fabric default)."""
    for c in INT_COLS:
        df = df.withColumn(c, F.col(c).cast(IntegerType()))
    return df


def standardise_text(df: DataFrame) -> DataFrame:
    return df.withColumn("product_id", _clean_str("product_id")).withColumn(
        "product_category_name", normalise_category("product_category_name")
    )


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Exact duplicates only (e.g. bronze re-appended)."""
    return df.dropDuplicates()


# Entry point
def clean_products(products: DataFrame) -> DataFrame:
    """Bronze products -> Silver products."""
    products = _strip_bom_and_whitespace(products)
    _check_required_columns(products, SOURCE_COLUMNS, "products")

    return (
        products.select(*SOURCE_COLUMNS)
        .transform(rename_columns)
        .transform(cast_types)
        .transform(standardise_text)
        .transform(drop_exact_duplicates)
        .select(*OUTPUT_COLUMNS)
    )
