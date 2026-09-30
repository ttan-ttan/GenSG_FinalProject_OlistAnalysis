"""
Silver-layer cleaning for the products dataset

Casts types, fixes source column typos, normalises text and attaches the
English category name from product_category_name_translation.

Rules:
    - Missing category -> stays NULL in both languages.
    - Categories with no translation (pc_gamer,
      portateis_cozinha_e_preparadores_de_alimentos) -> English NULL.
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

TRANSLATION_COLUMNS = ["product_category_name", "product_category_name_english"]

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
    "product_category_name_english",
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
    """Clean header names; the translation CSV header carries a UTF-8 BOM."""
    return df.toDF(*[c.replace("﻿", "").strip() for c in df.columns])


def _check_required_columns(df: DataFrame, required: list, name: str) -> None:
    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"{name}: missing required column(s): {sorted(missing)}")


def _clean_str(col_name: str) -> Column:
    """Trim; empty string -> null."""
    trimmed = F.trim(F.col(col_name).cast(StringType()))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


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
        "product_category_name", F.lower(_clean_str("product_category_name"))
    )


def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Exact duplicates only (e.g. bronze re-appended)."""
    return df.dropDuplicates()


# Translation
def clean_translation(translation: DataFrame) -> DataFrame:
    """Normalise the translation table: trim, lowercase, drop nulls, one row per category."""
    translation = _strip_bom_and_whitespace(translation)
    _check_required_columns(translation, TRANSLATION_COLUMNS, "translation")

    return (
        translation.select(*TRANSLATION_COLUMNS)
        .withColumn(
            "product_category_name", F.lower(_clean_str("product_category_name"))
        )
        .withColumn(
            "product_category_name_english",
            F.lower(_clean_str("product_category_name_english")),
        )
        .dropna(subset=TRANSLATION_COLUMNS)
        .dropDuplicates(["product_category_name"])
    )


def add_english_category(df: DataFrame, translation: DataFrame) -> DataFrame:
    """Left join. No match -> English NULL."""
    return df.join(F.broadcast(translation), "product_category_name", "left")


# Entry point
def clean_products(products: DataFrame, translation: DataFrame) -> DataFrame:
    """Bronze products + translation -> Silver products."""
    products = _strip_bom_and_whitespace(products)
    _check_required_columns(products, SOURCE_COLUMNS, "products")

    translation_clean = clean_translation(translation)

    return (
        products.select(*SOURCE_COLUMNS)
        .transform(rename_columns)
        .transform(cast_types)
        .transform(standardise_text)
        .transform(drop_exact_duplicates)
        .transform(lambda d: add_english_category(d, translation_clean))
        .select(*OUTPUT_COLUMNS)
    )
