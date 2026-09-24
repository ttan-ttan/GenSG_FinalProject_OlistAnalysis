"""
Test Suite: Validation Logic for Products Dataset

Builds Silver-shaped rows directly (no cleaning) so each rule is tested in isolation.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.cleaning_products import OUTPUT_COLUMNS
from src.validation_products import validate_products

PID_1 = "1e9e8ef04dbcff4541ed26657ea517e5"
PID_2 = "3aa071139cb16b67ca9e5dea641aaa2f"

SILVER_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
    StructField("product_name_length", IntegerType(), True),
    StructField("product_description_length", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),
    StructField("product_volume_cm3", LongType(), True),
    StructField("is_category_missing", BooleanType(), True),
    StructField("is_dims_missing", BooleanType(), True),
])


def _row(**overrides):
    base = {
        "product_id": PID_1,
        "product_category_name": "perfumaria",
        "product_category_name_english": "perfumery",
        "product_name_length": 40,
        "product_description_length": 287,
        "product_photos_qty": 1,
        "product_weight_g": 225,
        "product_length_cm": 16,
        "product_height_cm": 10,
        "product_width_cm": 14,
        "product_volume_cm3": 2240,
        "is_category_missing": False,
        "is_dims_missing": False,
    }
    base.update(overrides)
    return tuple(base[f.name] for f in SILVER_SCHEMA.fields)


def _df(spark, rows):
    return (spark.createDataFrame(rows, SILVER_SCHEMA)
            .withColumn("_silver_processed_at", F.current_timestamp())
            .select(*OUTPUT_COLUMNS))


def test_valid_rows_pass(spark):
    df = _df(spark, [
        _row(),
        _row(product_id=PID_2, product_weight_g=0),  # zero weight OK
    ])
    assert validate_products(df).count() == 2


def test_missing_category_row_passes(spark):
    """The 610-row source pattern: category + text metadata missing."""
    df = _df(spark, [_row(product_category_name="unknown",
                          product_category_name_english="unknown",
                          is_category_missing=True,
                          product_name_length=None,
                          product_description_length=None,
                          product_photos_qty=None)])
    assert validate_products(df).count() == 1


@pytest.mark.parametrize("category", ["pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos"])
def test_known_untranslated_row_passes(spark, category):
    df = _df(spark, [_row(product_category_name=category,
                          product_category_name_english="unknown")])
    assert validate_products(df).count() == 1


def test_missing_dims_row_passes(spark):
    df = _df(spark, [_row(product_height_cm=None, product_volume_cm3=None,
                          is_dims_missing=True)])
    assert validate_products(df).count() == 1


def test_empty_df_passes(spark):
    assert validate_products(_df(spark, [])).count() == 0


def test_missing_column_raises(spark):
    df = _df(spark, [_row()]).drop("is_dims_missing")
    with pytest.raises(ValueError, match="missing column"):
        validate_products(df)


@pytest.mark.parametrize("overrides, expected", [
    ({"product_id": None}, "product_id is null"),
    ({"product_id": "abc"}, "product_id not 32-char lowercase hex"),
    ({"product_id": PID_1.upper()}, "product_id not 32-char lowercase hex"),
    ({"product_category_name_english": None}, "product_category_name_english is null"),
    ({"product_category_name_english": "unknown"}, "new untranslated category"),
    ({"is_category_missing": True}, "is_category_missing inconsistent"),
    ({"is_dims_missing": True}, "is_dims_missing inconsistent"),
    ({"product_width_cm": None, "product_volume_cm3": None},
     "is_dims_missing inconsistent"),
    ({"product_weight_g": -1}, "product_weight_g negative"),
    ({"product_length_cm": 0, "product_volume_cm3": 0}, "product_length_cm not positive"),
    ({"product_height_cm": -5, "product_volume_cm3": -1120},
     "product_height_cm not positive"),
    ({"product_width_cm": 0, "product_volume_cm3": 0}, "product_width_cm not positive"),
    ({"product_volume_cm3": 1}, "product_volume_cm3 mismatch"),
    ({"product_photos_qty": 0}, "product_photos_qty < 1"),
    ({"product_name_length": 0}, "product_name_length not positive"),
    ({"product_description_length": 0}, "product_description_length not positive"),
])
def test_rule_violation_raises(spark, overrides, expected):
    df = _df(spark, [_row(**overrides)])
    with pytest.raises(ValueError, match=expected):
        validate_products(df)


def test_duplicate_product_id_raises(spark):
    df = _df(spark, [_row(), _row(product_weight_g=999)])
    with pytest.raises(ValueError, match="duplicate product_id"):
        validate_products(df)


def test_all_failures_reported_together(spark):
    df = _df(spark, [_row(product_weight_g=-1, product_photos_qty=0)])
    with pytest.raises(ValueError) as exc:
        validate_products(df)
    assert "product_weight_g negative" in str(exc.value)
    assert "product_photos_qty < 1" in str(exc.value)

