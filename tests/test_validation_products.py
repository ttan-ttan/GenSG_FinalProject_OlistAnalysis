"""
Test Suite: Validation Logic for Products Dataset

Builds Silver-shaped rows directly so each rule is tested in isolation.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.cleaning_products import OUTPUT_COLUMNS
from src.validation_products import validate_products

PID_1 = "1e9e8ef04dbcff4541ed26657ea517e5"
PID_2 = "3aa071139cb16b67ca9e5dea641aaa2f"

SILVER_SCHEMA = StructType(
    [
        StructField(c, StringType() if i < 2 else IntegerType(), True)
        for i, c in enumerate(OUTPUT_COLUMNS)
    ]
)


def _row(**overrides):
    base = {
        "product_id": PID_1,
        "product_category_name": "perfumaria",
        "product_name_length": 40,
        "product_description_length": 287,
        "product_photos_qty": 1,
        "product_weight_g": 225,
        "product_length_cm": 16,
        "product_height_cm": 10,
        "product_width_cm": 14,
    }
    base.update(overrides)
    return tuple(base[c] for c in OUTPUT_COLUMNS)


def _df(spark, rows):
    return spark.createDataFrame(rows, SILVER_SCHEMA)


# Valid cases
def test_valid_rows_pass(spark):
    df = _df(spark, [_row(), _row(product_id=PID_2, product_weight_g=0)])
    assert validate_products(df).count() == 2


def test_returns_df_unchanged(spark):
    df = _df(spark, [_row()])
    out = validate_products(df)
    assert out.columns == OUTPUT_COLUMNS
    assert out.collect() == df.collect()


def test_multi_word_category_passes(spark):
    df = _df(spark, [_row(product_category_name="cama mesa banho")])
    assert validate_products(df).count() == 1


def test_missing_category_row_passes(spark):
    """category + text metadata missing -> all NULL."""
    df = _df(
        spark,
        [
            _row(
                product_category_name=None,
                product_name_length=None,
                product_description_length=None,
                product_photos_qty=None,
            )
        ],
    )
    assert validate_products(df).count() == 1


def test_untranslated_category_passes_in_silver(spark):
    """Translation is checked in Gold, not here."""
    df = _df(spark, [_row(product_category_name="pc gamer")])
    assert validate_products(df).count() == 1


def test_missing_dims_row_passes(spark):
    """all dimensions + weight missing."""
    df = _df(
        spark,
        [
            _row(
                product_weight_g=None,
                product_length_cm=None,
                product_height_cm=None,
                product_width_cm=None,
            )
        ],
    )
    assert validate_products(df).count() == 1


def test_empty_df_passes(spark):
    assert validate_products(_df(spark, [])).count() == 0


def test_extra_column_allowed(spark):
    df = _df(spark, [_row()]).withColumn("extra", F.lit(1))
    assert validate_products(df).count() == 1


# Schema
def test_missing_column_raises(spark):
    df = _df(spark, [_row()]).drop("product_category_name")
    with pytest.raises(ValueError, match="missing column"):
        validate_products(df)


# Row rules
@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"product_id": None}, "product_id is null"),
        ({"product_id": "abc"}, "product_id not 32-char lowercase hex"),
        ({"product_id": PID_1.upper()}, "product_id not 32-char lowercase hex"),
        ({"product_id": f" {PID_1}"}, "product_id not 32-char lowercase hex"),
        (
            {"product_category_name": "Perfumaria"},
            "product_category_name not normalised",
        ),
        (
            {"product_category_name": " perfumaria"},
            "product_category_name not normalised",
        ),
        (
            {"product_category_name": "cama_mesa_banho"},
            "product_category_name not normalised",
        ),
        ({"product_category_name": ""}, "product_category_name not normalised"),
        ({"product_weight_g": -1}, "product_weight_g negative"),
        ({"product_length_cm": 0}, "product_length_cm not positive"),
        ({"product_height_cm": -5}, "product_height_cm not positive"),
        ({"product_width_cm": 0}, "product_width_cm not positive"),
        ({"product_photos_qty": 0}, "product_photos_qty < 1"),
        ({"product_name_length": 0}, "product_name_length not positive"),
        ({"product_description_length": 0}, "product_description_length not positive"),
    ],
)
def test_rule_violation_raises(spark, overrides, expected):
    df = _df(spark, [_row(**overrides)])
    with pytest.raises(ValueError, match=expected):
        validate_products(df)


def test_duplicate_product_id_raises(spark):
    df = _df(spark, [_row(), _row(product_weight_g=999)])
    with pytest.raises(ValueError, match=r"duplicate product_id \(1 rows\)"):
        validate_products(df)


def test_multiple_null_ids_not_counted_as_duplicates(spark):
    df = _df(spark, [_row(product_id=None), _row(product_id=None)])
    with pytest.raises(ValueError) as exc:
        validate_products(df)
    assert "product_id is null (2 rows)" in str(exc.value)
    assert "duplicate product_id" not in str(exc.value)


def test_all_failures_reported_together(spark):
    df = _df(spark, [_row(product_weight_g=-1, product_photos_qty=0)])
    with pytest.raises(ValueError) as exc:
        validate_products(df)
    assert "product_weight_g negative" in str(exc.value)
    assert "product_photos_qty < 1" in str(exc.value)


def test_failure_counts_rows(spark):
    df = _df(
        spark, [_row(product_weight_g=-1), _row(product_id=PID_2, product_weight_g=-2)]
    )
    with pytest.raises(ValueError, match=r"product_weight_g negative \(2 rows\)"):
        validate_products(df)
