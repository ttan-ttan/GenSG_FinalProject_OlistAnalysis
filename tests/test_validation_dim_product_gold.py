"""
Test Suite: Gold DimProduct validation
"""

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.cleaning_dim_product_gold import OUTPUT_COLUMNS
from src.validation_dim_product_gold import (
    KNOWN_UNTRANSLATED,
    validate_dim_product_gold,
)

P1, P2 = "p1", "p2"

DIM_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_category_name_english", StringType()),
        StructField("product_weight_g", IntegerType()),
        StructField("product_length_cm", IntegerType()),
        StructField("product_height_cm", IntegerType()),
        StructField("product_width_cm", IntegerType()),
        StructField("baseline_price_med", DoubleType()),
    ]
)
SILVER_SCHEMA = "product_id string, product_category_name string"


def _dim(spark, rows):
    """rows: (product_id, english, baseline)"""
    return spark.createDataFrame(
        [(p, e, 225, 16, 10, 14, b) for p, e, b in rows], DIM_SCHEMA
    )


def _silver(spark, rows):
    """rows: (product_id, native_category)"""
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def _validate(spark, dim_rows, silver_rows):
    """Build test DataFrames and run Gold validation."""
    return validate_dim_product_gold(_dim(spark, dim_rows), _silver(spark, silver_rows))


# Valid cases
def test_valid_passes_and_returns_dim(spark):
    """Return a valid product dimension unchanged."""
    dim = _dim(spark, [(P1, "perfumery", 10.0), (P2, "bed bath table", None)])
    silver = _silver(spark, [(P1, "perfumaria"), (P2, "cama mesa banho")])
    out = validate_dim_product_gold(dim, silver)
    assert out.columns == OUTPUT_COLUMNS
    assert out.collect() == dim.collect()


def test_missing_native_category_allows_null_english(spark):
    """Allow a null English category when the native category is null."""
    assert _validate(spark, [(P1, None, 10.0)], [(P1, None)]).count() == 1


@pytest.mark.parametrize("category", sorted(KNOWN_UNTRANSLATED))
def test_known_untranslated_allows_null_english(spark, category):
    """Allow null English values for the known untranslated categories."""
    assert _validate(spark, [(P1, None, 10.0)], [(P1, category)]).count() == 1


# Failures
@pytest.mark.parametrize(
    "dim_rows, silver_rows, expected",
    [
        ([(P1, None, 10.0)], [(P1, "perfumaria")], "new untranslated category"),
        (
            [(P1, "bed_bath_table", 10.0)],
            [(P1, "cama mesa banho")],
            "product_category_name_english not normalised",
        ),
        (
            [(P1, "Perfumery", 10.0)],
            [(P1, "perfumaria")],
            "product_category_name_english not normalised",
        ),
        ([(P1, "perfumery", 0.0)], [(P1, "perfumaria")], "baseline_price_med"),
        ([(P1, "perfumery", -1.0)], [(P1, "perfumaria")], "baseline_price_med"),
        (
            [(P1, "perfumery", float("nan"))],
            [(P1, "perfumaria")],
            "baseline_price_med",
        ),
        (
            [(P1, "perfumery", float("inf"))],
            [(P1, "perfumaria")],
            "baseline_price_med",
        ),
        (
            [(P1, "perfumery", 1.0), (P1, "perfumery", 1.0)],
            [(P1, "perfumaria")],
            "duplicate product_id",
        ),
        (
            [(P1, "perfumery", 1.0)],
            [(P1, "perfumaria"), (P2, "perfumaria")],
            "row count != Silver",
        ),
        ([], [(P1, "perfumaria")], "no rows"),
    ],
)
def test_rule_violation_raises(spark, dim_rows, silver_rows, expected):
    """Raise when any parameterized product-dimension rule is violated."""
    with pytest.raises(ValueError, match=expected):
        _validate(spark, dim_rows, silver_rows)


def test_null_product_id_raises(spark):
    """Reject a null product key."""
    with pytest.raises(ValueError, match="product_id is null"):
        _validate(spark, [(None, "perfumery", 1.0)], [(P1, "perfumaria")])


def test_missing_column_raises(spark):
    """Reject dimensions missing a required Gold output column."""
    dim = _dim(spark, [(P1, "perfumery", 1.0)]).drop("baseline_price_med")
    with pytest.raises(ValueError, match="missing column"):
        validate_dim_product_gold(dim, _silver(spark, [(P1, "perfumaria")]))


def test_missing_silver_reference_column_raises(spark):
    """Reject Silver references missing a required category column."""
    dim = _dim(spark, [(P1, "perfumery", 1.0)])
    silver = _silver(spark, [(P1, "perfumaria")]).drop("product_category_name")
    with pytest.raises(ValueError, match="Silver products missing column"):
        validate_dim_product_gold(dim, silver)


def test_all_failures_reported_together(spark):
    """Report multiple product-dimension rule failures in one exception."""
    with pytest.raises(ValueError) as exc:
        _validate(spark, [(P1, None, -1.0)], [(P1, "perfumaria")])
    assert "new untranslated category" in str(exc.value)
    assert "baseline_price_med not positive" in str(exc.value)
