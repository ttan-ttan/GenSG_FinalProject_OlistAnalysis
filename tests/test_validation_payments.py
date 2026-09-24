import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.validation_payments import ValidationError, validate_order_payments

# tests feed already-clean/typed data, since validate_order_payments expects that
CLEAN_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])

# fixed 32-char fake IDs, reused across tests
ID_A = "a" * 32
ID_B = "b" * 32
ID_C = "c" * 32


def make_clean_df(spark, rows=None):
    # default fixture: all valid rows, including one legit zero-value voucher
    if rows is None:
        rows = [
            (ID_A, 1, "credit_card", 3, 99.9),
            (ID_B, 1, "boleto", 1, 50.0),
            (ID_C, 2, "voucher", 0, 0.0),  # legitimate zero-value voucher
        ]
    return spark.createDataFrame(rows, schema=CLEAN_SCHEMA)


def test_valid_dataframe_passes(spark):
    # fully clean data -> no errors, correct row count
    report = validate_order_payments(make_clean_df(spark), strict=False)
    assert report["passed"] is True
    assert report["errors"] == []
    assert report["row_count"] == 3


def test_missing_column_raises_in_strict_mode(spark):
    # strict=True should raise instead of just returning a failed report
    df = make_clean_df(spark).drop("payment_value")
    with pytest.raises(ValidationError):
        validate_order_payments(df, strict=True)


def test_missing_column_reports_without_raising_when_not_strict(spark):
    # strict=False should return a failed report instead of raising
    df = make_clean_df(spark).drop("payment_value")
    report = validate_order_payments(df, strict=False)
    assert report["passed"] is False
    assert any("payment_value" in e for e in report["errors"])


def test_null_in_required_column_flagged(spark):
    # null order_id should surface as a null-value error
    rows = [(ID_A, 1, "credit_card", 3, 99.9), (None, 1, "boleto", 1, 50.0)]
    report = validate_order_payments(make_clean_df(spark, rows), strict=False)
    assert report["passed"] is False
    assert any("null value" in e for e in report["errors"])


def test_negative_payment_value_flagged(spark):
    # payment_value < 0 should be a critical error
    rows = [(ID_A, 1, "credit_card", 3, -5.0)]
    report = validate_order_payments(make_clean_df(spark, rows), strict=False)
    assert report["passed"] is False
    assert any("negative payment_value" in e for e in report["errors"])


def test_invalid_order_id_length_flagged(spark):
    # order_id not exactly 32 chars should be flagged
    rows = [("too_short", 1, "credit_card", 3, 99.9)]
    report = validate_order_payments(make_clean_df(spark, rows), strict=False)
    assert report["passed"] is False
    assert any("order_id" in e for e in report["errors"])


def test_invalid_payment_sequential_flagged(spark):
    # payment_sequential must be >= 1
    rows = [(ID_A, 0, "credit_card", 3, 99.9)]
    report = validate_order_payments(make_clean_df(spark, rows), strict=False)
    assert report["passed"] is False
    assert any("payment_sequential" in e for e in report["errors"])


def test_invalid_payment_type_flagged(spark):
    # payment_type outside VALID_PAYMENT_TYPES should be flagged, and named in the message
    rows = [(ID_A, 1, "bitcoin", 3, 99.9)]
    report = validate_order_payments(make_clean_df(spark, rows), strict=False)
    assert report["passed"] is False
    assert any("bitcoin" in e for e in report["errors"])


def test_duplicate_order_id_sequential_flagged(spark):
    # two rows sharing the same (order_id, payment_sequential) key -> duplicate error
    rows = [
        (ID_A, 1, "credit_card", 3, 99.9),
        (ID_A, 1, "boleto", 1, 50.0),
    ]
    report = validate_order_payments(make_clean_df(spark, rows), strict=False)
    assert report["passed"] is False
    assert any("duplicate" in e for e in report["errors"])


def test_zero_value_is_warning_not_error(spark):
    # zero payment_value / installments should warn, not fail validation
    report = validate_order_payments(make_clean_df(spark), strict=False)
    assert report["passed"] is True
    assert any("payment_value == 0" in w for w in report["warnings"])
    assert any("payment_installments == 0" in w for w in report["warnings"])


def test_strict_mode_raises_with_error_details(spark):
    # raised exception message should include the specific failure reason
    rows = [(ID_A, 1, "credit_card", 3, -1.0)]
    with pytest.raises(ValidationError, match="negative payment_value"):
        validate_order_payments(make_clean_df(spark, rows), strict=True)


def test_empty_dataframe_passes_with_zero_rows(spark):
    # zero rows shouldn't crash the aggregations (coalesce handles null sums) or fail validation
    df = make_clean_df(spark, [])
    report = validate_order_payments(df, strict=False)
    assert report["row_count"] == 0
    assert report["passed"] is True