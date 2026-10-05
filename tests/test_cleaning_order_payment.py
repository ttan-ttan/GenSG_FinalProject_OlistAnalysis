""" test_cleaning_order_payment  """

import os
import sys
import pytest
from pyspark.sql.types import StringType, StructField, StructType
from src.validation_payments import validate_order_payments
from src.cleaning_order_payments import (
    REQUIRED_COLUMNS,
    aggregate_order_payment_totals,
    clean_order_payments,
)

# so tests can import from src/ regardless of where pytest is run from
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


# Raw input is all-strings, matching how the CSV is actually read.
RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", StringType(), True),
        StructField("payment_value", StringType(), True),
    ]
)

# fixed 32-char fake IDs, reused across tests
ID_A = "a" * 32
ID_B = "b" * 32
ID_C = "c" * 32


def make_raw_df(spark, rows=None):
    """Helper to create a raw input DataFrame for testing. If rows is None,
    uses a default fixture covering the main cleaning cases in one go."""
    # default fixture covering the main cleaning cases in one go
    if rows is None:
        rows = [
            # uppercase -> normalized
            (ID_A.upper(), "1", "CREDIT_CARD", "1", "10.0"),
            # exact dup after normalizing
            (ID_A, "1", "credit_card", "1", "10.0"),
            (ID_B, "1", " boleto ", "3", "50.5"),  # padded whitespace
            (ID_C, "2", "voucher", "0", "0.0"),  # zero installments/value
            (None, "1", "debit_card", "2", "20.0"),  # null key -> dropped
        ]
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def test_missing_required_column_raises(spark):
    """Test that clean_order_payments raises ValueError if any required column is missing."""
    # df is missing payment_sequential/installments/value entirely
    df = spark.createDataFrame([(ID_A, "boleto")], ["order_id", "payment_type"])
    with pytest.raises(ValueError):
        clean_order_payments(df)


def test_lowercases_and_strips_strings(spark):
    """Test that clean_order_payments lowercases and strips string columns."""
    # every row's order_id/payment_type should already be clean after processing
    result = clean_order_payments(make_raw_df(spark)).collect()
    assert all(r["order_id"] == r["order_id"].lower() for r in result)
    assert all(r["payment_type"] == r["payment_type"].strip().lower() for r in result)


def test_drops_rows_with_null_required_fields(spark):
    """Test that clean_order_payments drops rows with null required fields."""
    # the row with order_id=None should be removed
    raw = make_raw_df(spark)
    result = clean_order_payments(raw)
    assert result.filter(result.order_id.isNull()).count() == 0
    assert (
        result.count() < raw.count()
    )  # fewer rows than input confirms something was dropped


def test_drops_exact_duplicate_rows(spark):
    """Test that clean_order_payments drops exact duplicate rows."""
    # ID_A appears twice (once uppercase) but normalizes to the same row -> collapses to 1
    result = clean_order_payments(make_raw_df(spark))
    assert result.filter(result.order_id == ID_A).count() == 1


def test_drops_duplicate_order_id_sequential_pairs(spark):
    """Test that clean_order_payments drops duplicate (order_id, payment_sequential) pairs."""
    # same (order_id, payment_sequential) key, different payload -> only first kept
    rows = [
        (ID_A, "1", "credit_card", "1", "10.0"),
        (ID_A, "1", "boleto", "2", "99.0"),  # same key, different payload
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert len(result) == 1
    assert result[0]["payment_type"] == "credit_card"  # first occurrence wins


def test_credit_card_zero_installments_fixed_and_negative_dropped(spark):
    """Test that clean_order_payments fixes credit_card zero installments to 1,
    and drops negative installments."""
    rows = [
        (ID_A, "1", "credit_card", "0", "10.0"),
        (ID_B, "1", "boleto", "-1", "20.0"),
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert len(result) == 1
    assert result[0]["payment_installments"] == 1


def test_drops_negative_payment_value(spark):
    """Test that clean_order_payments drops rows with negative payment values."""
    # negative payments are invalid, validation would flag them, so cleaning drops them
    rows = [
        (ID_A, "1", "credit_card", "1", "-5.0"),
        (ID_B, "1", "boleto", "1", "20.0"),
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert [r["order_id"] for r in result] == [ID_B]


def test_drops_payment_sequential_below_one(spark):
    """Test that clean_order_payments drops rows with payment_sequential below 1."""
    rows = [
        (ID_A, "0", "credit_card", "1", "10.0"),
        (ID_B, "1", "boleto", "1", "20.0"),
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert [r["order_id"] for r in result] == [ID_B]


def test_drops_order_id_with_wrong_length(spark):
    """Test that clean_order_payments drops rows with order_id of wrong length."""
    rows = [
        ("too_short", "1", "credit_card", "1", "10.0"),
        (ID_B, "1", "boleto", "1", "20.0"),
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert [r["order_id"] for r in result] == [ID_B]


def test_drops_unknown_payment_type(spark):
    """Test that clean_order_payments drops rows with unknown payment_type."""
    rows = [
        (ID_A, "1", "bitcoin", "1", "10.0"),
        (ID_B, "1", "boleto", "1", "20.0"),
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert [r["order_id"] for r in result] == [ID_B]


def test_not_defined_kept_by_default_and_droppable(spark):
    """Test that clean_order_payments keeps 'not_defined' payment_type
    by default, but can drop it if requested."""
    rows = [
        (ID_A, "1", "not_defined", "1", "10.0"),
        (ID_B, "1", "boleto", "1", "20.0"),
    ]
    assert clean_order_payments(make_raw_df(spark, rows)).count() == 2
    dropped = clean_order_payments(make_raw_df(spark, rows), drop_not_defined=True)
    assert [r["order_id"] for r in dropped.collect()] == [ID_B]


def test_split_payments_are_kept_and_order_total_is_aggregated(spark):
    """Test that clean_order_payments keeps split payments and aggregates the total per order."""
    rows = [
        (ID_A, "1", "credit_card", "2", "10.0"),
        (ID_A, "2", "voucher", "1", "5.5"),
        (ID_B, "1", "boleto", "1", "20.0"),
    ]
    cleaned = clean_order_payments(make_raw_df(spark, rows))
    totals = {
        row["order_id"]: (
            row["payment_record_count"],
            row["order_payment_total"],
        )
        for row in aggregate_order_payment_totals(cleaned).collect()
    }
    assert cleaned.filter(cleaned.order_id == ID_A).count() == 2
    assert totals[ID_A] == (2, 15.5)
    assert totals[ID_B] == (1, 20.0)


def test_casts_dtypes_correctly(spark):
    """Test that clean_order_payments casts columns to the correct types."""
    # confirm final schema types match CLEAN_SCHEMA expectations
    result = clean_order_payments(make_raw_df(spark))
    dtypes = dict(result.dtypes)
    assert dtypes["order_id"] == "string"
    assert dtypes["payment_sequential"] == "int"
    assert dtypes["payment_type"] == "string"
    assert dtypes["payment_installments"] == "int"
    assert dtypes["payment_value"] == "double"


def test_input_dataframe_is_unchanged(spark):
    """Test that clean_order_payments does not mutate the input DataFrame."""
    # cleaning should never mutate the original df (Spark DFs are immutable, but verify anyway)
    raw = make_raw_df(spark)
    before_count = raw.count()
    before_cols = list(raw.columns)
    clean_order_payments(raw)
    assert raw.count() == before_count
    assert list(raw.columns) == before_cols


def test_column_name_standardization(spark):
    """Test that clean_order_payments standardizes column names to lowercase."""
    # headers uppercased on input should still be lowercased/standardized on output
    raw = make_raw_df(spark)
    raw = raw.toDF(*[c.upper() for c in raw.columns])
    result = clean_order_payments(raw)
    assert result.columns == REQUIRED_COLUMNS


def test_non_numeric_value_is_dropped_not_crashed(spark):
    """Test that clean_order_payments drops rows with non-numeric
    payment_value instead of raising an error."""
    # bad numeric string should become null on cast, then get dropped -- not raise an error
    rows = [
        (ID_A, "1", "credit_card", "1", "10.0"),
        # uncastable -> null -> dropped
        (ID_B, "1", "boleto", "2", "not_a_number"),
    ]
    result = clean_order_payments(make_raw_df(spark, rows))
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == ID_A


def test_output_column_order_is_stable(spark):
    """Test that clean_order_payments always outputs columns in the REQUIRED_COLUMNS order."""
    # regardless of input column order, output should always match REQUIRED_COLUMNS order
    result = clean_order_payments(make_raw_df(spark))
    assert result.columns == REQUIRED_COLUMNS


def test_cleaned_output_passes_validation(spark):
    """Test that the cleaned output passes validation with no errors."""
    # the most important alignment test: messy data goes in, and whatever comes
    # out of cleaning must have ZERO validation errors
    rows = [
        (ID_A.upper(), "1", " CREDIT_CARD ", "0", "10.0"),  # messy but fixable
        (ID_A, "1", "credit_card", "1", "10.0"),  # duplicate
        (ID_B, "1", "boleto", "1", "-5.0"),  # negative value
        (ID_C, "0", "voucher", "1", "5.0"),  # bad sequence
        ("short", "1", "boleto", "1", "5.0"),  # bad order_id length
        (ID_C, "1", "bitcoin", "1", "5.0"),  # unknown type
        (ID_C, "2", "voucher", "1", "abc"),  # invalid number
        (ID_B, "2", "not_defined", "1", "7.0"),  # allowed by default
    ]
    cleaned = clean_order_payments(make_raw_df(spark, rows))
    report = validate_order_payments(cleaned, strict=False)
    assert report["errors"] == []
    assert report["passed"] is True
