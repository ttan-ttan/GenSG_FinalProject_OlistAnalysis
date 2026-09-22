import os
import sys

import pytest
from pyspark.sql.types import StringType, StructField, StructType

# so tests can import from src/ regardless of where pytest is run from
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "src")))

from src.cleaning_order_payments import REQUIRED_COLUMNS, clean_order_payments

# Raw input is all-strings, matching how the CSV is actually read.
RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
])

# fixed 32-char fake IDs, reused across tests
ID_A = "a" * 32
ID_B = "b" * 32
ID_C = "c" * 32


def make_raw_df(spark, rows=None):
    # default fixture covering the main cleaning cases in one go
    if rows is None:
        rows = [
            (ID_A.upper(), "1", "CREDIT_CARD", "1", "10.0"),   # uppercase -> normalized
            (ID_A, "1", "credit_card", "1", "10.0"),           # exact dup after normalizing
            (ID_B, "1", " boleto ", "3", "50.5"),              # padded whitespace
            (ID_C, "2", "voucher", "0", "0.0"),                # zero installments/value
            (None, "1", "debit_card", "2", "20.0"),            # null key -> dropped
        ]
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def test_missing_required_column_raises(spark):
    # df is missing payment_sequential/installments/value entirely
    df = spark.createDataFrame([(ID_A, "boleto")], ["order_id", "payment_type"])
    with pytest.raises(ValueError):
        clean_order_payments(df)


def test_lowercases_and_strips_strings(spark):
    # every row's order_id/payment_type should already be clean after processing
    result = clean_order_payments(make_raw_df(spark)).collect()
    assert all(r["order_id"] == r["order_id"].lower() for r in result)
    assert all(r["payment_type"] == r["payment_type"].strip().lower() for r in result)


def test_drops_rows_with_null_required_fields(spark):
    # the row with order_id=None should be removed
    raw = make_raw_df(spark)
    result = clean_order_payments(raw)
    assert result.filter(result.order_id.isNull()).count() == 0
    assert result.count() < raw.count()  # fewer rows than input confirms something was dropped


def test_drops_exact_duplicate_rows(spark):
    # ID_A appears twice (once uppercase) but normalizes to the same row -> collapses to 1
    result = clean_order_payments(make_raw_df(spark))
    assert result.filter(result.order_id == ID_A).count() == 1


def test_drops_duplicate_order_id_sequential_pairs(spark):
    # same (order_id, payment_sequential) key, different payload -> only first kept
    rows = [
        (ID_A, "1", "credit_card", "1", "10.0"),
        (ID_A, "1", "boleto", "2", "99.0"),  # same key, different payload
    ]
    result = clean_order_payments(make_raw_df(spark, rows)).collect()
    assert len(result) == 1
    assert result[0]["payment_type"] == "credit_card"  # first occurrence wins


def test_casts_dtypes_correctly(spark):
    # confirm final schema types match CLEAN_SCHEMA expectations
    result = clean_order_payments(make_raw_df(spark))
    dtypes = dict(result.dtypes)
    assert dtypes["order_id"] == "string"
    assert dtypes["payment_sequential"] == "int"
    assert dtypes["payment_type"] == "string"
    assert dtypes["payment_installments"] == "int"
    assert dtypes["payment_value"] == "double"


def test_input_dataframe_is_unchanged(spark):
    # cleaning should never mutate the original df (Spark DFs are immutable, but verify anyway)
    raw = make_raw_df(spark)
    before_count = raw.count()
    before_cols = list(raw.columns)
    clean_order_payments(raw)
    assert raw.count() == before_count
    assert list(raw.columns) == before_cols


def test_column_name_standardization(spark):
    # headers uppercased on input should still be lowercased/standardized on output
    raw = make_raw_df(spark)
    raw = raw.toDF(*[c.upper() for c in raw.columns])
    result = clean_order_payments(raw)
    assert result.columns == REQUIRED_COLUMNS


def test_non_numeric_value_is_dropped_not_crashed(spark):
    # bad numeric string should become null on cast, then get dropped -- not raise an error
    rows = [
        (ID_A, "1", "credit_card", "1", "10.0"),
        (ID_B, "1", "boleto", "2", "not_a_number"),  # uncastable -> null -> dropped
    ]
    result = clean_order_payments(make_raw_df(spark, rows))
    assert result.count() == 1
    assert result.collect()[0]["order_id"] == ID_A


def test_output_column_order_is_stable(spark):
    # regardless of input column order, output should always match REQUIRED_COLUMNS order
    result = clean_order_payments(make_raw_df(spark))
    assert result.columns == REQUIRED_COLUMNS