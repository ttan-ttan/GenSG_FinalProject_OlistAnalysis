"""
cleaning_order_payments.py

PySpark cleaning module for the Olist order_payments dataset.

Importable in a Fabric notebook via:
    import sys
    sys.path.append('/lakehouse/default/Files/code/src')
    from cleaning_order_payments import clean_order_payments
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Columns needed in the final output
REQUIRED_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]

# The only payment types we accept (must match validation_payments.py)
VALID_PAYMENT_TYPES = ["boleto", "credit_card", "debit_card", "not_defined", "voucher"]

# Olist order_id is a fixed-length hex string (must match validation_payments.py)
ORDER_ID_LENGTH = 32

# Read the raw CSV as all-strings so malformed values become nulls during our
# own explicit cast step, instead of Spark silently nulling or failing the read.
RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", StringType(), True),
        StructField("payment_value", StringType(), True),
    ]
)

# The data types we expect AFTER cleaning (for reference)
CLEAN_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
    ]
)


def _standardize_columns(df: DataFrame) -> DataFrame:
    """Strip and lowercase column names."""
    return df.toDF(*[c.strip().lower() for c in df.columns])


def _check_required_columns(df: DataFrame) -> None:
    """Stop early with a clear error if a column we need is missing."""
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required column(s): {sorted(missing)}")


def clean_order_payments(df: DataFrame, drop_not_defined: bool = False) -> DataFrame:
    """
    Clean the raw order_payments Spark DataFrame.

    Steps:
      1. Standardize column names (strip + lowercase headers).
      2. Verify all required columns are present.
      3. Remember the original row order (so "keep first" really means first).
      4. Trim whitespace and lowercase order_id / payment_type.
      5. Cast numeric columns; values that fail to cast become null
         (this is how invalid data types are caught).
      6. Treat zero credit-card installments as a single payment.
      7. Drop rows with nulls in any required column.
      8. Drop rows that break the validation rules:
           - negative installments
           - payment_sequential below 1
           - negative payment_value
           - order_id that is not 32 characters
           - payment_type that is not a known type
      9. Optionally drop 'not_defined' payments (they belong to canceled orders).
     10. Keep one row per (order_id, payment_sequential): the first in source
         order. This also removes exact duplicate rows.
     11. Return the required columns in a stable order.

    Every rule that validation_payments.py treats as an ERROR is enforced here,
    so cleaned output should always pass validation.

    Spark DataFrames are immutable, so this returns a new DataFrame and the
    input is untouched.
    """
    df = _standardize_columns(df)
    _check_required_columns(df)

    df = df.select(*REQUIRED_COLUMNS)

    # Number the rows NOW, before anything can reshuffle them. We use this
    # number later to decide which duplicate is "first".
    ordering_col = "__row_order__"
    df = df.withColumn(ordering_col, F.monotonically_increasing_id())

    # Trim / lowercase the text columns and cast the number columns.
    # A value that cannot be cast (like "abc") becomes null.
    df = (
        df.withColumn("order_id", F.lower(F.trim(F.col("order_id").cast(StringType()))))
        .withColumn(
            "payment_type", F.lower(F.trim(F.col("payment_type").cast(StringType())))
        )
        .withColumn(
            "payment_sequential", F.col("payment_sequential").cast(IntegerType())
        )
        .withColumn(
            "payment_installments", F.col("payment_installments").cast(IntegerType())
        )
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
    )

    # A credit-card payment with zero installments is a one-time payment.
    df = df.withColumn(
        "payment_installments",
        F.when(
            (F.col("payment_type") == "credit_card")
            & (F.col("payment_installments") == 0),
            F.lit(1),
        ).otherwise(F.col("payment_installments")),
    )

    # Drop rows with nulls (this includes values that failed the casts above)
    df = df.na.drop(subset=REQUIRED_COLUMNS)

    # Drop rows that break the same rules validation checks
    df = df.filter(
        (F.col("payment_installments") >= 0)  # no negative installments
        & (F.col("payment_sequential") >= 1)  # sequence starts at 1
        & (F.col("payment_value") >= 0)  # no negative payments
        & (F.length(F.col("order_id")) == ORDER_ID_LENGTH)  # 32-char order_id
        & F.col("payment_type").isin(VALID_PAYMENT_TYPES)  # known payment type
    )

    # Optional: 'not_defined' payments belong to canceled orders
    if drop_not_defined:
        df = df.filter(F.col("payment_type") != "not_defined")

    # Keep ONE row per (order_id, payment_sequential): the earliest one in the
    # source file. Exact duplicate rows share the same key, so they are removed
    # here too. (dropDuplicates(subset) can't promise which row survives, so we
    # use a window + row_number instead.)
    window = Window.partitionBy("order_id", "payment_sequential").orderBy(
        F.col(ordering_col).asc()
    )
    df = (
        df.withColumn("__rn__", F.row_number().over(window))
        .filter(F.col("__rn__") == 1)
        .drop("__rn__", ordering_col)
    )

    return df.select(*REQUIRED_COLUMNS)


def aggregate_order_payment_totals(df: DataFrame) -> DataFrame:
    """Return one row per order with its payment-record count and total."""
    df = _standardize_columns(df)
    _check_required_columns(df)
    return df.groupBy("order_id").agg(
        F.count(F.lit(1)).alias("payment_record_count"),
        F.sum(F.col("payment_value").cast(DecimalType(18, 2))).alias(
            "order_payment_total"
        ),
    )


def read_raw_order_payments(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the raw order_payments CSV using the all-string schema."""
    return (
        spark.read.option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .schema(RAW_SCHEMA)
        .csv(input_path)
    )


def clean_order_payments_path(
    spark: SparkSession,
    input_path: str,
    output_path: str | None = None,
    output_format: str = "delta",
    drop_not_defined: bool = False,
) -> DataFrame:
    """Read a raw CSV, clean it, and optionally write the result out."""
    raw = read_raw_order_payments(spark, input_path)
    cleaned = clean_order_payments(raw, drop_not_defined=drop_not_defined)
    if output_path:
        cleaned.write.format(output_format).mode("overwrite").save(output_path)
    return cleaned
