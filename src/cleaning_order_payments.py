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

# Read the raw CSV as all-strings so malformed values become nulls during our
# own explicit cast step, instead of Spark silently nulling or failing the read.
RAW_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
])

CLEAN_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
])


def _standardize_columns(df: DataFrame) -> DataFrame:
    """Strip and lowercase column names."""
    return df.toDF(*[c.strip().lower() for c in df.columns])


def _check_required_columns(df: DataFrame) -> None:
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required column(s): {sorted(missing)}")


def clean_order_payments(df: DataFrame) -> DataFrame:
    """
    Clean the raw order_payments Spark DataFrame.

    Steps:
      1. Standardize column names (strip + lowercase headers).
      2. Verify all required columns are present.
      3. Trim whitespace and lowercase order_id / payment_type.
      4. Cast numeric columns; values that fail to cast become null.
         (Spark's cast is the equivalent of pandas errors="coerce".)
      5. Drop rows with nulls in any required column.
      6. Drop fully duplicate rows.
      7. Drop duplicate (order_id, payment_sequential) rows, deterministically
         keeping the first occurrence in source order.
      8. Return the required columns in a stable order.

    Spark DataFrames are immutable, so this returns a new DataFrame and the
    input is untouched.
    """
    df = _standardize_columns(df)
    _check_required_columns(df)

    df = df.select(*REQUIRED_COLUMNS)

    df = (
        df.withColumn("order_id", F.lower(F.trim(F.col("order_id").cast(StringType()))))
          .withColumn("payment_type", F.lower(F.trim(F.col("payment_type").cast(StringType()))))
          .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
          .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
          .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
    )

    df = df.na.drop(subset=REQUIRED_COLUMNS)

    df = df.dropDuplicates()

    # dropDuplicates(subset=...) does NOT guarantee which row survives. To get a
    # deterministic "keep the first occurrence", capture the original row order
    # and keep row_number == 1 within each key.
    ordering_col = "__row_order__"
    df = df.withColumn(ordering_col, F.monotonically_increasing_id())
    window = Window.partitionBy("order_id", "payment_sequential").orderBy(F.col(ordering_col).asc())
    df = (
        df.withColumn("__rn__", F.row_number().over(window))
          .filter(F.col("__rn__") == 1)
          .drop("__rn__", ordering_col)
    )

    return df.select(*REQUIRED_COLUMNS)


def read_raw_order_payments(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the raw order_payments CSV using the all-string schema."""
    return (
        spark.read
        .option("header", "true")
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
) -> DataFrame:
    """Read a raw CSV, clean it, and optionally write the result out."""
    raw = read_raw_order_payments(spark, input_path)
    cleaned = clean_order_payments(raw)
    if output_path:
        cleaned.write.format(output_format).mode("overwrite").save(output_path)
    return cleaned