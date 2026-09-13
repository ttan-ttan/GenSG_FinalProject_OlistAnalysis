# pylint: disable=redefined-outer-name
# pylint: disable=no-member
# pylint: disable=invalid-unary-operand-type
# pylint: disable=missing-function-docstring

"""
Unit Tests for Customer Cleaning
Ensure the cleaning logic behaves correctly and prevents regressions
in the Silver transformation layer.

Test Coverage:
1. Type casting
2. City/state normalization
3. Zip code validation
4. Removal of invalid rows
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from src.cleaning_customers import clean_customers

spark = SparkSession.builder.getOrCreate()


def test_clean_customers_basic():
    raw = spark.createDataFrame(
        [
            ("C001", "U001", "1234", " Sao Paulo ", "sp"),
            ("C002", "U002", "99999", " Rio de Janeiro ", "rj"),
            # invalid: null customer_id
            (None, "U003", "5000", "Campinas", "sp"),
            ("C004", "U004", "0500", "Curitiba", "pr"),  # invalid zip (too short)
        ],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"]
    )

    df = clean_customers(raw)

    # Null customer_id removed
    assert df.filter(F.col("customer_id").isNull()).count() == 0

    # Zip code range 1000–99999
    assert df.filter(~F.col("customer_zip_code_prefix").between(
        1000, 99999)).count() == 0

    # City normalized to lowercase + trimmed
    row = df.filter(F.col("customer_id") == "C001").first()
    assert row.customer_city == "sao paulo"

    # State normalized to uppercase
    assert row.customer_state == "SP"

    # Only valid rows remain
    assert df.count() == 2
