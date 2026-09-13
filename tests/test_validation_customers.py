# pylint: disable=no-member
# pylint: disable=invalid-unary-operand-type
# pylint: disable=missing-function-docstring

"""
Unit Tests for Customer Validation
Ensure the validation logic correctly enforces business rules
before promoting Silver → Gold.

Test Coverage:
1. Valid state codes
2. Valid zip code range
3. Valid timestamp (exists, correct type, not null, not future)
4. Unique customer_id
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from src.validation_customers import validate_customers

spark = SparkSession.builder.getOrCreate()


def test_validate_customers_valid():
    df = spark.createDataFrame(
        [
            ("C001", "SP", 1234, "2020-01-01 10:00:00"),
            ("C002", "RJ", 5000, "2020-01-02 12:00:00"),
        ],
        ["customer_id", "customer_state", "customer_zip_code_prefix",
         "customer_first_purchase_date"]
    ).withColumn(
        "customer_first_purchase_date",
        F.to_timestamp("customer_first_purchase_date")
    )

    validated = validate_customers(df)
    assert validated.count() == 2


def test_validate_invalid_state():
    df = spark.createDataFrame(
        [
            ("C001", "XX", 1234, "2020-01-01 10:00:00"),
        ],
        ["customer_id", "customer_state", "customer_zip_code_prefix",
         "customer_first_purchase_date"]
    ).withColumn(
        "customer_first_purchase_date",
        F.to_timestamp("customer_first_purchase_date")
    )

    try:
        validate_customers(df)
        assert False, "Expected ValueError for invalid state"
    except ValueError:
        pass


def test_validate_invalid_zip():
    df = spark.createDataFrame(
        [
            ("C001", "SP", 50, "2020-01-01 10:00:00"),  # invalid zip
        ],
        ["customer_id", "customer_state", "customer_zip_code_prefix",
         "customer_first_purchase_date"]
    ).withColumn(
        "customer_first_purchase_date",
        F.to_timestamp("customer_first_purchase_date")
    )

    try:
        validate_customers(df)
        assert False, "Expected ValueError for invalid zip"
    except ValueError:
        pass


def test_validate_future_timestamp():
    df = spark.createDataFrame(
        [
            ("C001", "SP", 1234, "2999-01-01 10:00:00"),  # future date
        ],
        ["customer_id", "customer_state", "customer_zip_code_prefix",
         "customer_first_purchase_date"]
    ).withColumn(
        "customer_first_purchase_date",
        F.to_timestamp("customer_first_purchase_date")
    )

    try:
        validate_customers(df)
        assert False, "Expected ValueError for future timestamp"
    except ValueError:
        pass


def test_validate_duplicate_customer_id():
    df = spark.createDataFrame(
        [
            ("C001", "SP", 1234, "2020-01-01 10:00:00"),
            ("C001", "SP", 5000, "2020-01-02 12:00:00"),  # duplicate
        ],
        ["customer_id", "customer_state", "customer_zip_code_prefix",
         "customer_first_purchase_date"]
    ).withColumn(
        "customer_first_purchase_date",
        F.to_timestamp("customer_first_purchase_date")
    )

    try:
        validate_customers(df)
        assert False, "Expected ValueError for duplicate customer_id"
    except ValueError:
        pass
