"""Tests for Gold seller dimension cleaning."""

from src.cleaning_dim_seller_gold import clean_sellers_gold


def test_gold_clean_valid(spark):
    """Valid seller records should pass cleaning."""
    df = spark.createDataFrame(
        [
            ("S001", "sao paulo", "SP", 12345),
            ("S002", "campinas", "SP", 13056),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )

    out = clean_sellers_gold(df)
    assert out.count() == 2


def test_gold_clean_invalid_zip(spark):
    """ZIP < 1000 should be removed."""
    df = spark.createDataFrame(
        [
            ("S001", "sao paulo", "SP", 999),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )

    out = clean_sellers_gold(df)
    assert out.count() == 0


def test_gold_clean_missing_critical_fields(spark):
    """Missing seller ID or ZIP should be removed."""
    df = spark.createDataFrame(
        [
            (None, "sao paulo", "SP", 12345),
            ("S002", "campinas", "SP", None),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )

    out = clean_sellers_gold(df)

    assert out.count() == 0


def test_gold_clean_standardization(spark):
    """City should be lowercase, state uppercase."""
    df = spark.createDataFrame(
        [
            ("S001", "Sao Paulo", "sp", 12345),
        ],
        ["seller_id", "seller_city", "seller_state", "seller_zip_code_prefix"],
    )

    out = clean_sellers_gold(df).first()
    assert out["seller_city"] == "sao paulo"
    assert out["seller_state"] == "SP"


# python -m pytest -v tests/test_cleaning_dim_seller_gold.py tests/test_validation_dim_seller_
# gold.py
# need to stop after completion to exit.
