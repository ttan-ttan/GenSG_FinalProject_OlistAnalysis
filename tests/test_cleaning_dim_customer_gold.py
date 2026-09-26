"""Tests for Gold customer dimension cleaning."""

from src.cleaning_dim_customer_gold import clean_customers_gold

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_city",
    "customer_state",
    "customer_first_purchase_date",
]


def test_gold_cleaning_standardizes_city_and_state(spark):
    """Gold cleaning trims and standardizes city and state values."""
    df = spark.createDataFrame(
        [("C001", "  Sao Paulo  ", " sp ", "2020-01-01")],
        CUSTOMER_COLUMNS,
    )

    result = clean_customers_gold(df).first()

    assert result["customer_city"] == "sao paulo"
    assert result["customer_state"] == "SP"


def test_gold_cleaning_removes_unusable_customer_records(spark):
    """Gold cleaning removes rows without an ID, date, or with a future date."""
    df = spark.createDataFrame(
        [
            (None, "sao paulo", "SP", "2020-01-01"),
            ("C002", "campinas", "SP", None),
            ("C003", "santos", "SP", "2999-01-01"),
            ("C004", "recife", "PE", "2021-05-10"),
        ],
        CUSTOMER_COLUMNS,
    )

    result = clean_customers_gold(df)

    assert [row["customer_id"] for row in result.collect()] == ["C004"]
