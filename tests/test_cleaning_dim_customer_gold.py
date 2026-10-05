"""Tests for Gold customer dimension cleaning."""

from src.cleaning_dim_customer_gold import build_dim_customer, clean_customers_gold

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


def test_build_dim_customer_uses_unique_customer_grain(spark):
    customers = spark.createDataFrame(
        [
            ("c1", "u1", "sao paulo", "SP"),
            ("c2", "u1", "campinas", "SP"),
            ("c3", "u2", "recife", "PE"),
        ],
        ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
    )
    orders = spark.createDataFrame(
        [("c1", "2018-01-03"), ("c2", "2018-01-01"), ("c3", "2018-02-01")],
        ["customer_id", "order_purchase_timestamp"],
    )

    result = build_dim_customer(customers, orders)

    assert result.count() == 2
    assert result.columns == [
        "customer_key",
        "customer_unique_id",
        "first_order_date",
        "state",
        "city",
    ]
    assert (
        result.filter("customer_unique_id = 'u1'")
        .first()["first_order_date"]
        .strftime("%Y-%m-%d")
        == "2018-01-01"
    )
