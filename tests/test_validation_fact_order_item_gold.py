import pytest

from src.validation_fact_order_item_gold import validate_fact_order_item_gold


def test_validate_fact_order_item_gold_accepts_valid_row(spark):
	df = spark.createDataFrame(
		[("order-1", 1, "product-1", "seller-1", 8, 42, 99.9, 12.5, 112.4)],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"customer_key",
			"purchase_date_key",
			"price",
			"freight_value",
			"item_total_value",
		],
	)

	assert validate_fact_order_item_gold(df).count() == 1


def test_validate_fact_order_item_gold_rejects_null_order_keys(spark):
	df = spark.createDataFrame(
		[("order-1", 1, "product-1", "seller-1", None, 42, 99.9, 12.5, 112.4)],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"customer_key",
			"purchase_date_key",
			"price",
			"freight_value",
			"item_total_value",
		],
	)

	with pytest.raises(ValueError, match="customer_key contains null values"):
		validate_fact_order_item_gold(df)
