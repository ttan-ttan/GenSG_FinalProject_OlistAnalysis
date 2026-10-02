import pytest

from src.cleaning_fact_order_item_gold import (
	build_fact_order_items_gold,
	clean_fact_order_item_gold,
)


def test_clean_fact_order_item_gold_adds_line_value(spark):
	df = spark.createDataFrame(
		[
			(
				" order-1 ",
				"1",
				" product-1 ",
				" seller-1 ",
				"2018-01-10 10:00:00",
				"99.90",
				"12.50",
			)
		],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"shipping_limit_date",
			"price",
			"freight_value",
		],
	)

	fact = clean_fact_order_item_gold(df)
	row = fact.first()

	assert fact.count() == 1
	assert row["order_id"] == "order-1"
	assert row["product_id"] == "product-1"
	assert row["seller_id"] == "seller-1"
	assert row["order_item_id"] == 1
	assert row["item_total_value"] == 112.4


def test_build_fact_order_items_gold_links_to_orders(spark):
	items = spark.createDataFrame(
		[("order-1", 1, "product-1", "seller-1", "2018-01-10 12:00:00", 99.9, 12.5)],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"shipping_limit_date",
			"price",
			"freight_value",
		],
	)
	orders = spark.createDataFrame(
		[("order-1", 8, 42)],
		["order_id", "customer_key", "purchase_date_key"],
	)

	fact = build_fact_order_items_gold(items, orders)
	row = fact.first()

	assert fact.count() == 1
	assert row["customer_key"] == 8
	assert row["purchase_date_key"] == 42
	assert row["item_total_value"] == 112.4


def test_build_fact_order_items_gold_rejects_unknown_order(spark):
	items = spark.createDataFrame(
		[("missing-order", 1, "product-1", "seller-1", "2018-01-10 12:00:00", 99.9, 12.5)],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"shipping_limit_date",
			"price",
			"freight_value",
		],
	)
	orders = spark.createDataFrame(
		[("order-1", 8, 42)],
		["order_id", "customer_key", "purchase_date_key"],
	)

	with pytest.raises(ValueError, match="missing from fact_orders"):
		build_fact_order_items_gold(items, orders)


def test_build_fact_order_items_gold_matches_trimmed_order_ids(spark):
	items = spark.createDataFrame(
		[(" order-1 ", 1, "product-1", "seller-1", "2018-01-10 12:00:00", 99.9, 12.5)],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"shipping_limit_date",
			"price",
			"freight_value",
		],
	)
	orders = spark.createDataFrame(
		[("order-1", 8, 42)],
		["order_id", "customer_key", "purchase_date_key"],
	)

	result = build_fact_order_items_gold(items, orders)

	assert result.count() == 1
	assert result.first()["order_id"] == "order-1"


def test_build_fact_order_items_gold_rejects_duplicate_parent_orders(spark):
	items = spark.createDataFrame(
		[("order-1", 1, "product-1", "seller-1", "2018-01-10 12:00:00", 99.9, 12.5)],
		[
			"order_id",
			"order_item_id",
			"product_id",
			"seller_id",
			"shipping_limit_date",
			"price",
			"freight_value",
		],
	)
	orders = spark.createDataFrame(
		[("order-1", 8, 42), ("order-1", 9, 42)],
		["order_id", "customer_key", "purchase_date_key"],
	)

	with pytest.raises(ValueError, match="unique, non-null order_id"):
		build_fact_order_items_gold(items, orders)
