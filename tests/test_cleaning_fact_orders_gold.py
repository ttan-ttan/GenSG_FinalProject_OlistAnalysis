from pyspark.sql import functions as F

from src.cleaning_fact_orders_gold import (
	build_fact_orders_gold,
	clean_fact_orders_gold,
)


def test_clean_fact_orders_gold_adds_lifecycle_measures(spark):
	df = spark.createDataFrame(
		[
			(
				"order-1",
				"customer-1",
				"delivered",
				"2018-01-10 10:00:00",
				"2018-01-10 12:00:00",
				"2018-01-11 09:00:00",
				"2018-01-15 14:00:00",
				"2018-01-14 00:00:00",
			)
		],
		[
			"order_id",
			"customer_id",
			"order_status",
			"order_purchase_timestamp",
			"order_approved_at",
			"order_delivered_carrier_date",
			"order_delivered_customer_date",
			"order_estimated_delivery_date",
		],
	)

	fact = clean_fact_orders_gold(df)
	row = fact.first()

	assert fact.count() == 1
	assert row["order_id"] == "order-1"
	assert row["approval_delay_hours"] == 2.0
	assert row["carrier_dispatch_days"] == 1
	assert row["delivery_days"] == 5
	assert row["delivery_delay_days"] == 1


def test_clean_fact_orders_gold_clamps_early_delivery_and_keeps_open_delay_null(spark):
	df = spark.createDataFrame(
		[
			("order-early", "2018-01-12 10:00:00", "2018-01-14 00:00:00"),
			("order-open", None, "2018-01-14 00:00:00"),
		],
		[
			"order_id",
			"order_delivered_customer_date",
			"order_estimated_delivery_date",
		],
	)

	rows = {
		row["order_id"]: row["delivery_delay_days"]
		for row in clean_fact_orders_gold(df).collect()
	}

	assert rows["order-early"] == 0
	assert rows["order-open"] is None


def test_build_fact_orders_gold_aggregates_order_value_and_item_count(spark):
	orders = spark.createDataFrame(
		[
			(
				"order-1",
				"customer-1",
				"delivered",
				"2018-01-10 10:00:00",
				"2018-01-10 12:00:00",
				"2018-01-11 09:00:00",
				"2018-01-15 14:00:00",
				"2018-01-14 00:00:00",
			)
		],
		[
			"order_id",
			"customer_id",
			"order_status",
			"order_purchase_timestamp",
			"order_approved_at",
			"order_delivered_carrier_date",
			"order_delivered_customer_date",
			"order_estimated_delivery_date",
		],
	)
	items = spark.createDataFrame(
		[("order-1", 1, 20.0), ("order-1", 2, 30.5)],
		["order_id", "order_item_id", "price"],
	)
	customers = spark.createDataFrame(
		[("customer-1", 101)], ["customer_id", "customer_key"]
	)
	dates = spark.createDataFrame(
		[
			("2018-01-10", 1),
			("2018-01-11", 2),
			("2018-01-14", 3),
			("2018-01-15", 4),
		],
		["date_value", "date_key"],
	).withColumn("date_value", F.to_date("date_value"))

	fact = build_fact_orders_gold(orders, items, customers, dates)
	row = fact.first()

	assert fact.count() == 1
	assert row["customer_key"] == 101
	assert row["item_count"] == 2
	assert row["order_value"] == 50.5
