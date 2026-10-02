"""Gold-layer transformations for the order-item-grain fact table."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_fact_order_item_gold(df: DataFrame) -> DataFrame:
	"""Standardize line measures and add item-plus-freight value."""
	id_columns = ["order_id", "product_id", "seller_id"]
	for column in id_columns:
		df = df.withColumn(column, F.trim(F.col(column)))

	df = (
		df.withColumn("order_item_id", F.col("order_item_id").cast("int"))
		.withColumn("price", F.col("price").cast("double"))
		.withColumn("freight_value", F.col("freight_value").cast("double"))
	)

	return df.withColumn(
		"item_total_value",
		F.round(F.col("price") + F.col("freight_value"), 2),
	)


def build_fact_order_items_gold(
	order_items: DataFrame, fact_orders: DataFrame
) -> DataFrame:
	"""Build an item-grain fact linked to the Gold Orders fact."""
	required_item_columns = [
		"order_id",
		"order_item_id",
		"product_id",
		"seller_id",
		"shipping_limit_date",
		"price",
		"freight_value",
	]
	missing = [column for column in required_item_columns if column not in order_items.columns]
	if missing:
		raise ValueError(f"Missing required Order Items columns: {missing}")
	order_columns = ["order_id", "customer_key", "purchase_date_key"]
	missing_order_columns = [
		column for column in order_columns if column not in fact_orders.columns
	]
	if missing_order_columns:
		raise ValueError(
			f"fact_orders is missing required columns: {missing_order_columns}"
		)
	if (
		fact_orders.filter(F.col("order_id").isNull()).limit(1).count()
		or fact_orders.groupBy("order_id")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	):
		raise ValueError("fact_orders must contain unique, non-null order_id values")

	duplicate_items = (
		order_items.groupBy("order_id", "order_item_id")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	)
	if duplicate_items:
		raise ValueError("Duplicate order_id and order_item_id combination detected")

	clean_items = clean_fact_order_item_gold(order_items)
	if clean_items.filter(F.col("order_id").isNull()).limit(1).count():
		raise ValueError("order_items contains null order_id values")

	orphan_items = clean_items.join(
		fact_orders.select("order_id").distinct(), on="order_id", how="left_anti"
	)
	if orphan_items.limit(1).count():
		raise ValueError("Order Items contains order_id values missing from fact_orders")

	orders_lookup = fact_orders.select(*order_columns)
	result = clean_items.join(
		orders_lookup, on="order_id", how="inner"
	)
	if result.count() != clean_items.count():
		raise ValueError("Gold Orders join changed the one-row-per-order-item grain")

	from src.validation_fact_order_item_gold import validate_fact_order_item_gold

	validate_fact_order_item_gold(result)
	return result
