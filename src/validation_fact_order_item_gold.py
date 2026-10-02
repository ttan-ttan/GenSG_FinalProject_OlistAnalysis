"""Quality checks for the Gold order-item-grain fact table."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_fact_order_item_gold(df: DataFrame) -> DataFrame:
	"""Reject item facts with invalid grain, measures, or parent order keys."""
	required_columns = [
		"order_id",
		"order_item_id",
		"product_id",
		"seller_id",
		"customer_key",
		"purchase_date_key",
		"price",
		"freight_value",
		"item_total_value",
	]
	missing = [column for column in required_columns if column not in df.columns]
	if missing:
		raise ValueError(f"Missing required Gold fact columns: {missing}")

	for column in (
		"order_id",
		"order_item_id",
		"product_id",
		"seller_id",
		"customer_key",
		"purchase_date_key",
	):
		if df.filter(F.col(column).isNull()).limit(1).count():
			raise ValueError(f"{column} contains null values")

	if (
		df.groupBy("order_id", "order_item_id")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	):
		raise ValueError("Duplicate order_id and order_item_id combination detected")

	if df.filter(F.col("order_item_id") <= 0).limit(1).count():
		raise ValueError("Invalid order_item_id detected")
	if df.filter(F.col("price").isNull() | (F.col("price") <= 0)).limit(1).count():
		raise ValueError("Invalid price detected")
	if df.filter(F.col("freight_value").isNull() | (F.col("freight_value") < 0)).limit(1).count():
		raise ValueError("Invalid freight_value detected")
	if df.filter(
		F.abs(F.col("item_total_value") - (F.col("price") + F.col("freight_value")))
		> F.lit(0.01)
	).limit(1).count():
		raise ValueError("item_total_value does not match price plus freight_value")

	return df
