"""Quality checks for the Gold order-grain fact table."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_fact_orders_gold(df: DataFrame) -> DataFrame:
	"""Reject a Gold fact that violates key, grain, or order business rules."""
	required_columns = [
		"order_id",
		"customer_key",
		"purchase_date_key",
		"order_purchase_timestamp",
		"order_approved_at",
		"order_delivered_carrier_date",
		"order_delivered_customer_date",
		"item_count",
		"order_value",
		"delivery_delay_days",
	]
	missing = [column for column in required_columns if column not in df.columns]
	if missing:
		raise ValueError(f"Missing required Gold fact columns: {missing}")

	for column in ("order_id", "customer_key", "purchase_date_key"):
		if df.filter(F.col(column).isNull()).limit(1).count():
			raise ValueError(f"{column} contains null values")

	if (
		df.groupBy("order_id")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	):
		raise ValueError("Duplicate order_id values detected")

	if df.filter(F.col("item_count").isNull() | (F.col("item_count") < 0)).limit(1).count():
		raise ValueError("Invalid item_count detected")
	if df.filter(F.col("order_value").isNull() | (F.col("order_value") < 0)).limit(1).count():
		raise ValueError("Invalid order_value detected")

	timestamp_rules = [
		(
			"order_approved_at",
			F.col("order_approved_at") < F.col("order_purchase_timestamp"),
		),
		(
			"order_delivered_carrier_date",
			F.col("order_delivered_carrier_date") < F.col("order_purchase_timestamp"),
		),
		(
			"order_delivered_customer_date",
			F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"),
		),
	]
	for column, invalid_sequence in timestamp_rules:
		if df.filter(F.col(column).isNotNull() & invalid_sequence).limit(1).count():
			raise ValueError(f"{column} occurs before order_purchase_timestamp")

	if df.filter(
		F.col("delivery_delay_days").isNotNull()
		& (F.col("delivery_delay_days") < 0)
	).limit(1).count():
		raise ValueError("delivery_delay_days must be greater than or equal to zero")

	return df
