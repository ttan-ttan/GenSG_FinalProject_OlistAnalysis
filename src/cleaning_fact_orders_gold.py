"""Gold-layer transformations for the order-grain fact table."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.cleaning_dim_date_gold import ORDER_DATE_COLUMNS


def clean_fact_orders_gold(df: DataFrame) -> DataFrame:
	"""Add order lifecycle measures without changing the source row grain."""
	return (
		df.withColumn(
			"approval_delay_hours",
			(
				F.unix_timestamp("order_approved_at")
				- F.unix_timestamp("order_purchase_timestamp")
			)
			/ F.lit(3600.0),
		)
		.withColumn(
			"carrier_dispatch_days",
			F.datediff(
				F.col("order_delivered_carrier_date"),
				F.col("order_purchase_timestamp"),
			),
		)
		.withColumn(
			"delivery_days",
			F.datediff(
				F.col("order_delivered_customer_date"),
				F.col("order_purchase_timestamp"),
			),
		)
		.withColumn(
			"delivery_delay_days",
			F.when(
				F.col("order_delivered_customer_date").isNotNull()
				& F.col("order_estimated_delivery_date").isNotNull(),
				F.greatest(
					F.datediff(
						F.col("order_delivered_customer_date"),
						F.col("order_estimated_delivery_date"),
					),
					F.lit(0),
				),
			),
		)
	)


def build_fact_orders_gold(
	orders: DataFrame,
	order_items: DataFrame,
	dim_customer: DataFrame,
	dim_date: DataFrame,
) -> DataFrame:
	"""Join Silver-approved inputs into a validated order-grain fact."""
	required_orders = [
		"order_id",
		"customer_id",
		"order_status",
		*ORDER_DATE_COLUMNS,
	]
	missing = [column for column in required_orders if column not in orders.columns]
	if missing:
		raise ValueError(f"Missing required Orders columns: {missing}")
	for column in ("order_id", "order_item_id", "price"):
		if column not in order_items.columns:
			raise ValueError(f"Missing required Order Items column: {column}")
	if "customer_id" not in dim_customer.columns or "customer_key" not in dim_customer.columns:
		raise ValueError("dim_customer must contain customer_id and customer_key")
	if "date_value" not in dim_date.columns or "date_key" not in dim_date.columns:
		raise ValueError("dim_date must contain date_value and date_key")

	if orders.filter(F.col("order_id").isNull()).limit(1).count():
		raise ValueError("order_id contains null values")
	if (
		orders.groupBy("order_id")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	):
		raise ValueError("Duplicate order_id values detected")

	orphan_items = order_items.join(
		orders.select("order_id").distinct(), on="order_id", how="left_anti"
	)
	if orphan_items.limit(1).count():
		raise ValueError("Order Items contains order_id values missing from Orders")

	unmatched_customers = orders.select("customer_id").distinct().join(
		dim_customer.select("customer_id").distinct(),
		on="customer_id",
		how="left_anti",
	)
	if unmatched_customers.limit(1).count():
		raise ValueError("Orders contains customer_id values missing from dim_customer")
	if (
		dim_customer.groupBy("customer_id")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	):
		raise ValueError("Duplicate customer_id values detected in dim_customer")
	if (
		dim_date.groupBy("date_value")
		.count()
		.filter(F.col("count") > 1)
		.limit(1)
		.count()
	):
		raise ValueError("Duplicate date_value values detected in dim_date")

	item_metrics = order_items.groupBy("order_id").agg(
		F.count(F.lit(1)).cast("long").alias("item_count"),
		F.round(F.sum(F.col("price").cast("double")), 2).alias("order_value"),
	)
	fact = (
		orders.alias("orders")
		.join(dim_customer.select("customer_id", "customer_key").alias("customer"),
			  on="customer_id", how="inner")
		.join(item_metrics, on="order_id", how="left")
		.withColumn("item_count", F.coalesce(F.col("item_count"), F.lit(0)).cast("long"))
		.withColumn("order_value", F.coalesce(F.col("order_value"), F.lit(0.0)))
	)

	for column, key_name in [
		("order_purchase_timestamp", "purchase_date_key"),
		("order_approved_at", "approval_date_key"),
		("order_delivered_carrier_date", "carrier_date_key"),
		("order_delivered_customer_date", "delivery_date_key"),
		("order_estimated_delivery_date", "estimated_delivery_date_key"),
	]:
		date_lookup = dim_date.select(
			F.col("date_value").alias(f"{key_name}_value"),
			F.col("date_key").alias(key_name),
		)
		fact = fact.join(
			date_lookup,
			F.to_date(F.col(column)) == F.col(f"{key_name}_value"),
			how="left",
		).drop(f"{key_name}_value")

	if fact.count() != orders.count():
		raise ValueError("Gold dimension joins changed the one-row-per-order grain")
	for timestamp_column, key_column in [
		("order_purchase_timestamp", "purchase_date_key"),
		("order_approved_at", "approval_date_key"),
		("order_delivered_carrier_date", "carrier_date_key"),
		("order_delivered_customer_date", "delivery_date_key"),
		("order_estimated_delivery_date", "estimated_delivery_date_key"),
	]:
		if fact.filter(
			F.col(timestamp_column).isNotNull() & F.col(key_column).isNull()
		).limit(1).count():
			raise ValueError(f"Missing date dimension key for {timestamp_column}")

	fact = clean_fact_orders_gold(fact)
	from src.validation_fact_orders_gold import validate_fact_orders_gold

	validate_fact_orders_gold(fact)
	return fact
