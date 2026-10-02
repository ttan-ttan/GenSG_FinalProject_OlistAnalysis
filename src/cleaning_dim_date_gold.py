"""Gold date dimension construction for Orders date roles."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


ORDER_DATE_COLUMNS = [
	"order_purchase_timestamp",
	"order_approved_at",
	"order_delivered_carrier_date",
	"order_delivered_customer_date",
	"order_estimated_delivery_date",
]


def clean_dim_date_gold(orders: DataFrame) -> DataFrame:
	"""Build a conformed date dimension from available Orders timestamps."""
	missing = [column for column in ORDER_DATE_COLUMNS if column not in orders.columns]
	if missing:
		raise ValueError(f"Missing required order date columns: {missing}")

	date_frames = [
		orders.select(F.to_date(F.col(column)).alias("date_value"))
		for column in ORDER_DATE_COLUMNS
	]
	dates = date_frames[0]
	for frame in date_frames[1:]:
		dates = dates.unionByName(frame)

	dates = dates.filter(F.col("date_value").isNotNull()).distinct()
	window = Window.orderBy("date_value")

	return (
		dates.withColumn("date_key", F.row_number().over(window))
		.withColumn("calendar_year", F.year("date_value"))
		.withColumn("quarter", F.quarter("date_value"))
		.withColumn("month", F.month("date_value"))
		.withColumn("month_name", F.date_format("date_value", "MMMM"))
		.withColumn("day_of_month", F.dayofmonth("date_value"))
		.withColumn("day_of_week", F.dayofweek("date_value"))
		.withColumn("day_name", F.date_format("date_value", "EEEE"))
		.withColumn(
			"is_weekend", F.dayofweek("date_value").isin([1, 7])
		)
	)
