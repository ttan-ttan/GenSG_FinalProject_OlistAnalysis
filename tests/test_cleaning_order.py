from pyspark.sql import SparkSession

from src.cleaning_orders import clean_orders


def test_clean_orders(spark):
    """Test cleaning logic for the Orders dataset."""


    # Create one fake Orders record
    data = [
        (
            "order_001",
            "customer_001",
            "  DELIVERED  ",
            "2018-01-10 10:00:00",
            "2018-01-10 11:00:00",
            "2018-01-11 09:00:00",
            "2018-01-15 14:00:00",
            "2018-01-20 00:00:00",
        )
    ]

    columns = [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    # Turn the fake record into a Spark DataFrame
    df = spark.createDataFrame(data, columns)

    # Run our cleaning function
    cleaned_df = clean_orders(df)

    # Get the first row so we can inspect the cleaned values
    row = cleaned_df.first()

    # Check that spaces are removed and status becomes lowercase
    assert row["order_status"] == "delivered"

    # Check that the date columns were converted to timestamps
    assert str(
        cleaned_df.schema["order_purchase_timestamp"].dataType
    ) == "TimestampType()"

    assert str(
        cleaned_df.schema["order_approved_at"].dataType
    ) == "TimestampType()"

    assert str(
        cleaned_df.schema["order_delivered_carrier_date"].dataType
    ) == "TimestampType()"

    assert str(
        cleaned_df.schema["order_delivered_customer_date"].dataType
    ) == "TimestampType()"

    assert str(
        cleaned_df.schema["order_estimated_delivery_date"].dataType
    ) == "TimestampType()"

    # Make sure cleaning did not remove the row
    assert cleaned_df.count() == 1
