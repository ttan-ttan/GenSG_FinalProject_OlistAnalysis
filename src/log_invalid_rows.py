# pylint: disable=E1101
"""
Utility module for logging invalid rows detected during SQL DQ checks.
"""

from pyspark.sql import SparkSession


def log_invalid_rows(dataset: str, invalid_df):
    """
    Logs invalid rows into dq_invalid_logs table.

    Parameters:
        dataset (str): dataset name, e.g. 'customers'
        invalid_df (DataFrame): dataframe containing invalid rows
    """

    spark = SparkSession.builder.getOrCreate()

    # Create table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dq_invalid_logs (
            dataset STRING,
            column_name STRING,
            rule STRING,
            invalid_value STRING,
            log_timestamp TIMESTAMP
        )
    """)

    # Convert invalid rows into a flattened log format
    # (You can customize this depending on your DQ rules)
    log_df = (
        invalid_df
        .withColumn("dataset", spark.sql(f"SELECT '{dataset}'").first()[0])
        .withColumn("log_timestamp", spark.sql("SELECT current_timestamp()").first()[0])
    )

    # Append to delta table
    log_df.write.format("delta").mode("append").saveAsTable("dq_invalid_logs")

    print(
        f"[INVALID LOG] {dataset} → {invalid_df.count()} invalid rows logged.")
