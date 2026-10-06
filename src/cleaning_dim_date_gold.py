"""
cleaning_dim_date_gold
Gold-layer build for the date dimension (dbo.gold_dim_date).
Called by gold_dispatcher via run_clean(spark).

Columns required by the Power BI semantic model:
    date_key, date, year, month, dow, is_black_friday, event_window, is_weekend,
    event_period, event_period_sort, period_group, period_group_sort
Text labels below are matched exactly by DAX measures; do not rename them
without updating the semantic model.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

OUTPUT_TABLE = "dbo.gold_dim_date"

# ---- Calendar range ----
START_DATE = "2016-01-01"
END_DATE = "2018-12-31"
WINDOW_DAYS = 7

# ---- 2017 Black Friday study periods (inclusive) ----
PRE_START, PRE_END = "2017-10-27", "2017-11-23"
EVENT_START, EVENT_END = "2017-11-24", "2017-11-26"
POST_START, POST_END = "2017-11-27", "2017-12-24"

# ---- Labels ----
WEEKEND_YES, WEEKEND_NO = "Weekend", "Weekday"
BF_YES, BF_NO = "Black Friday", "Other Day"
WINDOW_YES = f"Within {WINDOW_DAYS} days of Black Friday"
WINDOW_NO = f"More than {WINDOW_DAYS} days away"
PERIOD_PRE = "Pre 28 days"
PERIOD_EVENT = "Black Friday 3 days"
PERIOD_POST = "Post 28 days"
PERIOD_OTHER = "Other days"
GROUP_EVENT, GROUP_BASE, GROUP_OTHER = "Black Friday", "Baseline", "Other days"

OUTPUT_COLUMNS = [
    "date_key",
    "date",
    "year",
    "month",
    "dow",
    "is_black_friday",
    "event_window",
    "is_weekend",
    "event_period",
    "event_period_sort",
    "period_group",
    "period_group_sort",
]


def black_friday_of(year_col):
    """Black Friday = day after the 4th Thursday of November."""
    nov_first = F.make_date(year_col, F.lit(11), F.lit(1))
    days_to_first_thu = (5 - F.dayofweek(nov_first) + 7) % 7
    return F.date_add(nov_first, days_to_first_thu + 22)


def event_period_sort_of(date_col):
    """1 = Pre, 2 = Black Friday 3 days, 3 = Post, 4 = Other."""
    return (
        F.when(date_col.between(F.lit(PRE_START), F.lit(PRE_END)), 1)
        .when(date_col.between(F.lit(EVENT_START), F.lit(EVENT_END)), 2)
        .when(date_col.between(F.lit(POST_START), F.lit(POST_END)), 3)
        .otherwise(4)
    )


def add_date_attributes(df: DataFrame) -> DataFrame:
    """Add all dimension attributes to a DataFrame with a `date` column."""
    bf = black_friday_of(F.year("date"))
    sort = F.col("event_period_sort")
    group_sort = F.col("period_group_sort")
    return (
        df.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("dow", F.dayofweek("date"))  # 1 = Sunday ... 7 = Saturday
        .withColumn(
            "is_weekend",
            F.when(F.col("dow").isin(1, 7), WEEKEND_YES).otherwise(WEEKEND_NO),
        )
        .withColumn(
            "is_black_friday",
            F.when(F.col("date") == bf, BF_YES).otherwise(BF_NO),
        )
        .withColumn(
            "event_window",
            F.when(F.abs(F.datediff("date", bf)) <= WINDOW_DAYS, WINDOW_YES).otherwise(
                WINDOW_NO
            ),
        )
        .withColumn("event_period_sort", event_period_sort_of(F.col("date")))
        .withColumn(
            "event_period",
            F.when(sort == 1, PERIOD_PRE)
            .when(sort == 2, PERIOD_EVENT)
            .when(sort == 3, PERIOD_POST)
            .otherwise(PERIOD_OTHER),
        )
        .withColumn(
            "period_group_sort",
            F.when(sort == 2, 1).when(sort.isin(1, 3), 2).otherwise(3),
        )
        .withColumn(
            "period_group",
            F.when(group_sort == 1, GROUP_EVENT)
            .when(group_sort == 2, GROUP_BASE)
            .otherwise(GROUP_OTHER),
        )
        .select(*OUTPUT_COLUMNS)
    )


def build_dim_date(source, start_date=START_DATE, end_date=END_DATE) -> DataFrame:
    """
    Build one row per calendar day from start_date to end_date.
    `source` is a SparkSession or any DataFrame (its session is used),
    so the calendar does not depend on which dates have orders.
    """
    spark = source.sparkSession if isinstance(source, DataFrame) else source
    dates = spark.sql(
        f"SELECT explode(sequence(to_date('{start_date}'), "
        f"to_date('{end_date}'), interval 1 day)) AS date"
    )
    return add_date_attributes(dates)


def run_clean(spark) -> DataFrame:
    """Entry point for gold_dispatcher."""
    try:
        from validation_dim_date_gold import validate_dim_date_gold
    except ImportError:
        from src.validation_dim_date_gold import validate_dim_date_gold

    dim_date = validate_dim_date_gold(build_dim_date(spark))
    (
        dim_date.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(OUTPUT_TABLE)
    )

    spark.table(OUTPUT_TABLE).groupBy(
        "period_group_sort", "period_group", "event_period_sort", "event_period"
    ).count().orderBy("event_period_sort").show(truncate=False)
    print(f"Gold dimension table '{OUTPUT_TABLE}' created successfully.")
    return dim_date
