# validation_reviews.py
# Checks that the reviews data is clean. Each check returns pass/fail plus a detail message.

from pathlib import Path
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED_COLUMNS = [
    "review_id", "order_id", "review_score", "review_comment_title",
    "review_comment_message", "review_creation_date", "review_answer_timestamp",
]
KEY_COLUMNS = ["review_id", "order_id", "review_score", "review_creation_date", "review_answer_timestamp"]


def _result(name: str, passed: bool, detail: str = "") -> dict:
    """Small helper that builds one check result."""
    return {"check": name, "passed": bool(passed), "detail": detail}


def validate_reviews(
    df: DataFrame,
    orders_df: Optional[DataFrame] = None,
    expect_one_review_per_order: bool = False,
) -> List[dict]:
    """Run all checks on the reviews table."""
    results = []

    # 1. All expected columns must exist
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    results.append(_result("required_columns_present", not missing, f"missing: {missing}"))
    if missing:
        return results

    # 2. Date columns must be real timestamps, and the score must be an integer
    types = dict(df.dtypes)
    results.append(_result("creation_date_is_timestamp", types["review_creation_date"] == "timestamp", types["review_creation_date"]))
    results.append(_result("answer_timestamp_is_timestamp", types["review_answer_timestamp"] == "timestamp", types["review_answer_timestamp"]))
    results.append(_result("score_is_int", types["review_score"] == "int", types["review_score"]))

    # 3. Key columns must not contain nulls
    null_rows = df.filter(" OR ".join(f"{c} IS NULL" for c in KEY_COLUMNS)).count()
    results.append(_result("no_nulls_in_key_columns", null_rows == 0, f"rows with nulls: {null_rows}"))

    # 4. Comments must be filled (placeholders), never null or empty
    empty_comments = df.filter(
        F.col("review_comment_title").isNull() | (F.col("review_comment_title") == "")
        | F.col("review_comment_message").isNull() | (F.col("review_comment_message") == "")
    ).count()
    results.append(_result("comments_filled", empty_comments == 0, f"empty comments: {empty_comments}"))

    # 5. Scores must be between 1 and 5
    bad_scores = df.filter(~F.col("review_score").between(1, 5)).count()
    results.append(_result("score_between_1_and_5", bad_scores == 0, f"bad scores: {bad_scores}"))

    # 6. (review_id, order_id) must be unique
    dup_keys = df.groupBy("review_id", "order_id").count().filter("count > 1").count()
    results.append(_result("review_order_key_unique", dup_keys == 0, f"duplicate keys: {dup_keys}"))

    # 7. Answer time must not be before creation time
    bad_dates = df.filter(F.col("review_answer_timestamp") < F.col("review_creation_date")).count()
    results.append(_result("answer_after_creation", bad_dates == 0, f"rows answered before created: {bad_dates}"))

    # 8. No line breaks left inside comments
    newline_rows = df.filter(
        F.col("review_comment_title").rlike("[\r\n]") | F.col("review_comment_message").rlike("[\r\n]")
    ).count()
    results.append(_result("no_line_breaks_in_comments", newline_rows == 0, f"rows with line breaks: {newline_rows}"))

    # 9. (optional) only one review per order, if we asked for that
    if expect_one_review_per_order:
        multi = df.groupBy("order_id").count().filter("count > 1").count()
        results.append(_result("one_review_per_order", multi == 0, f"orders with >1 review: {multi}"))

    # 10. (optional) every order_id should exist in the orders table
    if orders_df is not None:
        orphans = (
            df.select("order_id").distinct()
            .join(orders_df.select("order_id").distinct(), on="order_id", how="left_anti")
            .count()
        )
        results.append(_result("order_ids_exist_in_orders", orphans == 0, f"orphan order_ids: {orphans}"))

    return results


def all_passed(results: List[dict]) -> bool:
    """True only if every check passed."""
    return all(r["passed"] for r in results)


def print_report(results: List[dict]) -> None:
    """Print a simple PASS / FAIL report."""
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"[{status}] {r['check']} - {r['detail']}")


def assert_valid(results: List[dict]) -> None:
    """Stop the pipeline with an error if any check failed."""
    failed = [r for r in results if not r["passed"]]
    if failed:
        raise ValueError(f"Reviews validation failed: {failed}")


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from cleaning_reviews import load_reviews, clean_reviews

    spark = SparkSession.builder.master("local[*]").appName("validate_reviews").getOrCreate()
    input_path = Path(__file__).resolve().parents[1] / "data" / "raw" / "olist_order_reviews_dataset.csv"
    clean = clean_reviews(load_reviews(spark, str(input_path)))
    print_report(validate_reviews(clean))