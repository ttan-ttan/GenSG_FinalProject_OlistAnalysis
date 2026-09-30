"""
VS Code Module: product_category.py
Handles string sanitization, validation, and missing value handling for product category dataset.
"""

import re
from typing import Dict, List, Optional
import pandas as pd


def sanitize_category_name(text: Optional[str]) -> str:
    """Standardize string formatting: strip whitespace, handle case, remove special symbols."""
    if not isinstance(text, str) or not text.strip():
        return "unknown"

    # Trim and lower
    cleaned = text.strip().lower()
    # Replace underscores/hyphens with spaces
    cleaned = re.sub(r'[\_\-]+', ' ', cleaned)
    # Remove special characters keeping alphanumeric and spaces
    cleaned = re.sub(r'[^\w\s]', '', cleaned)
    # Replace multiple spaces with a single space
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    return cleaned if cleaned else "unknown"


def clean_category_dataframe(
    df: pd.DataFrame,
    pt_col: str = "product_category_name",
    en_col: str = "product_category_name_english"
) -> pd.DataFrame:
    """Applies sanitization and translations to DataFrame."""
    cleaned_df = df.copy()

    # Sanitize category strings
    cleaned_df[pt_col] = cleaned_df[pt_col].apply(sanitize_category_name)

    if en_col in cleaned_df.columns:
        cleaned_df[en_col] = cleaned_df[en_col].apply(
            lambda x: sanitize_category_name(
                x).title() if isinstance(x, str) else "Unknown"
        )
    else:
        cleaned_df[en_col] = "Unknown"

    # Deduplicate based on primary key category name
    cleaned_df = cleaned_df.drop_duplicates(
        subset=[pt_col]).reset_index(drop=True)
    return cleaned_df


def validate_category_dataframe(
    df: pd.DataFrame,
    pt_col: str = "product_category_name",
    en_col: str = "product_category_name_english"
) -> List[str]:
    """Runs data quality validation checks returning list of failure messages."""
    errors = []

    # Check 1: Primary category key must not contain nulls
    if df[pt_col].isnull().any():
        errors.append(
            "Validation Failure: Found NULL values in Portuguese category column.")

    # Check 2: Check for empty strings
    if (df[pt_col] == "").any():
        errors.append(
            "Validation Failure: Empty strings found in category names.")

    # Check 3: Check uniqueness
    if df[pt_col].duplicated().any():
        errors.append("Validation Failure: Duplicate category entries exist.")

    # Check 4: Unmapped English translation ratio check
    unmapped_ratio = (df[en_col] == "Unknown").mean()
    if unmapped_ratio > 0.10:
        errors.append(
            f"Validation Alert: More than 10% ({unmapped_ratio:.1%}) of categories lack English translation.")

    return errors
