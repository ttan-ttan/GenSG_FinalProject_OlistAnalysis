"""
VS Code Test Suite: test_product_category.py
Execute with: `pytest tests/`
"""

import pandas as pd
from src.cleaning_validate_product_category import (
    sanitize_category_name,
    clean_category_dataframe,
    validate_category_dataframe,
)


def test_sanitize_category_name():
    assert sanitize_category_name("  Cama_Mesa_Banho  ") == "cama mesa banho"
    assert sanitize_category_name("ELETRODOMÉSTICOS_2") == "eletrodomésticos 2"
    assert sanitize_category_name(None) == "unknown"
    assert sanitize_category_name("") == "unknown"


def test_clean_category_dataframe():
    raw_data = pd.DataFrame(
        {
            "product_category_name": [
                " perfumaria ",
                " perfumaria ",
                "cama_mesa_banho",
            ],
            "product_category_name_english": [
                "perfumery",
                "perfumery",
                "bed_bath_table",
            ],
        }
    )

    cleaned_df = clean_category_dataframe(raw_data)

    # Must deduplicate and sanitize
    assert len(cleaned_df) == 2
    assert cleaned_df.loc[0, "product_category_name"] == "perfumaria"
    assert cleaned_df.loc[0, "product_category_name_english"] == "Perfumery"


def test_validate_category_dataframe():
    valid_df = pd.DataFrame(
        {
            "product_category_name": ["perfumaria", "esporte_lazer"],
            "product_category_name_english": ["Perfumery", "Sports Leisure"],
        }
    )

    errors = validate_category_dataframe(valid_df)
    assert len(errors) == 0  # No errors expected
