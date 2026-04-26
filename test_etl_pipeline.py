"""
Unit Tests — AWS ETL Pipeline
Tests transformation logic using PySpark local mode.
Run with: pytest tests/
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import Row
import sys
sys.path.insert(0, "../src")


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder
        .appName("ETL-Tests")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame mimicking raw S3 input."""
    data = [
        Row(id="1", date="2024-01-15", amount=  250.0, category="sales"),
        Row(id="2", date="2024-01-16", amount= -50.0,  category="returns"),
        Row(id="3", date="2024-01-17", amount=  100.0, category=" marketing "),
        Row(id=None, date="2024-01-18", amount= 300.0, category="sales"),   # null id
        Row(id="5", date=None,         amount= 400.0, category="sales"),    # null date
    ]
    return spark.createDataFrame(data)


# ─────────────────────────────────────────────────────────────────────────────

def test_drop_null_ids(spark, sample_df):
    """Rows with null id should be removed."""
    from etl_pipeline import transform
    result = transform(sample_df)
    assert result.filter("id IS NULL").count() == 0, "Null IDs should be dropped"


def test_drop_null_dates(spark, sample_df):
    """Rows with null date should be removed."""
    from etl_pipeline import transform
    result = transform(sample_df)
    assert result.filter("date IS NULL").count() == 0, "Null dates should be dropped"


def test_negative_amounts_zeroed(spark, sample_df):
    """Negative amounts should be replaced with 0.0."""
    from etl_pipeline import transform
    result = transform(sample_df)
    neg = result.filter("amount < 0").count()
    assert neg == 0, "No negative amounts should exist after transform"


def test_string_trimming(spark, sample_df):
    """String fields should be trimmed of whitespace."""
    from etl_pipeline import transform
    result = transform(sample_df)
    row = result.filter("id = '3'").select("category").first()
    assert row["category"] == row["category"].strip(), "Category should be trimmed"


def test_string_uppercasing(spark, sample_df):
    """String fields should be uppercased."""
    from etl_pipeline import transform
    result = transform(sample_df)
    row = result.filter("id = '1'").select("category").first()
    assert row["category"] == row["category"].upper(), "Category should be uppercase"


def test_audit_columns_added(spark, sample_df):
    """Audit columns etl_loaded_at and etl_source should be present."""
    from etl_pipeline import transform
    result = transform(sample_df)
    assert "etl_loaded_at" in result.columns, "etl_loaded_at column missing"
    assert "etl_source"    in result.columns, "etl_source column missing"


def test_record_count_after_transform(spark, sample_df):
    """Only 3 valid records should survive (rows 1, 3, with valid id+date+amount)."""
    from etl_pipeline import transform
    result = transform(sample_df)
    # rows 4 (null id) and 5 (null date) are dropped → 3 remain
    assert result.count() == 3, f"Expected 3 records, got {result.count()}"
