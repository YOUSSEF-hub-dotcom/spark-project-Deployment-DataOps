import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession"""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("DataOpsUnitTest") \
        .getOrCreate()

def test_clean_data(spark):
    # 1. Creating mock data
    input_data = [
        ("Alice", 100.0),   # Valid record
        ("Bob", -50.0),     # amount <= 0 (will be removed)

        ("Charlie", 0.0),   # amount <= 0 (will be removed)
        (None, 200.0),      # name is NULL (will be removed)
    ]
    
    schema = ["name", "amount"]
    input_df = spark.createDataFrame(input_data, schema)
    
    # 2. Calling the clean_data function
    result_df = clean_data(input_df)
    results = result_df.collect()
    
    # 3. Assertions
    assert len(results) == 1
    
    row = results[0]
    assert row["name"] == "Alice"
    assert row["amount"] == 100.0
    
    # 4. Check if amount_with_tax is correctly calculated
    assert "amount_with_tax" in row.asDict()
    assert pytest.approx(row["amount_with_tax"], 0.01) == 120.0
    