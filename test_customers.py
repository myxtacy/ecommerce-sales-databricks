import pytest
from pyspark.sql import Row
from src.transformations.customers import transform_customers


# 1. Name Cleaning (special chars + trimming)
@pytest.mark.parametrize("input_name, expected", [
    ("Pete@#$ Takahito", "Pete Takahito"),
    (" Alice Johnson ", "Alice Johnson"),
    ("Bob Smith", "Bob Smith")
])
@pytest.mark.smoke
def test_customer_name_cleaning(spark, input_name, expected):
    df = spark.createDataFrame([Row(**{
        "Customer ID": "C1",
        "Customer Name": input_name,
        "phone": "12345",
        "Country": "United States"
    })])

    row = transform_customers(df).first()

    assert row["customer_name_clean"] == expected


# 2. Phone Cleaning (error + blank → null)
@pytest.mark.parametrize("phone_input", [
    "#ERROR!",
    "",
    None
])
@pytest.mark.regression
def test_customer_phone_cleaning(spark, phone_input):
    df = spark.createDataFrame([Row(**{
        "Customer ID": "C2",
        "Customer Name": "Test User",
        "phone": phone_input,
        "Country": "United States"
    })])

    row = transform_customers(df).first()

    assert row["phone_clean"] is None


# 3. Country Retention
@pytest.mark.parametrize("country", [
    "United States",
    "Canada",
    "India"
])
@pytest.mark.smoke
def test_customer_country_retained(spark, country):
    df = spark.createDataFrame([Row(**{
        "Customer ID": "C3",
        "Customer Name": "User",
        "phone": "123",
        "Country": country
    })])

    row = transform_customers(df).first()

    assert row["country"] == country


# 4. Deduplication
@pytest.mark.regression
def test_customer_deduplication(spark):
    df = spark.createDataFrame([
        Row(**{"Customer ID": "C4", "Customer Name": "Eva", "phone": "111", "Country": "US"}),
        Row(**{"Customer ID": "C4", "Customer Name": "Eva", "phone": "111", "Country": "US"})
    ])

    result = transform_customers(df).collect()

    assert len(result) == 1
    assert result[0]["customer_id"] == "C4"