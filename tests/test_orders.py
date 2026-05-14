import pytest
from pyspark.sql import Row
from datetime import date
from src.transformations.orders import transform_orders


# 1. Order Date Parsing (valid + invalid)
@pytest.mark.parametrize("order_date_input, expected", [
    ("21/8/2016", date(2016, 8, 21)),
    ("1/1/2020", date(2020, 1, 1)),
    ("bad-date", None)
])
@pytest.mark.smoke
def test_order_date_parsing(spark, order_date_input, expected):
    df = spark.createDataFrame([Row(**{
        "Row ID": 1,
        "Order ID": "O1",
        "Order Date": order_date_input,
        "Ship Date": "25/8/2016",
        "Ship Mode": "Standard Class",
        "Customer ID": "C1",
        "Product ID": "P1",
        "Quantity": 2,
        "Price": 10.0,
        "Discount": 0.0,
        "Profit": 1.23
    })])

    row = transform_orders(df).first()

    assert row["order_date"] == expected


# 2. Ship Date Parsing
@pytest.mark.parametrize("ship_date_input, expected", [
    ("25/8/2016", date(2016, 8, 25)),
    ("1/1/2021", date(2021, 1, 1))
])
@pytest.mark.smoke
def test_ship_date_parsing(spark, ship_date_input, expected):
    df = spark.createDataFrame([Row(**{
        "Row ID": 2,
        "Order ID": "O2",
        "Order Date": "21/8/2016",
        "Ship Date": ship_date_input,
        "Ship Mode": "Standard Class",
        "Customer ID": "C1",
        "Product ID": "P1",
        "Quantity": 2,
        "Price": 10.0,
        "Discount": 0.0,
        "Profit": 1.23
    })])

    row = transform_orders(df).first()

    assert row["ship_date"] == expected


# 3. Profit Rounding
@pytest.mark.parametrize("profit_input, expected", [
    (12.3456, 12.35),
    (10.0, 10.0),
    (5.678, 5.68)
])
@pytest.mark.regression
def test_profit_rounding(spark, profit_input, expected):
    df = spark.createDataFrame([Row(**{
        "Row ID": 3,
        "Order ID": "O3",
        "Order Date": "21/8/2016",
        "Ship Date": "25/8/2016",
        "Ship Mode": "Standard Class",
        "Customer ID": "C1",
        "Product ID": "P1",
        "Quantity": 2,
        "Price": 10.0,
        "Discount": 0.0,
        "Profit": profit_input
    })])

    row = transform_orders(df).first()

    assert row["profit"] == pytest.approx(expected, 0.01)


# 4. Order Year Derivation
@pytest.mark.smoke
def test_order_year_derivation(spark):
    df = spark.createDataFrame([Row(**{
        "Row ID": 4,
        "Order ID": "O4",
        "Order Date": "21/8/2016",
        "Ship Date": "25/8/2016",
        "Ship Mode": "Standard Class",
        "Customer ID": "C1",
        "Product ID": "P1",
        "Quantity": 2,
        "Price": 10.0,
        "Discount": 0.0,
        "Profit": 1.23
    })])

    row = transform_orders(df).first()

    assert row["order_year"] == 2016


# 5. Business Keys Retention
@pytest.mark.regression
def test_business_keys_retained(spark):
    df = spark.createDataFrame([Row(**{
        "Row ID": 5,
        "Order ID": "O5",
        "Order Date": "21/8/2016",
        "Ship Date": "25/8/2016",
        "Ship Mode": "Standard Class",
        "Customer ID": "C99",
        "Product ID": "P99",
        "Quantity": 2,
        "Price": 10.0,
        "Discount": 0.0,
        "Profit": 1.23
    })])

    row = transform_orders(df).first()

    assert row["customer_id"] == "C99"
    assert row["product_id"] == "P99"