import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from src.transformations.orders import build_orders_enriched
from src.transformations.aggregates import aggregate_profit


ORDERS_SCHEMA = StructType([
    StructField("row_id", IntegerType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("ship_date", StringType(), True),
    StructField("ship_mode", StringType(), True),
    StructField("order_year", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sales_amount", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("profit", DoubleType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_name_clean", StringType(), True),
    StructField("country", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_sub_category", StringType(), True),
])


# 1. Orders Enrichment (Join Scenarios)
@pytest.mark.parametrize(
    "customer_id, product_id, exp_customer, exp_country, exp_category, exp_sub_category",
    [
        # full match
        ("C1", "P1", "Alice", "United States", "Technology", "Phones"),

        # missing customer
        ("C2", "P1", None, None, "Technology", "Phones"),

        # missing product
        ("C1", "P9", "Alice", "United States", None, None),
    ]
)
@pytest.mark.smoke
def test_orders_enrichment(
    spark, customer_id, product_id,
    exp_customer, exp_country, exp_category, exp_sub_category
):
    orders_df = spark.createDataFrame(
        [(1, "O1", None, None, "Standard", 2017, customer_id, product_id, 1, 100.0, 0.0, 20.0)],
        schema=ORDERS_SCHEMA
    )

    customers_df = spark.createDataFrame(
        [("C1", "Alice Raw", "Alice", "United States")],
        schema=CUSTOMERS_SCHEMA
    )

    products_df = spark.createDataFrame(
        [("P1", "Phone", "Technology", "Phones")],
        schema=PRODUCTS_SCHEMA
    )

    row = build_orders_enriched(orders_df, customers_df, products_df).first()

    assert row["customer_name"] == exp_customer
    assert row["customer_country"] == exp_country
    assert row["product_category"] == exp_category
    assert row["product_sub_category"] == exp_sub_category


# 2. Profit Aggregation (multiple scenarios)
@pytest.mark.parametrize(
    "profits, expected_total",
    [
        ([10.25, 5.75], 16.0),   # sum
        ([10.0, -3.0], 7.0),     # negative handling
        ([None, 2.0], 2.0),      # null handling
    ]
)
@pytest.mark.regression
def test_profit_aggregation(spark, profits, expected_total):
    schema = StructType([
        StructField("order_year", IntegerType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_sub_category", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("profit", DoubleType(), True),
    ])

    rows = [
        (2017, "Technology", "Phones", "C1", "Alice", p)
        for p in profits
    ]

    df = spark.createDataFrame(rows, schema=schema)
    result = aggregate_profit(df).first()

    assert result["total_profit"] == pytest.approx(expected_total, 0.01)


# 3. Profit Grouping by Year
@pytest.mark.smoke
def test_profit_grouped_by_year(spark):
    df = spark.createDataFrame([
        Row(order_year=2017, product_category="Technology",
            product_sub_category="Phones", customer_id="C1",
            customer_name="Alice", profit=10.0),

        Row(order_year=2018, product_category="Technology",
            product_sub_category="Phones", customer_id="C1",
            customer_name="Alice", profit=5.0)
    ])

    result = aggregate_profit(df).collect()

    years = {r["order_year"] for r in result}

    assert len(result) == 2
    assert years == {2017, 2018}
