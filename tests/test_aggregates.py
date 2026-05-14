import pytest
from pyspark.sql import Row
from src.transformations.orders import build_orders_enriched
from src.transformations.aggregates import aggregate_profit


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
    orders_df = spark.createDataFrame([
        Row(
            row_id=1,
            order_id="O1",
            order_date=None,
            ship_date=None,
            ship_mode="Standard",
            order_year=2017,
            customer_id=customer_id,
            product_id=product_id,
            quantity=1,
            sales_amount=100.0,
            discount=0.0,
            profit=20.0
        )
    ])

    customers_df = spark.createDataFrame([
        Row(
            customer_id="C1",
            customer_name="Alice Raw",
            customer_name_clean="Alice",
            country="United States"
        )
    ])

    products_df = spark.createDataFrame([
        Row(
            product_id="P1",
            product_name="Phone",
            product_category="Technology",
            product_sub_category="Phones"
        )
    ])

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
    rows = [
        Row(
            order_year=2017,
            product_category="Technology",
            product_sub_category="Phones",
            customer_id="C1",
            customer_name="Alice",
            profit=p
        )
        for p in profits
    ]

    df = spark.createDataFrame(rows)
    result = aggregate_profit(df).first()

    # Approx for float safety
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