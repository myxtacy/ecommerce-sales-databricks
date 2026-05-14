import pytest
from pyspark.sql import Row
from src.transformations.products import transform_products


# 1. Rename + Cast + Field Validation
@pytest.mark.parametrize("input_row, expected", [
    (
        {
            "Product ID": "P1",
            "Category": "Technology",
            "Sub-Category": "Phones",
            "Product Name": "Phone X",
            "Price per product": "99.90"
        },
        {
            "product_id": "P1",
            "product_category": "Technology",
            "product_sub_category": "Phones",
            "price_per_product": 99.9
        }
    )
])
@pytest.mark.smoke
def test_product_rename_and_cast(spark, input_row, expected):
    df = spark.createDataFrame([Row(**input_row)])
    row = transform_products(df).first()

    assert row["product_id"] == expected["product_id"]
    assert row["product_category"] == expected["product_category"]
    assert row["product_sub_category"] == expected["product_sub_category"]
    assert row["price_per_product"] == expected["price_per_product"]


# 2. Price Casting (multiple scenarios)
@pytest.mark.parametrize("price_input, expected", [
    ("99.90", 99.9),
    (150, 150.0),
    (25.5, 25.5)
])
@pytest.mark.regression
def test_product_price_casting(spark, price_input, expected):
    df = spark.createDataFrame([Row(**{
        "Product ID": "P2",
        "Category": "Furniture",
        "Sub-Category": "Chairs",
        "Product Name": "Chair A",
        "Price per product": price_input
    })])

    row = transform_products(df).first()

    # Using approx for float safety
    assert row["price_per_product"] == pytest.approx(expected, 0.01)


# 3. Deduplication Logic
@pytest.mark.regression
def test_product_deduplication_by_id(spark):
    df = spark.createDataFrame([
        Row(**{
            "Product ID": "P3",
            "Category": "Technology",
            "Sub-Category": "Accessories",
            "Product Name": "Mouse",
            "Price per product": 25.5
        }),
        Row(**{
            "Product ID": "P3",
            "Category": "Technology",
            "Sub-Category": "Accessories",
            "Product Name": "Mouse",
            "Price per product": 25.5
        })
    ])

    result = transform_products(df).collect()

    assert len(result) == 1
    assert result[0]["product_id"] == "P3"


# 4. Category & Subcategory Integrity
@pytest.mark.parametrize("category, sub_category", [
    ("Office Supplies", "Paper"),
    ("Furniture", "Chairs"),
    ("Technology", "Accessories")
])
@pytest.mark.smoke
def test_product_category_and_subcategory_retained(spark, category, sub_category):
    df = spark.createDataFrame([Row(**{
        "Product ID": "P4",
        "Category": category,
        "Sub-Category": sub_category,
        "Product Name": "Test Product",
        "Price per product": 5.0
    })])

    row = transform_products(df).first()

    assert row["product_category"] == category
    assert row["product_sub_category"] == sub_category