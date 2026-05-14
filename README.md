# E-commerce Sales Processing with Databricks

## Objective
Build an end-to-end PySpark-based data pipeline in Databricks to process e-commerce sales data. The pipeline ingests raw data, applies data cleansing and transformations, enriches datasets, and produces aggregated business insights.

---

## Architecture Overview
The solution follows a **Bronze → Silver → Gold** layered architecture:

- **Bronze Layer**
  - Raw ingestion of source data (Orders, Products, Customers)

- **Silver Layer**
  - Data cleansing and normalization
  - Schema standardization
  - Deduplication and data quality handling

- **Gold Layer**
  - Enriched datasets (joins across dimensions and facts)
  - Aggregated business metrics for reporting

---

## Datasets
- `Orders.json`
- `Products.csv`
- `Customer.xlsx`

---

## Key Transformations

### Orders
- Column standardization and renaming
- Date parsing (`order_date`, `ship_date`)
- Data type casting (quantity, price, discount, profit)
- Profit rounding for financial consistency
- Derivation of `order_year`
- Deduplication based on `(order_id, product_id)`

---

### Customers
- Customer name cleaning using regex (removal of special characters)
- Whitespace trimming across key fields
- Null/invalid phone handling
- Deduplication based on `customer_id`

---

### Products
- Schema standardization
- Price casting and rounding
- Category/sub-category normalization
- Deduplication based on `product_id`

---

### Enrichment
- Join orders with customer and product datasets
- Broadcast joins for performance optimization
- Fallback logic for customer name (`clean → raw`)
- Enriched attributes:
  - Customer name & country
  - Product category & sub-category

---

### Aggregation
- Yearly aggregation of:
  - Total profit
  - Total sales
  - Total quantity
- Grouped by:
  - `order_year`
  - `product_category`
  - `product_sub_category`
  - `customer_id`
- Null-safe aggregation using `coalesce`
- Rounded financial metrics

---

## Execution Steps

1. Import or open
2. Update input data paths if required
3. Run all notebook cells sequentially
4. Validate outputs in Silver & Gold layers

---

## Testing Strategy

The project includes a comprehensive **pytest-based testing framework**:

- **Fixtures**
- Reusable Spark session (`conftest.py`)

- **Parametrized Tests**
- Covers multiple input scenarios efficiently

- **Markers**
- `smoke` → critical validations
- `regression` → full test coverage

- **Data Validation**
- Schema, transformations, joins, and aggregations

- **Float Precision Handling**
- `pytest.approx` used for reliable numeric comparisons

---

## Running Tests

pytest -m smoke        # Run critical tests
pytest -m regression   # Run full test suite
pytest                 # Run everything


Tech Stack

Azure Databricks
PySpark (DataFrame API)
Pytest (Unit testing)


Key Highlights

End-to-end data engineering pipeline
Clean, modular transformation design
Robust data quality handling
Optimized joins using Spark broadcast
Scalable testing framework with pytest
Production-style architecture (Bronze/Silver/Gold)
  
