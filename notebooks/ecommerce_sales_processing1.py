# Databricks notebook source
# =============================================================================
# Notebook: 01_raw_tables.py
# Purpose : Ingest source data and register raw Delta tables in the catalog.
# Layer   : Bronze (raw)
# =============================================================================

# COMMAND ----------
# %md
# ## 01 – Raw Tables
# Reads source data (CSV / Parquet / streaming) and writes unmodified Delta tables
# for orders, customers, products, and transactions.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/ecommerce_databricks")   # Databricks path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)
from utils.spark_helpers import (
    get_or_create_spark,
    validate_not_empty,
    drop_duplicates_with_log,
    cast_columns,
)

# COMMAND ----------
# ── Configuration ──────────────────────────────────────────────────────────────

DATABASE       = "ecommerce"
RAW_BASE_PATH  = "/mnt/delta/raw"          # adjust to your ADLS / S3 mount
SOURCE_PATH    = "/mnt/source"             # landing zone

spark = get_or_create_spark()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

# COMMAND ----------

ORDERS_SCHEMA = StructType([
    StructField("order_id",    StringType(),  nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("product_id",  StringType(),  nullable=False),
    StructField("order_date",  StringType(),  nullable=False),
    StructField("quantity",    IntegerType(), nullable=False),
    StructField("unit_price",  DoubleType(),  nullable=False),
    StructField("unit_cost",   DoubleType(),  nullable=False),
    StructField("status",      StringType(),  nullable=True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",        StringType(), nullable=False),
    StructField("first_name",         StringType(), nullable=False),
    StructField("last_name",          StringType(), nullable=False),
    StructField("email",              StringType(), nullable=True),
    StructField("country",            StringType(), nullable=True),
    StructField("city",               StringType(), nullable=True),
    StructField("registration_date",  StringType(), nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",   StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category",     StringType(), nullable=True),
    StructField("sub_category", StringType(), nullable=True),
    StructField("brand",        StringType(), nullable=True),
    StructField("list_price",   DoubleType(), nullable=False),
    StructField("cost_price",   DoubleType(), nullable=False),
])

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id",   StringType(), nullable=False),
    StructField("order_id",         StringType(), nullable=False),
    StructField("transaction_date", StringType(), nullable=True),
    StructField("payment_method",   StringType(), nullable=True),
    StructField("amount",           DoubleType(), nullable=True),
    StructField("status",           StringType(), nullable=True),
])

# COMMAND ----------
# ── Helper: read CSV with schema ───────────────────────────────────────────────

def read_csv(path: str, schema: StructType):
    """Read a CSV file using an explicit schema."""
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")           # keep bad rows; flag in _corrupt_record
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(path)
    )

# COMMAND ----------
# ── Helper: load from sample data when running in unit-test / dev mode ─────────
# In production this block is replaced by read_csv() calls above.

def load_from_sample():
    """
    Load DataFrames directly from Python lists (used in tests / notebooks
    without a live source mount).
    """
    import sys
    sys.path.insert(0, "/Workspace/ecommerce_databricks")
    from data.sample_data import (
        ORDERS_DATA,    ORDERS_COLUMNS,
        CUSTOMERS_DATA, CUSTOMERS_COLUMNS,
        PRODUCTS_DATA,  PRODUCTS_COLUMNS,
        TRANSACTIONS_DATA, TRANSACTIONS_COLUMNS,
    )

    df_orders       = spark.createDataFrame(ORDERS_DATA,       ORDERS_COLUMNS)
    df_customers    = spark.createDataFrame(CUSTOMERS_DATA,    CUSTOMERS_COLUMNS)
    df_products     = spark.createDataFrame(PRODUCTS_DATA,     PRODUCTS_COLUMNS)
    df_transactions = spark.createDataFrame(TRANSACTIONS_DATA, TRANSACTIONS_COLUMNS)

    return df_orders, df_customers, df_products, df_transactions

# COMMAND ----------
# ── Ingest & cast ──────────────────────────────────────────────────────────────

def ingest_orders(df_raw):
    """Clean and cast the orders DataFrame."""
    df = drop_duplicates_with_log(df_raw, subset=["order_id"])
    df = cast_columns(df, {
        "order_date": "date",
        "quantity":   "integer",
        "unit_price": "double",
        "unit_cost":  "double",
    })
    # Add metadata
    df = df.withColumn("_ingested_at", F.current_timestamp())
    return df

def ingest_customers(df_raw):
    """Clean and cast the customers DataFrame."""
    df = drop_duplicates_with_log(df_raw, subset=["customer_id"])
    df = cast_columns(df, {"registration_date": "date"})
    df = df.withColumn("_ingested_at", F.current_timestamp())
    return df

def ingest_products(df_raw):
    """Clean and cast the products DataFrame."""
    df = drop_duplicates_with_log(df_raw, subset=["product_id"])
    df = cast_columns(df, {
        "list_price": "double",
        "cost_price": "double",
    })
    df = df.withColumn("_ingested_at", F.current_timestamp())
    return df

def ingest_transactions(df_raw):
    """Clean and cast the transactions DataFrame."""
    df = drop_duplicates_with_log(df_raw, subset=["transaction_id"])
    df = cast_columns(df, {
        "transaction_date": "date",
        "amount": "double",
    })
    df = df.withColumn("_ingested_at", F.current_timestamp())
    return df

# COMMAND ----------
# ── Write raw Delta tables ──────────────────────────────────────────────────────

def write_raw_table(df, table_name: str, partition_cols=None):
    """
    Persist a DataFrame as a managed Delta table in the raw layer.
    Overwrites the table on each run (full-refresh pattern).
    """
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(f"{DATABASE}.{table_name}")
    print(f" Written {DATABASE}.{table_name} ({df.count()} rows)")

# COMMAND ----------
# ── Main execution ─────────────────────────────────────────────────────────────

df_orders_raw, df_customers_raw, df_products_raw, df_transactions_raw = load_from_sample()

# Ingest
df_orders       = ingest_orders(df_orders_raw)
df_customers    = ingest_customers(df_customers_raw)
df_products     = ingest_products(df_products_raw)
df_transactions = ingest_transactions(df_transactions_raw)

# Validate
validate_not_empty(df_orders,       "raw_orders")
validate_not_empty(df_customers,    "raw_customers")
validate_not_empty(df_products,     "raw_products")
validate_not_empty(df_transactions, "raw_transactions")

# Persist
write_raw_table(df_orders,       "raw_orders",       partition_cols=["status"])
write_raw_table(df_customers,    "raw_customers")
write_raw_table(df_products,     "raw_products",     partition_cols=["category"])
write_raw_table(df_transactions, "raw_transactions", partition_cols=["status"])

# COMMAND ----------
print("Notebook 01_raw_tables completed successfully.")
