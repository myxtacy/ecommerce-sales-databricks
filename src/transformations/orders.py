from pyspark.sql import functions as F


def transform_orders(df):
    # Select and rename columns
    df = df.selectExpr(
        "`Row ID` as row_id",
        "`Order ID` as order_id",
        "`Order Date` as order_date",
        "`Ship Date` as ship_date",
        "`Ship Mode` as ship_mode",
        "`Customer ID` as customer_id",
        "`Product ID` as product_id",
        "Quantity as quantity",
        "Price as sales_amount",
        "Discount as discount",
        "Profit as profit"
    )

    # Transformations
    df = (
        df.withColumn("order_id", F.trim(F.col("order_id")))
          .withColumn("customer_id", F.trim(F.col("customer_id")))
          .withColumn("product_id", F.trim(F.col("product_id")))

          # SAFE date parsing (handles bad values)
          .withColumn(
              "order_date",
              F.to_date(
                  F.when(
                      F.col("order_date").rlike(r"\d{1,2}/\d{1,2}/\d{4}"),
                      F.col("order_date")
                  ),
                  "d/M/yyyy"
              )
          )
          .withColumn(
              "ship_date",
              F.to_date(
                  F.when(
                      F.col("ship_date").rlike(r"\d{1,2}/\d{1,2}/\d{4}"),
                      F.col("ship_date")
                  ),
                  "d/M/yyyy"
              )
          )

          # Type casting
          .withColumn("quantity", F.col("quantity").cast("int"))
          .withColumn(
              "sales_amount",
              F.round(F.col("sales_amount").cast("double"), 2)
          )
          .withColumn("discount", F.col("discount").cast("double"))
          .withColumn(
              "profit",
              F.round(F.col("profit").cast("double"), 2)
          )

          # Derived column
          .withColumn("order_year", F.year("order_date"))

          # Data quality filters
          .filter(
              (F.col("order_id").isNotNull()) &
              (F.col("order_date").isNotNull()) &
              (F.col("order_id") != "")
          )

          # Deduplication
          .dropDuplicates(["order_id", "product_id"])
    )

    return df


def build_orders_enriched(orders_df, customers_df, products_df):
    return (
        orders_df.alias("o")
        .join(
            F.broadcast(customers_df.alias("c")),
            "customer_id",
            "left"
        )
        .join(
            F.broadcast(products_df.alias("p")),
            "product_id",
            "left"
        )
        .select(
            "row_id",
            "order_id",
            "order_date",
            "ship_date",
            "ship_mode",
            "order_year",

            # Customer fields
            "customer_id",
            F.coalesce(
                F.col("c.customer_name_clean"),
                F.col("c.customer_name")
            ).alias("customer_name"),
            F.col("c.country").alias("customer_country"),

            # Product fields
            "product_id",
            F.col("p.product_name").alias("product_name"),
            F.col("p.product_category").alias("product_category"),
            F.col("p.product_sub_category").alias("product_sub_category"),

            # Measures
            "quantity",
            "sales_amount",
            "discount",
            "profit"
        )
    )
