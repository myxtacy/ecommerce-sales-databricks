from pyspark.sql import functions as F

def transform_products(df):
    df = df.selectExpr(
        "`Product ID` as product_id",
        "Category as product_category",
        "`Sub-Category` as product_sub_category",
        "`Product Name` as product_name",
        "`Price per product` as price_per_product"
    )

    df = (
        df.withColumn("product_id", F.trim(F.col("product_id")))
          .withColumn("product_name", F.trim(F.col("product_name")))
          .withColumn("product_category", F.trim(F.col("product_category")))
          .withColumn("product_sub_category", F.trim(F.col("product_sub_category")))

          .withColumn(
              "price_per_product",
              F.round(
                  F.coalesce(
                      F.col("price_per_product").cast("double"),
                      F.lit(0.0)
                  ),
                  2
              )
          )

          .filter(
              (F.col("product_id").isNotNull()) &
              (F.col("product_id") != "")
          )

          .dropDuplicates(["product_id"])
    )

    return df