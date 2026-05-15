from pyspark.sql import functions as F

def transform_customers(df):
    df = df.select(
        F.col("`Customer ID`").alias("customer_id"),
        F.col("`Customer Name`").alias("customer_name"),
        F.col("Country").alias("country"),
        F.col("phone")
    )

    df = df.withColumn("phone", F.col("phone").cast("string"))

    df = (
        df.withColumn("customer_id", F.trim(F.col("customer_id")))
          .withColumn("customer_name", F.trim(F.col("customer_name")))
          .withColumn("country", F.trim(F.col("country")))

          # Clean name
          .withColumn(
              "customer_name_clean",
              F.trim(
                  F.regexp_replace(
                      F.col("customer_name"),
                      r"[^A-Za-z\s\-']",
                      ""
                  )
              )
          )

          # Phone cleaning
          .withColumn(
              "phone_clean",
              F.when(
                  F.col("phone").isNull() |
                  F.col("phone").isin("#ERROR!", "", "null", "NULL") |
                  (F.trim(F.col("phone")) == ""),
                  F.lit(None)
              ).otherwise(F.trim(F.col("phone")))
          )
    )

    df = (
        df.filter(
            (F.col("customer_id").isNotNull()) &
            (F.col("customer_id") != "")
        )
        .dropDuplicates(["customer_id"])
    )

    return df
