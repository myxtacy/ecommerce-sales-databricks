from pyspark.sql import functions as F

def transform_customers(df):
    df = df.selectExpr(
        "`Customer ID` as customer_id",
        "`Customer Name` as customer_name",
        "Country as country",
        "phone"
    )

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
    )

    # Phone cleaning
    df = df.withColumn(
        "phone_clean",
        F.when(
            F.col("phone").isin("#ERROR!", "", "null", "NULL") |
            (F.trim(F.col("phone")) == ""),
            F.lit(None)
        ).otherwise(F.trim(F.col("phone")))
    )

    df = (
        df.filter(
            (F.col("customer_id").isNotNull()) &
            (F.col("customer_id") != "")
        )
        .dropDuplicates(["customer_id"])
    )

    return df
