from pyspark.sql import functions as F

def transform_customers(df):
    df = df.selectExpr(
        "`Customer ID` as customer_id",
        "`Customer Name` as customer_name",
        "Country as country",
        "Segment as segment",
        "City as city",
        "State as state",
        "`Postal Code` as postal_code",
        "Region as region"
    )

    df = (
        df.withColumn("customer_id", F.trim(F.col("customer_id")))
          .withColumn("customer_name", F.trim(F.col("customer_name")))
          .withColumn("country", F.trim(F.col("country")))
          .withColumn("segment", F.trim(F.col("segment")))
          .withColumn("city", F.trim(F.col("city")))
          .withColumn("state", F.trim(F.col("state")))

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

          # Postal code normalization
          .withColumn("postal_code", F.col("postal_code").cast("string"))
    )

    # Phone cleaning
    if "phone" in df.columns:
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
