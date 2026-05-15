from pyspark.sql import functions as F

def aggregate_profit(df):

    df = df.withColumn(
        "profit",
        F.coalesce(F.col("profit").cast("double"), F.lit(0.0))
    )

    return (
        df.filter(
            F.col("order_year").isNotNull() &
            F.col("customer_id").isNotNull()
        )
        .groupBy(
            "order_year",
            "product_category",
            "product_sub_category",
            "customer_id"
        )
        .agg(
            F.first("customer_name", ignorenulls=True).alias("customer_name"),

            F.round(
                F.sum("profit"),   #
                2
            ).alias("total_profit")
        )
    )
