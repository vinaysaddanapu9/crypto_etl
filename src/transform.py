from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import logging

def transform_data(spark, pandas_df):
    logging.info("Transform: creating Spark DataFrame")
    sdf = spark.createDataFrame(pandas_df)

    sdf = sdf.select(
        col("id").alias("crypto_id"),
        col("symbol"),
        col("name"),
        col("current_price").alias("price_usd"),
        col("market_cap"),
        col("total_volume"),
        col("high_24h"),
        col("low_24h"),
        current_timestamp().alias("load_timestamp")
    )

    # Validation: remove null or negative prices
    sdf = sdf.filter(col("price_usd").isNotNull() & (col("price_usd") > 0))

    valid_count = sdf.count()
    print(valid_count)
    logging.info(f"Transform: filtered to {valid_count} valid rows")
    return sdf