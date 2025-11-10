# src/load.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import BooleanType
import logging

def load_data(sdf, config, jdbc_driver_path):
    if sdf is None or sdf.rdd.isEmpty():
        print("No data to load — DataFrame is empty.")
        return

    spark = SparkSession.builder.getOrCreate()
    jdbc_url = f"jdbc:postgresql://{config['pg_host']}:{config['pg_port']}/{config['pg_db']}"
    table_name = "daily_crypto_prices"

    # Try loading existing data
    try:
        existing_df = spark.read.format('jdbc') \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config['pg_user']) \
            .option("password", config['pg_password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        logging.info(f"Loaded existing records from {table_name}")
    except Exception as e:
        logging.warning(f"No existing table found ({e}), initializing SCD table.")

        existing_df = spark.createDataFrame([], sdf.schema) \
        .withColumn("effective_date", lit(None).cast("timestamp")) \
        .withColumn("end_date",lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True).cast(BooleanType()))

    #Add tracking columns to new data
    sdf = sdf.withColumn("effective_date", current_timestamp()) \
                .withColumn("end_date", lit(None).cast("timestamp")) \
                .withColumn("is_current", lit(True).cast(BooleanType()))



    if existing_df.rdd.isEmpty():
        sdf.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", config['pg_user']) \
            .option("password", config['pg_password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        logging.info("Initial SCD table created.")
        return

    # Join on id (natural key)
    joined_df = sdf.alias("new").join(
        existing_df.filter(col("is_current") == True).alias("old"),
        on="id",
        how="left"
    )

    #Find changed records (compare fields that can change)
    changed_df = joined_df.filter(
        (col("old.current_price") != col("new.current_price")) |
        (col("old.market_cap") != col("new.market_cap"))
    )

    logging.info(f"Detected {changed_df.count()} changed records.")

    # Expire old records
    closed_old_df = changed_df.select("old.*") \
        .withColumn("end_date", current_timestamp()) \
               .withColumn("is_current",lit(False))

    # --- Step 5: Keep unchanged + expired + new ---
    unchanged_df = existing_df.filter(col("is_current") == True) \
        .join(changed_df.select("old.crypto_id").distinct(), on="crypto_id", how="left_anti")

    final_df = unchanged_df.unionByName(closed_old_df).unionByName(sdf)

    # Write back (overwrite or upsert)
    final_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['pg_user']) \
        .option("password", config['pg_password']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    logging.info(f"SCD Type 2 update complete. Final record count: {final_df.count()}")
    logging.info("Load: write complete")
