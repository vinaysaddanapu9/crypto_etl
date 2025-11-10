import logging
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql import Row
import datetime
import os

def setup_logging():
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"etl_{datetime.now().strftime('%Y%m%d')}.log")

    logging.basicConfig(
        filename="./logs/etl.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config():
    load_dotenv("./config/.env")
    return {
        "pg_host": os.getenv("PG_HOST"),
        "pg_port": os.getenv("PG_PORT"),
        "pg_db": os.getenv("PG_DB"),
        "pg_user": os.getenv("PG_USER"),
        "pg_password": os.getenv("PG_PASSWORD")
    }

def validate_data(sdf):
    print("Running data quality checks...")

    duplicate_count = sdf.groupBy("crypto_id").count().filter(col("count") > 1).count()
    if duplicate_count > 1:
        raise ValueError(f"Found {duplicate_count} duplicate crypto_id(s).")

    null_prices = sdf.filter(col("price_usd").isNull() | (col("price_usd") <= 0)).count()
    if null_prices > 0:
        raise ValueError(f"Found {null_prices} rows with invalid price_usd values.")

    total_count = sdf.count()
    print(f"Data quality passed. Total valid rows: {total_count}\n")
    return sdf





