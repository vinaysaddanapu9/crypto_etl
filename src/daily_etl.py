import logging
import os, sys

# --- Ensure the correct Python interpreter is used by PySpark ---
os.environ["PYSPARK_PYTHON"] = "E:/PySpark_Projects/crypto_etl/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "E:/PySpark_Projects/crypto_etl/venv/Scripts/python.exe"

# --- Fix imports for src package ---
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from src.utils import setup_logging, load_config
from src.utils import validate_data
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data
from src.optimize import create_indexes

def main():
    setup_logging()
    logging.info("=== ETL Run Starting ===")
    config = load_config()

    # Path to JDBC jar (relative to project root)
    jdbc_driver_path = os.path.join("drivers", "postgresql-42.7.1.jar")
    if not os.path.exists(jdbc_driver_path):
        logging.error(f"JDBC driver not found at {jdbc_driver_path}")
        raise FileNotFoundError(jdbc_driver_path)

    spark = SparkSession.builder \
    .appName("CoinGecko_Daily_ETL") \
    .master("local[*]") \
    .config("spark.jars", jdbc_driver_path) \
    .config("spark.python.worker.reuse", "true") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        #Extract
        raw_df = extract_data()
        #Transform
        sdf = transform_data(spark, raw_df)
        # Validate
        sdf = validate_data(sdf)
        #Load
        load_data(sdf, config, jdbc_driver_path)
        create_indexes(config)
        logging.info("=== ETL Run Completed Successfully ===")
    except Exception as e:
        logging.exception(f"ETL failed: {e}")
        raise
    finally:
        spark.stop()
        logging.info("Spark session stopped")


if __name__ == "__main__":
    main()






