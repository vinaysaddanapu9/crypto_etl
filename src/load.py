# src/load.py
import logging

def load_data(sdf, config, jdbc_driver_path):
    if sdf is None or sdf.rdd.isEmpty():
        print("No data to load — DataFrame is empty.")
        return

    logging.info("Load: writing to PostgreSQL via JDBC")
    jdbc_url = f"jdbc:postgresql://{config['pg_host']}:{config['pg_port']}/{config['pg_db']}"
    sdf.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "daily_crypto_prices") \
        .option("user", config['pg_user']) \
        .option("password", config['pg_password']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    logging.info("Load: write complete")
