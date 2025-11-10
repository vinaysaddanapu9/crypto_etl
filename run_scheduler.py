import schedule
import time
import subprocess
import logging

logging.basicConfig(
    filename="./logs/scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_etl():
    logging.info("Running ETL job...")

    # Use venv's Python path
    python_path = r"E:\PySpark_Projects\crypto_etl\venv\Scripts\python.exe"
    etl_script = r"E:\PySpark_Projects\crypto_etl\src\daily_etl.py"

    result = subprocess.run([python_path, etl_script], capture_output=True, text=True)

    if result.returncode == 0:
        logging.info("ETL completed successfully")
    else:
        logging.error(f"ETL failed: {result.stderr}")

# Runs every day at 10:00 AM
schedule.every().day.at("10:00").do(run_etl)

logging.info("Scheduler started...")
print("Scheduler running...")

while True:
    schedule.run_pending()
    time.sleep(10)