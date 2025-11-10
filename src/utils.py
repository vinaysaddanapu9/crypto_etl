import logging
from dotenv import load_dotenv
from datetime import datetime
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
