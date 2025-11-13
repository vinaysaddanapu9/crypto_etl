# src/optimize.py
import psycopg2
import logging

def create_indexes(config):
    try:
        conn = psycopg2.connect(
            host=config['pg_host'],
            port=config['pg_port'],
            dbname=config['pg_db'],
            user=config['pg_user'],
            password=config['pg_password']
        )
        cur = conn.cursor()

        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_effective_unique
        ON daily_crypto_prices (crypto_id, effective_date);
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_crypto_iscurrent
        ON daily_crypto_prices (crypto_id)
        WHERE is_current = TRUE;
        """)

        conn.commit()
        cur.close()
        conn.close()
        logging.info("Post-load: Indexes created successfully.")
    except Exception as e:
        logging.error(f"Failed to create indexes: {e}")
