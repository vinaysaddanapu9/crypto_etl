# src/dashboard.py
import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import json
import os


# --- Get absolute path for config.json ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.json")

# --- Load DB config from your config.json ---
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# --- Database connection ---
def get_connection():
    return psycopg2.connect(
        host=config["pg_host"],
        port=config["pg_port"],
        database=config["pg_db"],
        user=config["pg_user"],
        password=config["pg_password"]
    )

# --- Query helpers ---
def load_latest_data():
    query = """
        SELECT crypto_id, name, symbol, price_usd, effective_date
        FROM daily_crypto_prices
        WHERE is_current = TRUE
        ORDER BY price_usd DESC;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def load_run_metrics():
    query = """
        SELECT COUNT(*) AS total_records,
               SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS active_records,
               SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historical_records,
               MAX(effective_date) AS last_update
        FROM daily_crypto_prices;
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df.iloc[0]

# --- Streamlit Layout ---
st.set_page_config(page_title="Crypto ETL Dashboard", layout="wide")

st.title("CoinGecko ETL Dashboard")
st.markdown("Monitor ETL data freshness, SCD2 changes, and crypto price trends.")

# --- Metrics ---
metrics = load_run_metrics()
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Records", f"{metrics['total_records']:,}")
col2.metric("Active Records", f"{metrics['active_records']:,}")
col3.metric("Historical Records", f"{metrics['historical_records']:,}")
col4.metric("Last Update", metrics['last_update'].strftime("%Y-%m-%d %H:%M"))

# --- Data Table ---
st.subheader("Latest Crypto Prices (Active Records)")
latest_df = load_latest_data()
st.dataframe(latest_df, use_container_width=True)

# --- Trend Chart ---
st.subheader("Price Trend Analysis")
selected = st.selectbox("Select crypto to view trend:", latest_df["name"].unique())

query_chart = f"""
    SELECT name, price_usd, effective_date
    FROM daily_crypto_prices
    WHERE name = '{selected}'
    ORDER BY effective_date;
"""
conn = get_connection()
chart_df = pd.read_sql(query_chart, conn)
conn.close()

fig = px.line(chart_df, x="effective_date", y="price_usd", title=f"{selected} Price Trend")
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.caption("ETL Dashboard powered by PySpark, PostgreSQL, and Streamlit 🚀")
