@echo off
cd E:\PySpark_Projects\crypto_etl
call venv\Scripts\activate
python src\daily_etl.py
deactivate
