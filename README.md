# Stock-Prices-Daily-Report-And-Weekly-Predictions
This project is a fully automated data pipeline built with Apache Airflow that collects, stores, analyzes, and predicts stock market behavior. It consists of two coordinated DAGs — one for daily reporting and another for weekly machine learning predictions.

Overview
This project automates the process of collecting, storing, and analyzing stock price data using Apache Airflow.
It performs two main workflows (DAGs):
Daily Report DAG
Fetches real-time stock price data from the Alpha Vantage API.
Saves the data into a PostgreSQL database.
Generates a Word report summarizing the daily market performance.
Weekly Prediction DAG
Uses historical data stored in PostgreSQL.
Trains a Random Forest Classifier (sklearn) model.
Predicts whether the first 15–30 minutes of Monday trading are likely to be bullish 📈 or bearish 📉.
⚙️ Tech Stack
Apache Airflow – Workflow orchestration
Python – Data processing & ML model
PostgreSQL – Data storage
Alpha Vantage API – Real-time market data
scikit-learn – Machine learning
pandas, requests, dotenv, io – Data wrangling & API handling
python-docx – Report generation

Setup Instructions
Clone the repository
git clone https://github.com/GugaGagloshvili060/stock-weekly-report.git
cd stock-weekly-report

Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # on Mac/Linux
venv\Scripts\activate     # on Windows


Install dependencies
pip install -r requirements.txt


Create your api key from here: https://www.alphavantage.co/support/#api-key
