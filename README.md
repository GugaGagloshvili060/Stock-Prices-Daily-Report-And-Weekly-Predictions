# Stock-Prices-Daily-Report-And-Weekly-Predictions

This project is a fully automated data pipeline built with Apache Airflow that collects, stores, analyzes, and predicts stock market behavior. It consists of two coordinated DAGs — one for daily reporting and another for weekly machine learning predictions.

## Overview

This project automates the process of collecting, storing, and analyzing stock price data using Apache Airflow. It performs two main workflows (DAGs):

### Daily Report DAG
- Fetches real-time stock price data from the Alpha Vantage API
- Saves the data into a PostgreSQL database
- Generates a Word report summarizing the daily market performance

### Weekly Prediction DAG
- Uses historical data stored in PostgreSQL
- Trains a Random Forest Classifier (sklearn) model
- Predicts whether the first 15–30 minutes of Monday trading are likely to be bullish 📈 or bearish 📉

## Tech Stack

- **Apache Airflow** – Workflow orchestration
- **Python** – Data processing & ML model
- **PostgreSQL** – Data storage
- **Alpha Vantage API** – Real-time market data
- **scikit-learn** – Machine learning
- **pandas, requests, dotenv, io** – Data wrangling & API handling
- **python-docx** – Report generation

## Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/GugaGagloshvili060/stock-weekly-report.git
cd stock-weekly-report
```

### 2. Create and activate a virtual environment

**On Mac/Linux:**
```bash
python -m venv venv
source venv/bin/activate
```

**On Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Create your API key

Get your Alpha Vantage API key from here: [https://www.alphavantage.co/support/#api-key](https://www.alphavantage.co/support/#api-key)

---

## Usage

1. Configure your Alpha Vantage API key in your environment variables
2. Set up your PostgreSQL database connection
3. Start Apache Airflow and enable the DAGs
4. Monitor the daily reports and weekly predictions

## Features

- ⚡ Automated daily stock data collection
- 📊 PostgreSQL database integration
- 📝 Automated Word report generation
- 🤖 Machine learning predictions for Monday market opening
- 🔄 Fully orchestrated with Apache Airflow

