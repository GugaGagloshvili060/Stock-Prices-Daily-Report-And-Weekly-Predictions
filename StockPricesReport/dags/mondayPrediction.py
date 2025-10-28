from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from io import StringIO
from sklearn.ensemble import RandomForestClassifier
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('ALPHAVANTAGE_API_KEY')

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=180)
}

with DAG(
    dag_id='weeklyReport',
    default_args=default_args,
    schedule='0 18 * * 6',  # every Saturday 6 PM
    catchup=False
) as dag:

    @task()
    def extract_stock_prices_data():
        url = "https://www.alphavantage.co/query"
        params = {
            "function": "TIME_SERIES_INTRADAY",
            'symbol': 'IBM',
            "interval": "60min",
            'outputsize': 'compact',
            'datatype': 'csv',
            'apikey': api_key
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            df.columns = [c.lower() for c in df.columns]
            return df
        else:
            raise Exception("Failed to fetch data from AlphaVantage")

    @task()
    def predict_data(df: pd.DataFrame):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['date'] = df['timestamp'].dt.date

        df['daily_return_pct'] = (df['close'] - df['open']) * 100 / df['open']
        df['volatility'] = (df['high'] - df['low']) / df['close']
        df['5d_avg_close'] = df['close'].rolling(5).mean()
        df['5d_std_close'] = df['close'].rolling(5).std()
        df = df.dropna()

        daily_first_hour = (
            df[df['hour'] == 4]
            .groupby('date')
            .agg(open_first=('open', 'first'), close_first=('close', 'last'))
            .reset_index()
        )
        daily_first_hour['target'] = (
            daily_first_hour['close_first'] > daily_first_hour['open_first']
        ).astype(int)

        # Merge back to match daily labels with hourly features
        df = df.merge(daily_first_hour[['date', 'target']], on='date', how='left')
        df = df.dropna(subset=['target'])

        # Features for ML
        features = [
            'open', 'high', 'low', 'close', 'volume',
            'daily_return_pct', 'volatility', '5d_avg_close', '5d_std_close'
        ]
        X = df[features]
        y = df['target']

        # Train model
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X, y)

        # Predict next candle
        latest = df[features].iloc[[-1]]
        prediction = model.predict(latest)[0]

        result = "📈 Bullish first hour expected" if prediction == 1 else "📉 Bearish first hour expected"
        print(result)

    # DAG flow
    raw_data = extract_stock_prices_data()
    prediction = predict_data(raw_data)
