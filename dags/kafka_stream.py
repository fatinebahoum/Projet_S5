from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2021, 1, 13, 10, 00)
}

def get_data(api_key):
    import requests

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': api_key,
    }

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        bitcoin_data = next((coin for coin in data.get('data', []) if coin.get('symbol') == 'BTC'), None)
        
        if bitcoin_data:
            return bitcoin_data
        else:
            print("Bitcoin data not found in the response.")
            return None
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def format_data(bitcoin_data):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['name'] = bitcoin_data['name']
    data['symbol'] = bitcoin_data['symbol']
    data['max_supply'] = bitcoin_data['max_supply']
    data['circulating_supply'] = bitcoin_data['circulating_supply']
    data['total_supply'] = bitcoin_data['total_supply']
    data['percent_change_1h'] = bitcoin_data["quote"]["USD"]["percent_change_1h"]
    data['percent_change_24h'] = bitcoin_data["quote"]["USD"]["percent_change_24h"]
    data['percent_change_7d'] = bitcoin_data["quote"]["USD"]["percent_change_7d"]
    data['market_cap'] = bitcoin_data["quote"]["USD"]["market_cap"]
    data['volume_24h'] = bitcoin_data["quote"]["USD"]["volume_24h"]
    data['last_updated'] = bitcoin_data['last_updated']
    
    return data
    
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    api_key = 'a248bb66-1db0-478a-8fea-5bc4e6eff197'  # Replace with your actual API key
    
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            bitcoin_data = get_data(api_key)
            if bitcoin_data:
                data = format_data(bitcoin_data)
                producer.send('bitcoin_data', json.dumps(data).encode('utf-8'))
                #print("Bitcoin Information:")
                #print(json.dumps(data, indent=3))
                time.sleep(5)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('data_automation',
          default_args= default_args,
          schedule_interval=timedelta(minutes=1),
          catchup=False) as dag:

    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable= stream_data
    )