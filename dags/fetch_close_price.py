from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid

default_args = {
    'owner': 'fatine',
    'start_date': datetime(2021, 12, 20, 10, 00)
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
    data['name'] = bitcoin_data['name']
    data['price'] = bitcoin_data["quote"]["USD"]["price"]
    data['date'] = bitcoin_data['last_updated']
    
    return data

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.historical_data (
            name TEXT,
            price TEXT,
            date TEXT,
            PRIMARY KEY (name, date)
        );
    """)

    print("Table created successfully!")

  
def fetch_and_save_data():
    import json
    import time
    import logging
    from cassandra.cluster import Cluster

    api_key = 'a248bb66-1db0-478a-8fea-5bc4e6eff197'  # Replace with your actual API key
    

    # Connexion à Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect()

    create_keyspace(session)
    create_table(session)

    try:
        historical_data = get_data(api_key)
        if historical_data:
            data = format_data(historical_data)
            data['price']=str(data['price'])
            
            # Insertion des données dans Cassandra
            try:
                session.execute("""
                    INSERT INTO spark_streams.historical_data(name, price, date)
                    VALUES (%s, %s, %s)
                """, (data['name'], data['price'], data['date']))

                logging.info(f"Inserted historical data into Cassandra")

            except Exception as cassandra_error:
                logging.error(f'Error inserting data into Cassandra: {cassandra_error}')
            
    except Exception as e:
        logging.error(f'An error occured: {e}')


dag = DAG('fetch_and_save_historical_data',
          default_args=default_args,
          schedule_interval='57 23 * * *',
          catchup=False)

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_hitstorical_data',
    python_callable=fetch_and_save_data,
    dag=dag  # Specify the DAG
)