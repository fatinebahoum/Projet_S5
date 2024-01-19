from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import logging
import numpy as np


def load_data_from_cassandra(spark, keyspace, table):
    # Chargez les données depuis Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df

def predict_next_day(data, model, scaler, time_step=15):
    last_days = data.reshape(1, time_step, 1)
    prediction = model.predict(last_days)
    prediction = scaler.inverse_transform(prediction)
    return prediction[0][0]

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.predictions (
            name TEXT,
            price TEXT,
            PRIMARY KEY (name, price)
        );
    """)

    print("Table created successfully!")


def batch_processing():
    from cassandra.cluster import Cluster
    # create spark connection
    spark_conn = create_spark_connection()

    # Connexion à Cassandra
    cluster = Cluster(['cassandra'])
    session = cluster.connect()

    create_keyspace(session)
    create_table(session)

    if(spark_conn):
        # Charger les données historiques depuis Cassandra
        print("Charger les données historiques depuis Cassandra")
        historical_data = load_data_from_cassandra(spark_conn, "spark_streams", "historical_data")
        print("historical_data : ",historical_data)
        if not historical_data.isEmpty():

            # Trier les données par last_updated
            print("Trier les données par last_updated")
            historical_data = historical_data.sort(col("date"))

            # Utiliser une fenêtre pour obtenir les 15 dernières lignes
            print("Utiliser une fenêtre pour obtenir les 15 dernières lignes")
            window_spec = Window.orderBy(col("date").desc())
            last_15_rows = historical_data.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") <= 15)


            # Sélectionner uniquement la colonne "price"
            print("Sélectionner uniquement la colonne price")
            last_15_prices = last_15_rows.select("price").collect()

            last_15_prices.reverse()

            print(last_15_prices)

            # Convertir les données en un tableau NumPy
            print("Convertir les données en un tableau NumPy")
            prices_array = [float(row.price) for row in last_15_prices]
            print(prices_array)
            
            # Charger le model LSTM
            model_path = '/opt/airflow/plugins/complete_model.h5'
            print("model_path : ",model_path)

            if(model_path):
                loaded_model = load_model(model_path)

                #Charger le scaler utilisé lors de l'entraînement du modèle
                scaler = MinMaxScaler(feature_range=(0,1))
                prices_array1 = scaler.fit_transform(np.array(prices_array).reshape(-1, 1))
                print(prices_array1)
                # Faire la prédiction pour le prochain jour
                next_day_prediction = predict_next_day(prices_array1, loaded_model, scaler)
                
                next_day_prediction = str(next_day_prediction)
                # Insertion des données dans Cassandra
                try:
                    session.execute("""
                        INSERT INTO spark_streams.predictions(name, price)
                        VALUES (%s, %s)
                    """, ('Bitcoin', next_day_prediction))

                    logging.info(f"Inserted predicted value into Cassandra")

                except Exception as cassandra_error:
                    logging.error(f'Error inserting predicted value into Cassandra: {cassandra_error}')
                
                print(f"Predicted Close Price for the Next Day: {next_day_prediction}")
                
                
            else: print("model path error")
        else:
            print("historical data error")
    else: print("spark connection error")


if __name__ == "__main__":
    batch_processing()
