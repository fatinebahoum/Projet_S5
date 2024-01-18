from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import logging


def load_data_from_cassandra(spark, keyspace, table):
    # Chargez les données depuis Cassandra
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()
    return df

def predict_next_day(data, model, scaler, time_step=15):
    last_days = last_days.reshape(1, time_step, 1)
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
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def batch_processing():
    # create spark connection
    spark_conn = create_spark_connection()

    if(spark_conn):
        # Charger les données historiques depuis Cassandra
        historical_data = load_data_from_cassandra(spark_conn, "spark_streams", "historical_data")
        if not historical_data.isEmpty():

            # Trier les données par last_updated
            historical_data = historical_data.sort(col("last_updated"))

            # Utiliser une fenêtre pour obtenir les 15 dernières lignes
            window_spec = Window.orderBy(col("last_updated").desc())
            last_15_rows = historical_data.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") <= 15)

            # Sélectionner uniquement la colonne "price"
            last_15_prices = last_15_rows.select("price").collect()

            print(last_15_prices)

            # Convertir les données en un tableau NumPy
            prices_array = [float(row.price) for row in last_15_prices]
            print(prices_array)
            """
            # Charger le model LSTM
            model_path = './complete_model.h5'
            if(model_path):
                loaded_model = load_model(model_path)
                # Charger le scaler utilisé lors de l'entraînement du modèle
                scaler = MinMaxScaler()
                print("loaded_model succesfully")
            else: print("model path error")"""
        else:
            print("historical data error")
    else: print("spark connection error")


if __name__ == "__main__":
    batch_processing()
