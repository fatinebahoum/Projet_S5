import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import uuid

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.bitcoin_data (
        id UUID PRIMARY KEY,
        name TEXT,
        symbol TEXT,
        max_supply TEXT,
        circulating_supply TEXT,
        total_supply TEXT,
        percent_change_1h TEXT,
        percent_change_24h TEXT,
        percent_change_7d TEXT,
        market_cap TEXT,
        volume_24h TEXT,
        last_updated TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    print(kwargs.get('id'))
    bitcoin_id = kwargs.get('id')
    if(bitcoin_id):
        name = kwargs.get('name')
        symbol = kwargs.get('symbol')
        max_supply = kwargs.get('max_supply')
        circulating_supply = kwargs.get('circulating_supply')
        total_supply = kwargs.get('total_supply')
        percent_change_1h = kwargs.get('percent_change_1h')
        percent_change_24h = kwargs.get('percent_change_24h')
        percent_change_7d = kwargs.get('percent_change_7d')
        market_cap = kwargs.get('market_cap')
        volume_24h = kwargs.get('volume_24h')
        last_updated = kwargs.get('last_updated')
    else: print("aaaaaaaaaaaaaaaaaaaaa")

    try:
        session.execute("""
            INSERT INTO spark_streams.bitcoin_data(id, name, symbol, max_supply, circulating_supply, 
                total_supply, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, volume_24h, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (bitcoin_id, name, symbol, max_supply, circulating_supply, 
                total_supply, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, volume_24h, last_updated))
        logging.info(f"Data inserted for {name} {last_updated}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


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

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'bitcoin_data') \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
        raise

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("max_supply", StringType(), False),
        StructField("circulating_supply", StringType(), False),
        StructField("total_supply", StringType(), False),
        StructField("percent_change_1h", StringType(), False),
        StructField("percent_change_24h", StringType(), False),
        StructField("percent_change_7d", StringType(), False),
        StructField("market_cap", StringType(), False),
        StructField("volume_24h", StringType(), False),
        StructField("last_updated", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()
    

    if spark_conn is not None:

        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        print(spark_df)

        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            

            logging.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'bitcoin_data')
                               .option("failOnDataLoss", "false")
                               .start())

            try:
                print("Awaiting termination...")
                streaming_query.awaitTermination()
                print("Streaming terminated.")

            except Exception as e:
                logging.error(f"An error occurred while waiting for termination: {e}")

            finally:
                # Arrêter le streaming de manière explicite
                streaming_query.stop()
                print("Streaming arrêté manuellement.")