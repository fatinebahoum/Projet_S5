from cassandra.cluster import Cluster
import csv

# Connect to Cassandra
cluster = Cluster(['localhost'])
session = cluster.connect('spark_streams')  # Replace 'your_cassandra_host' with your Cassandra host

# Path to your CSV file
csv_file_path = './historical_data.csv'

# Cassandra table columns
columns = ['id', 'name', 'price', 'last_updated']

# Prepare INSERT statement
insert_query = f"INSERT INTO historical_data ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

# Read data from CSV and insert into Cassandra
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Skip header row
    for row in csv_reader:
        session.execute(insert_query, tuple(row))

# Close the Cassandra connection
cluster.shutdown()
