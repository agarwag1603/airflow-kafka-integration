from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import csv
import time

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'kafka_csv_workflow_2',
    default_args=default_args,
    schedule_interval=None,
)

# Task 1: Create a CSV file
def create_csv():
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    }
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/raw_data/sample_data.csv', index=False)

create_csv_task = PythonOperator(
    task_id='create_csv',
    python_callable=create_csv,
    dag=dag,
)

# Task 2: Produce Kafka messages from the CSV

def produce_messages():
    producer = KafkaProducer(bootstrap_servers=['airflow_kafka_integration-kafka-1:9092'])
    with open('/opt/airflow/raw_data/sample_data.csv', 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            producer.send('csv_topic', value=','.join(row).encode())
            time.sleep(1)  # Give Kafka time to process messages
    producer.flush()

produce_task = PythonOperator(
    task_id='produce_messages',
    python_callable=produce_messages,
    dag=dag,
)

# Task 3: Consume Kafka messages and write them to a new CSV
def consume_messages():
    consumer = KafkaConsumer(
        'csv_topic',
        bootstrap_servers=['airflow_kafka_integration-kafka-1:9092'],
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000
    )
    with open('/opt/airflow/processed_data/consumed_data.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        for message in consumer:
            writer.writerow(message.value.decode().split(','))

consume_task = PythonOperator(
    task_id='consume_messages',
    python_callable=consume_messages,
    dag=dag,
)

# Set the task sequence
create_csv_task >> produce_task >> consume_task
