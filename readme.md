This project deals with creating a csv file using apache airflow that will be used as producer of message into kafka and will be consumed by kafka to store in a different location.


1) Create a project folder with Airflow_kafka_integration
2) Create dags/logs/raw_data/processed_data/config/plugin folders and check if the volume is proper in docker compose file. 
3) Place kafka_csv_dag.py dag file in dags folder.
4) use docker-compose.yaml file to spin below dockers services with apache kafka and airflow

**run command: docker compose up -d**

<img width="1492" alt="image" src="https://github.com/user-attachments/assets/ed29177e-7587-4050-9a50-3dcb90c36c0c">

5) An error w.r.t kafka package will occur on Airflow console, we will need to install kafka-python and pandas packages manually in each airflow web server, scheduler, worker containers using airflow user. 

Example:

**docker exec -it 54256ab3aa0e bash
pip install pandas kafka-python**

Note: Step 5 is to be done if requirement.txt file doesn't work.

6) An error w.r.t six package will occur on airflow web server. In this case install vim editor on  web server, scheduler, worker containers using root user and exit out.

Example:

**docker exec -it -u root 54256ab3aa0e bash**

**apt-get update **
**apt-get install -y vim**


Once installation is done on all 3 airflow containers using root, go to cd /home/airflow/.local/lib/python3.12/site-packages/kafka/ using airflow user one by one on web server, scheduler, worker containers and make below changes.

Example:
**docker exec -it 54256ab3aa0e bash**
**vi /home/airflow/.local/lib/python3.12/site-packages/kafka/codec.py**


Old:
from **kafka.vendor**.six.moves import range

New:
from six.moves import range

7) Create a topic on kafka container - **airflow_kafka_integration-kafka-1** using below command in interactive mode:

Open interactive mode:
**docker exec -it d91636359ec2 /bin/bash**

Create a topic:
**/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic csv_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1**


To check the topic:
**kafka-topics.sh --list --bootstrap-server localhost:9092**

8) Login to airflow on web using airflow/airflow as user name and password. Run the dag kafka_csv_workflow which has three tasks -- create_csv,produce_messages,consume_messages


[2024-11-18, 11:36:32 UTC] {conn.py:380} INFO - <BrokerConnection node_id=1 host=localhost:9092 <connecting> [IPv6 ('::1', 9092, 0, 0)]>: connecting to localhost:9092 [('::1', 9092, 0, 0) IPv6]
[2024-11-18, 11:36:32 UTC] {conn.py:418} ERROR - Connect attempt to <BrokerConnection node_id=1 host=localhost:9092 <connecting> [IPv6 ('::1', 9092, 0, 0)]> returned error 111. Disconnecting.
[2024-11-18, 11:36:32 UTC] {conn.py:919} INFO - <BrokerConnection node_id=1 host=localhost:9092 <connecting> [IPv6 ('::1', 9092, 0, 0)]>: Closing connection. KafkaConnectionError: 111 ECONNREFUSED

If the consumer has above error, copy the name of kafka container and paste in docker-compose.yaml line

Old:

      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092

New:
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://airflow_kafka_integration-kafka-1:9092


Also, in dags folder - change localhost in your .py file kafka_csv_dag.py to use the kafka service name

Old:
bootstrap_servers=['localhost:9092']

New:
bootstrap_servers=['airflow_kafka_integration-kafka-1:9092']
