# Build a real-time data ingestion and processing pipeline for traffic events

The system will:

1. Ingest live traffic sensor data from different road sensors using Kafka.
2. Process the data in real-time using PySpark Structured Streaming.
3. Perform data quality validation to ensure that:
   - No missing or corrupted records exist.
   - The sensor data is within valid ranges.
   - Duplicates are handled properly.
4. Aggregate traffic patterns to analyze trends (e.g., sudden speed drops, high congestion)
5. Write the final processed data back to kafka topics for real-time dashboards.

## Project Setup

### Kafka Setup

1. Launch the docker containers with the following command:

   ```bash
   docker-compose -f docker-compose.yml up -d
   ```

2. Verify that the containers are running:

   ```bash
   docker-compose -f docker-compose.yml ps
   ```

3. Get inside the docker container running kafka broker and create kafka topics for traffic events and analysis:

   ```bash
   docker exec -it kafka1 bash
   ```

   ```bash
   kafka-topics --create --topic traffic_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   kafka-topics --create --topic traffic_analysis --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

4. Confirm the topics were created successfully:

   ```bash
   kafka-topics --list --bootstrap-server localhost:9092
   ```

5. Close the docker terminal and create a virtual environment and install the required dependencies:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

6. Run the producer to start sending traffic data to the Kafka topic:

   ```bash
   python producer.py
   ```

7. Run the consumer to start processing the traffic data:

   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 streaming.py
   ```

8. Monitor the output in the console to see the processed data.

### Grafana Setup

1. Connect to the Grafana container and install the plugin:

   ```bash
   docker exec -it grafana bash
   ```
2. Restart the Grafana container:

   ```bash
   docker restart grafana
   ```
1. Open Grafana in your web browser at `http://localhost:3000`.
1. Log in with the default credentials:
   - Username: `admin`
   - Password: `admin`
1. Add a new data source:
