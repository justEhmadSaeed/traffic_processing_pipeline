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

1. Launch the docker containers with the following command:

   ```bash
   docker-compose -f docker-compose.yml up -d
   ```

2. Verify that the containers are running:

   ```bash
   docker-compose -f docker-compose.yml ps
   ```

3. Get inside the docker container running kafka broker and create a kafka topic for traffic events:

   ```bash
   docker exec -it kafka1 bash
   ```

   ```bash
   kafka-topics --create --topic traffic_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
