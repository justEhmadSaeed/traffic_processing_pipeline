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
   unset KAFKA_OPTS
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

### Prometheus and Grafana Setup

1.  Open http://localhost:9090/targets in your web browser to verify that Prometheus is scraping the metrics from the Kafka broker and Spark job.
2.  Ensure that the kafka1:7071 and host.docker.internal:8000 targets are up and running.
3.  You can query Kafka metrics by going to the "Graph" tab in Prometheus and entering the following queries:

    - `vehicle_count_total`

4.  Open Grafana in your web browser at `http://localhost:3000` (default: admin/admin).
5.  Configuration > Data Sources > Add Prometheus data source:

    - URL: ` http://prometheus:9090`
    - Click "Save & Test" to verify the connection.

6.  Create a New Dashboard and add visualization.
7.  Use the following queries to create visualizations:
    - `vehicle_count_total`

### Query Analysis Implementation Notes
#### Compute Real time traffic volume
For Computing real time traffic volume we grouped data by `sensor_id` in 5 minutes window. After that we used agg and sum functions to compute total vehicle count per sensor.
Relevant Docs:
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html

#### Detect Congestion Hotspots
For computing congestion hotspots, we used the lag function to go 1 and 2 steps behind, then we filtered those records where congestion_level remained high in all 3 intervals.
Relevant Docs:
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html

#### Calculate Avg Speed per sensor
For computing average speed per sensor we again took the help of agg, avg and groupBy methods but this time we used an overlapping window of 10 minutes with 5 minutes of overlap to get a better average.
Relevant Docs:
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.window.html

#### Identify Sudden speed drops
For identifying sudden speed drops we used lag function and compared speed at previous interval with speed at next interval. If the change in average_speed was above or equal to 0.5 we termed it as sudden speed drop.
Relevant Docs:
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html

#### Find the 3 Busiest Sensors
For identifying the busiest sensor we grouped data into 30 minutes window using timestamp and sensor_id and took sum of the vechicle count per sensor. After that we ordered the results in descending order and picked the top 3 results.
Relevant Docs:
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.orderBy.html
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.limit.html
