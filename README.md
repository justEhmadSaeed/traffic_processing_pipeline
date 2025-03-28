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
