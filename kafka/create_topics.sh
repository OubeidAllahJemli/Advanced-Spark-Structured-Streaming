#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create topics
echo "Creating topics..."

# Topic for raw events from producer
docker exec kafka11 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic raw-events \
  --if-not-exists

# Topic for invalid events (optional - for routing invalid data)
docker exec kafka11 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic invalid-events \
  --if-not-exists

# Topic for processed/valid events output
docker exec kafka11 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic processed-events \
  --if-not-exists

echo "Topics created successfully!"
echo ""
echo "Listing topics:"
docker exec kafka11 kafka-topics --list --bootstrap-server localhost:9092
