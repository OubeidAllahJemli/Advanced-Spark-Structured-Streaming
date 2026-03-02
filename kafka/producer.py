"""
Kafka Producer for streaming dirty JSON events.

This producer reads events from the dirty JSON file and sends them
to Kafka one by one, simulating a real-time stream.
"""

import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
import sys

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'raw-events'
DATA_FILE = '../data/events_dirty.json'
DELAY_BETWEEN_MESSAGES = 0.1  # seconds


def create_producer():
    """Create and configure Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        sys.exit(1)


def send_events(producer, data_file, topic):
    """Read events from file and send to Kafka."""
    # Get absolute path to data file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, data_file)
    
    if not os.path.exists(file_path):
        print(f"✗ Data file not found: {file_path}")
        sys.exit(1)
    
    print(f"✓ Reading events from: {file_path}")
    print(f"✓ Sending to topic: {topic}")
    print("-" * 60)
    
    sent_count = 0
    error_count = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                if not line:
                    continue
                
                try:
                    # Send the raw line as-is to Kafka
                    # (including invalid JSON - Spark will handle it)
                    future = producer.send(topic, value=line)
                    
                    # Wait for send to complete (with timeout)
                    future.get(timeout=10)
                    
                    sent_count += 1
                    
                    # Print progress every 100 messages
                    if sent_count % 100 == 0:
                        print(f"Sent {sent_count} messages...")
                    
                    # Simulate streaming delay
                    time.sleep(DELAY_BETWEEN_MESSAGES)
                    
                except KafkaError as e:
                    error_count += 1
                    print(f"✗ Failed to send line {line_num}: {e}")
                
    except KeyboardInterrupt:
        print("\n\n⚠ Producer interrupted by user")
    except Exception as e:
        print(f"\n✗ Error reading file: {e}")
    finally:
        # Flush and close producer
        producer.flush()
        producer.close()
        
        print("-" * 60)
        print(f"✓ Producer finished")
        print(f"  Total sent: {sent_count}")
        print(f"  Errors: {error_count}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Kafka Producer - Streaming Dirty Events")
    print("=" * 60)
    print()
    
    # Create producer
    producer = create_producer()
    
    # Send events
    send_events(producer, DATA_FILE, KAFKA_TOPIC)
    
    print("\n✓ All done!")


if __name__ == "__main__":
    main()
