import csv
import json
import time
from kafka import KafkaProducer
import os
from datetime import datetime
import random
from collections import defaultdict
import logging
import socket
import yaml
from pathlib import Path
import threading
from queue import Queue
import signal

def load_config():
    """Load configuration from YAML file"""
    config_path = Path('/app/config/config.yaml')
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        raise

# Load configuration
config = load_config()

# Configure logging
logging.basicConfig(
    level=getattr(logging, config['producer']['logging']['level']),
    format=config['producer']['logging']['format']
)
logger = logging.getLogger(__name__)

# Kafka configuration from config
KAFKA_CONFIG = config['kafka']
PRODUCER_CONFIG = config['producer']

# Global flag for graceful shutdown
running = True

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Received shutdown signal")
    running = False

def wait_for_kafka():
    """Wait for Kafka to be available"""
    max_retries = KAFKA_CONFIG['retry']['max_attempts']
    retry_delay = KAFKA_CONFIG['retry']['delay_seconds']
    
    for attempt in range(max_retries):
        try:
            # Try to resolve the host
            host, port = KAFKA_CONFIG['bootstrap_servers'].split(':')
            socket.gethostbyname(host)
            
            # Try to connect to Kafka
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=tuple(KAFKA_CONFIG['api_version'])
            )
            # Test the connection by sending a test message
            future = producer.send(KAFKA_CONFIG['topic'], value={'test': 'connection'})
            future.get(timeout=10)  # Wait for the message to be sent
            producer.close()
            logger.info("Successfully connected to Kafka")
            return True
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Could not connect to Kafka.")
                return False

def create_producer():
    """Create Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=tuple(KAFKA_CONFIG['api_version'])
        )
        logger.info("Successfully created Kafka producer")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def validate_gps_data(row):
    """Validate required GPS fields"""
    required_fields = ['lat', 'lon', 'ts', 'veh_id']
    for field in required_fields:
        if field not in row or not row[field]:
            return False, f"Missing required field: {field}"
    
    try:
        float(row['lat'])
        float(row['lon'])
        int(row['ts'])
    except ValueError:
        return False, "Invalid numeric values"
    
    return True, None

def format_timestamp(ts):
    """Convert Unix timestamp to readable format"""
    try:
        return datetime.fromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ts

def read_gps_data(file_path):
    """Read and group GPS data by vehicle"""
    vehicle_data = defaultdict(list)
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                is_valid, _ = validate_gps_data(row)
                if is_valid:
                    vehicle_data[row['veh_id']].append(row)
        logger.info(f"Successfully loaded data for {len(vehicle_data)} vehicles")
        return vehicle_data
    except Exception as e:
        logger.error(f"Error reading GPS data: {e}")
        raise

def process_vehicle(veh_id, data_points, producer, message_queue):
    """Process data for a single vehicle in a separate thread"""
    global running
    message_count = 0
    
    try:
        while running:
            for point in data_points:
                if not running:
                    break
                    
                try:
                    # Add current timestamp
                    current_ts = int(time.time() * 1000)
                    point['current_ts'] = current_ts
                    point['formatted_ts'] = format_timestamp(current_ts)
                    
                    # Send the GPS data to Kafka
                    producer.send(KAFKA_CONFIG['topic'], value=point)
                    message_count += 1
                    
                    # Log every 10th message
                    if message_count % 10 == 0:
                        logger.info(f"Vehicle {veh_id}: Sent {message_count} messages. Latest at {point['formatted_ts']}")
                    
                    # Random delay between configured min and max
                    delay = random.uniform(
                        PRODUCER_CONFIG['message_delay']['min'],
                        PRODUCER_CONFIG['message_delay']['max']
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"Error sending data for vehicle {veh_id}: {e}")
            
            logger.info(f"Vehicle {veh_id}: Completed one cycle. Total messages: {message_count}")
            
    except Exception as e:
        logger.error(f"Thread error for vehicle {veh_id}: {e}")
    finally:
        message_queue.put((veh_id, message_count))

def simulate_real_time(producer, vehicle_data):
    """Simulate real-time GPS data with concurrent processing"""
    threads = []
    message_queue = Queue()
    total_messages = 0
    
    try:
        # Start a thread for each vehicle
        for veh_id, data_points in vehicle_data.items():
            thread = threading.Thread(
                target=process_vehicle,
                args=(veh_id, data_points, producer, message_queue),
                daemon=True
            )
            threads.append(thread)
            thread.start()
            logger.info(f"Started thread for vehicle {veh_id}")
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            
        # Collect results
        while not message_queue.empty():
            veh_id, count = message_queue.get()
            total_messages += count
            logger.info(f"Vehicle {veh_id} finished with {count} messages")
            
    except KeyboardInterrupt:
        logger.info("\nStopping all vehicle threads...")
    finally:
        producer.flush()
        logger.info(f"\nFinished! Total messages sent: {total_messages}")

def main():
    try:
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Wait for Kafka to be available
        if not wait_for_kafka():
            raise Exception("Could not connect to Kafka after multiple attempts")
        
        # Create producer and start sending data
        producer = create_producer()
        vehicle_data = read_gps_data(PRODUCER_CONFIG['csv_file'])
        simulate_real_time(producer, vehicle_data)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main() 