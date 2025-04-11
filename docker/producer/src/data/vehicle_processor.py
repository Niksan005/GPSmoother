import logging
import random
import time
from typing import Dict, List, Any, Callable
from queue import Queue

from src.data.gps_data_loader import GPSDataLoader

logger = logging.getLogger(__name__)

class VehicleProcessor:
    """Responsible for processing vehicle GPS data."""
    
    def __init__(self, kafka_service, config: Dict[str, Any]):
        """
        Initialize the vehicle processor.
        
        Args:
            kafka_service: Kafka service for sending messages
            config: Dictionary containing producer configuration
        """
        self.kafka_service = kafka_service
        self.config = config
        self.running = True
        self.message_queue = Queue()
    
    def process_vehicle(self, veh_id: str, data_points: List[Dict[str, Any]]) -> None:
        """
        Process data for a single vehicle.
        
        Args:
            veh_id: Vehicle ID
            data_points: List of GPS data points for the vehicle
        """
        message_count = 0
        
        try:
            # Process each data point once
            for point in data_points:
                if not self.running:
                    break
                    
                try:
                    # Add current timestamp
                    current_ts = int(time.time() * 1000)
                    point['current_ts'] = current_ts
                    point['formatted_ts'] = GPSDataLoader.format_timestamp(current_ts)
                    
                    # Send the GPS data to Kafka
                    self.kafka_service.send_message(point)
                    message_count += 1
                    
                    # Log every 10th message
                    if message_count % 10 == 0:
                        logger.info(f"Vehicle {veh_id}: Sent {message_count} messages. Latest at {point['formatted_ts']}")
                    
                    # Random delay between configured min and max
                    delay = random.uniform(
                        self.config['message_delay']['min'],
                        self.config['message_delay']['max']
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"Error sending data for vehicle {veh_id}: {e}")
            
            logger.info(f"Vehicle {veh_id}: Completed processing. Total messages: {message_count}")
            
        except Exception as e:
            logger.error(f"Thread error for vehicle {veh_id}: {e}")
        finally:
            self.message_queue.put((veh_id, message_count))
    
    def stop(self) -> None:
        """Stop processing vehicles."""
        self.running = False
    
    def get_message_queue(self) -> Queue:
        """Get the message queue for collecting results."""
        return self.message_queue 