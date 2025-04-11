import logging
import threading
from typing import Dict, List, Any
from queue import Queue

from src.data.vehicle_processor import VehicleProcessor

logger = logging.getLogger(__name__)

class GPSSimulator:
    """Responsible for simulating real-time GPS data with concurrent processing."""
    
    def __init__(self, kafka_service, config: Dict[str, Any]):
        """
        Initialize the GPS simulator.
        
        Args:
            kafka_service: Kafka service for sending messages
            config: Dictionary containing producer configuration
        """
        self.kafka_service = kafka_service
        self.config = config
        self.vehicle_processor = VehicleProcessor(kafka_service, config)
        self.threads = []
    
    def simulate(self, vehicle_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """
        Simulate real-time GPS data with concurrent processing.
        
        Args:
            vehicle_data: Dictionary with vehicle IDs as keys and lists of GPS data points as values
            
        Returns:
            Total number of messages sent
        """
        total_messages = 0
        
        try:
            # Start a thread for each vehicle
            for veh_id, data_points in vehicle_data.items():
                thread = threading.Thread(
                    target=self.vehicle_processor.process_vehicle,
                    args=(veh_id, data_points),
                    daemon=True
                )
                self.threads.append(thread)
                thread.start()
                logger.info(f"Started thread for vehicle {veh_id}")
            
            # Wait for all threads to complete
            for thread in self.threads:
                thread.join()
                
            # Collect results
            message_queue = self.vehicle_processor.get_message_queue()
            while not message_queue.empty():
                veh_id, count = message_queue.get()
                total_messages += count
                logger.info(f"Vehicle {veh_id} finished with {count} messages")
                
        except KeyboardInterrupt:
            logger.info("\nStopping all vehicle threads...")
            self.stop()
        finally:
            self.kafka_service.flush()
            logger.info(f"\nFinished! Total messages sent: {total_messages}")
            return total_messages
    
    def stop(self) -> None:
        """Stop all vehicle processing threads."""
        self.vehicle_processor.stop()
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=1.0) 