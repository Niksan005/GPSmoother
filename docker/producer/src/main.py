import logging
import sys
from typing import Dict, Any

from src.config.config_loader import ConfigLoader
from src.data.gps_data_loader import GPSDataLoader
from src.kafka.kafka_producer import KafkaProducerService
from src.data.simulator import GPSSimulator
from src.utils.signal_handler import SignalHandler

logger = logging.getLogger(__name__)

class GPSProducerApp:
    """Main application class for the GPS Producer."""
    
    def __init__(self):
        """Initialize the GPS Producer application."""
        self.config_loader = ConfigLoader()
        self.kafka_config = self.config_loader.get_kafka_config()
        self.producer_config = self.config_loader.get_producer_config()
        self.kafka_service = KafkaProducerService(self.kafka_config)
        self.simulator = None
        self.signal_handler = SignalHandler(self.stop)
    
    def run(self) -> None:
        """
        Run the GPS Producer application.
        
        Raises:
            Exception: If there's an error running the application
        """
        try:
            # Set up signal handlers for graceful shutdown
            self.signal_handler.setup()
            
            # Wait for Kafka to be available
            if not self.kafka_service.wait_for_kafka():
                raise Exception("Could not connect to Kafka after multiple attempts")
            
            # Create Kafka producer
            self.kafka_service.create_producer()
            
            # Load GPS data
            csv_file_path = self.config_loader.get_csv_file_path()
            gps_loader = GPSDataLoader(csv_file_path)
            vehicle_data = gps_loader.load_data()
            
            # Create and run simulator
            self.simulator = GPSSimulator(self.kafka_service, self.producer_config)
            self.simulator.simulate(vehicle_data)
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            self.cleanup()
    
    def stop(self) -> None:
        """Stop the application gracefully."""
        if self.simulator:
            self.simulator.stop()
    
    def cleanup(self) -> None:
        """Clean up resources."""
        if self.kafka_service:
            self.kafka_service.close()


def main():
    """Entry point for the GPS Producer application."""
    app = GPSProducerApp()
    app.run()


if __name__ == "__main__":
    main() 