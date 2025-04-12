import json
import logging
import socket
import time
from typing import Dict, Any, Callable

from kafka import KafkaProducer
from src.kafka.topic_manager import KafkaTopicManager

logger = logging.getLogger(__name__)

class KafkaProducerService:
    """Responsible for Kafka producer operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Kafka producer service.
        
        Args:
            config: Dictionary containing Kafka configuration
        """
        self.config = config
        self.producer = None
        self.topic = config['topic']
        self.topic_manager = KafkaTopicManager(config)
    
    def wait_for_kafka(self) -> bool:
        """
        Wait for Kafka to be available.
        
        Returns:
            True if Kafka is available, False otherwise
        """
        max_retries = self.config['retry']['max_attempts']
        retry_delay = self.config['retry']['delay_seconds']
        
        for attempt in range(max_retries):
            try:
                # Try to resolve the host
                host, port = self.config['bootstrap_servers'].split(':')
                socket.gethostbyname(host)
                
                # Try to connect to Kafka
                producer = self._create_producer()
                # Test the connection by sending a test message
                future = producer.send('test-topic', value={'test': 'connection'})
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
    
    def create_producer(self) -> None:
        """
        Create and store a Kafka producer.
        
        Raises:
            Exception: If producer creation fails
        """
        try:
            self.producer = self._create_producer()
            # Create admin client for topic management
            self.topic_manager.create_admin_client()
            logger.info("Successfully created Kafka producer and admin client")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def _create_producer(self) -> KafkaProducer:
        """
        Create a Kafka producer with the configured settings.
        
        Returns:
            KafkaProducer instance
        """
        return KafkaProducer(
            bootstrap_servers=self.config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=tuple(self.config['api_version'])
        )
    
    def add_partitions(self, num_partitions: int) -> None:
        """
        Add partitions to the topic.
        
        Args:
            num_partitions: Number of partitions to add
            
        Raises:
            Exception: If partition addition fails
        """
        self.topic_manager.add_partitions(self.topic, num_partitions)
    
    def get_partition_count(self) -> int:
        """
        Get the current number of partitions for the topic.
        
        Returns:
            Number of partitions
        """
        return self.topic_manager.get_partition_count(self.topic)
    
    def send_message(self, message: Dict[str, Any]) -> None:
        """
        Send a message to the configured Kafka topic.
        
        Args:
            message: Dictionary containing the message to send
        """
        if not self.producer:
            raise RuntimeError("Kafka producer not initialized")
        
        try:
            # Get the vehicle ID from the message
            veh_id = message.get('veh_id')
            if not veh_id:
                raise ValueError("Message must contain 'veh_id' field")
            
            # Generate consistent hash key (0 to 2^32-1)
            hash_key = hash(veh_id) % (2**32)
            
            # Send message with hashed key for partition assignment
            self.producer.send(
                topic=self.topic,
                value=message,
                key=str(hash_key).encode('utf-8')  # Convert hash to string and encode
            )
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            raise
    
    def flush(self) -> None:
        """Flush any pending messages to Kafka."""
        if self.producer:
            self.producer.flush()
    
    def close(self) -> None:
        """Close the Kafka producer and admin client."""
        if self.producer:
            self.producer.close()
            self.producer = None
        self.topic_manager.close() 