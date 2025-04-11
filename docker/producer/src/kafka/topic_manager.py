import logging
from typing import Dict, Any, List
from kafka.admin import KafkaAdminClient, NewPartitions

logger = logging.getLogger(__name__)

class KafkaTopicManager:
    """Handles Kafka topic management operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the topic manager.
        
        Args:
            config: Dictionary containing Kafka configuration
        """
        self.config = config
        self.admin_client = None
    
    def create_admin_client(self) -> None:
        """Create the Kafka admin client."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.config['bootstrap_servers'],
                api_version=tuple(self.config['api_version'])
            )
            logger.info("Successfully created Kafka admin client")
        except Exception as e:
            logger.error(f"Failed to create Kafka admin client: {e}")
            raise
    
    def add_partitions(self, topic: str, num_partitions: int) -> None:
        """
        Add partitions to a topic.
        
        Args:
            topic: Topic name
            num_partitions: Number of partitions to add
            
        Raises:
            Exception: If partition addition fails
        """
        if not self.admin_client:
            raise RuntimeError("Kafka admin client not initialized")
            
        try:
            # Get current partition count
            current_partitions = len(self.admin_client.describe_topics([topic])[0].partitions)
            
            # Create new partitions request
            new_partitions = NewPartitions(topic, current_partitions + num_partitions)
            
            # Add partitions
            self.admin_client.create_partitions({topic: new_partitions})
            logger.info(f"Successfully added {num_partitions} partitions to topic {topic}")
            
        except Exception as e:
            logger.error(f"Failed to add partitions to topic {topic}: {e}")
            raise
    
    def get_partition_count(self, topic: str) -> int:
        """
        Get the current number of partitions for a topic.
        
        Args:
            topic: Topic name
            
        Returns:
            Number of partitions
        """
        if not self.admin_client:
            raise RuntimeError("Kafka admin client not initialized")
            
        try:
            topic_info = self.admin_client.describe_topics([topic])[0]
            return len(topic_info.partitions)
        except Exception as e:
            logger.error(f"Failed to get partition count for topic {topic}: {e}")
            raise
    
    def close(self) -> None:
        """Close the admin client."""
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None 