import yaml
import logging
from pathlib import Path
from typing import Dict, Any

class ConfigLoader:
    """Responsible for loading and providing access to configuration settings."""
    
    def __init__(self, config_path: str = '/app/config/config.yaml'):
        """
        Initialize the config loader.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._setup_logging()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict containing configuration settings
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logging.error(f"Failed to load config: {e}")
            raise
    
    def _setup_logging(self) -> None:
        """Configure logging based on settings in the config file."""
        logging.basicConfig(
            level=getattr(logging, self.config['producer']['logging']['level']),
            format=self.config['producer']['logging']['format']
        )
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration settings."""
        return self.config['kafka']
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration settings."""
        return self.config['producer']
    
    def get_csv_file_path(self) -> str:
        """Get the path to the CSV file."""
        return self.config['producer']['csv_file'] 