import csv
import logging
from collections import defaultdict
from typing import Dict, List, Tuple, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class GPSDataLoader:
    """Responsible for loading and validating GPS data from CSV files."""
    
    def __init__(self, file_path: str):
        """
        Initialize the GPS data loader.
        
        Args:
            file_path: Path to the CSV file containing GPS data
        """
        self.file_path = file_path
    
    def load_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Load and group GPS data by vehicle.
        
        Returns:
            Dictionary with vehicle IDs as keys and lists of GPS data points as values
            
        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            csv.Error: If there's an error reading the CSV file
        """
        vehicle_data = defaultdict(list)
        try:
            with open(self.file_path, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    is_valid, _ = self._validate_gps_data(row)
                    if is_valid:
                        vehicle_data[row['veh_id']].append(row)
            logger.info(f"Successfully loaded data for {len(vehicle_data)} vehicles")
            return vehicle_data
        except Exception as e:
            logger.error(f"Error reading GPS data: {e}")
            raise
    
    def _validate_gps_data(self, row: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate required GPS fields.
        
        Args:
            row: Dictionary containing GPS data fields
            
        Returns:
            Tuple of (is_valid, error_message)
        """
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
    
    @staticmethod
    def format_timestamp(ts: str) -> str:
        """
        Convert Unix timestamp to readable format.
        
        Args:
            ts: Unix timestamp in milliseconds
            
        Returns:
            Formatted timestamp string
        """
        try:
            return datetime.fromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return ts 