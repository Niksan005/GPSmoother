import csv
import json
from datetime import datetime

def convert_to_csv(input_file, output_file):
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        csv_writer = csv.writer(f_out)
        # Write header with all relevant fields
        csv_writer.writerow([
            'timestamp',
            'formatted_timestamp',
            'latitude',
            'longitude',
            'speed',
            'altitude',
            'signal',
            'satellites',
            'device_id',
            'vehicle_id',
            'status',
            'in_depot'
        ])
        
        for line in f_in:
            try:
                # Split the line into key and value
                _, value = line.strip().split(':', 1)
                data = json.loads(value)
                
                # Convert timestamp to ISO format if it exists
                timestamp = data.get('Timestamp', '')
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        timestamp = dt.isoformat()
                    except ValueError:
                        pass
                
                csv_writer.writerow([
                    timestamp,
                    data.get('FormattedTS', ''),
                    data.get('Latitude', ''),
                    data.get('Longitude', ''),
                    data.get('Speed', ''),
                    data.get('Altitude', ''),
                    data.get('Signal', ''),
                    data.get('Satellites', ''),
                    data.get('DeviceID', ''),
                    data.get('VehicleID', ''),
                    data.get('Status', ''),
                    data.get('InDepot', '')
                ])
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
            except ValueError as e:
                print(f"Error splitting line: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python convert_to_csv.py input.txt output.csv")
        sys.exit(1)
    
    convert_to_csv(sys.argv[1], sys.argv[2]) 