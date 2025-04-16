import csv
import json

def convert_to_csv(input_file, output_file):
    with open(input_file, 'r') as f_in, open(output_file, 'w', newline='') as f_out:
        csv_writer = csv.writer(f_out)
        # Write header
        csv_writer.writerow(['timestamp', 'latitude', 'longitude', 'speed'])
        
        for line in f_in:
            try:
                # Split the line into key and value
                _, value = line.strip().split(':', 1)
                data = json.loads(value)
                csv_writer.writerow([
                    data.get('timestamp', ''),
                    data.get('latitude', ''),
                    data.get('longitude', ''),
                    data.get('speed', '')
                ])
            except Exception as e:
                print(f"Error processing line: {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python convert_to_csv.py input.txt output.csv")
        sys.exit(1)
    
    convert_to_csv(sys.argv[1], sys.argv[2]) 