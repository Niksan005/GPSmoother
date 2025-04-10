# 🛰️ GPS Data Cleaner & Router

## Project Overview
This project implements a real-time GPS data processing pipeline that:
1. Ingests raw GPS coordinates
2. Cleans and smooths the data
3. Routes the coordinates along road networks
4. Outputs refined GPS traces

## 🎯 Core Features
- Real-time GPS data streaming using Kafka
- Data cleaning and validation
- Road network snapping using OSRM
- Structured output of processed GPS traces

## 🛠️ Technical Stack
- **Data Source**: `raw_gps.csv` containing timestamped latitude/longitude coordinates
- **Streaming**: Apache Kafka for real-time data processing
- **Processing**: Custom data cleaning and validation logic
- **Routing**: OSRM (Open Source Routing Machine) for road network alignment
- **Infrastructure**: Docker-based deployment

## ⚙️ Infrastructure Setup
The project uses Docker Compose (`osrm.yml`) to:
1. Download Bulgaria map data from Geofabrik
2. Process the map data through OSRM pipeline:
   - Extract
   - Partition
   - Customize
3. Deploy OSRM routing engine

## 📋 Project Requirements

### Data Processing Pipeline
1. **Input Processing**
   - Read from `raw_gps.csv` (simulating Kafka topic)
   - Parse and validate GPS coordinates

2. **Data Cleaning**
   - Remove noise and outliers
   - Deduplicate coordinates
   - Interpolate missing points

3. **Route Optimization**
   - Snap coordinates to road network
   - Optimize route paths
   - Generate clean GPS traces

### Output
- Processed GPS data stream (Kafka topic or structured file)
- Clean, validated, and road-aligned coordinates

## 🚀 Getting Started
1. Set up the Docker environment
2. Configure Kafka topics
3. Run the data processing pipeline
4. Monitor the output stream

## 📚 Documentation
Detailed setup and usage instructions are provided in the project documentation.