package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

// GPSProcessor handles the processing of GPS data
type GPSProcessor struct {
	logger *logrus.Logger
	osrmURL string
}

// NewGPSProcessor creates a new GPS processor
func NewGPSProcessor(logger *logrus.Logger, osrmURL string) *GPSProcessor {
	return &GPSProcessor{
		logger: logger,
		osrmURL: osrmURL,
	}
}

// SmoothGPSData applies data imputation and projection to GPS data
func (p *GPSProcessor) SmoothGPSData(points []ProcessedGPSData) ([]ProcessedGPSData, error) {
	if len(points) < 2 {
		return points, nil
	}

	var smoothedPoints []ProcessedGPSData
	for i := 0; i < len(points)-1; i++ {
		// Add the current point (after snapping)
		snappedPoint, err := p.snapToRoad(points[i])
		if err != nil {
			return nil, fmt.Errorf("failed to snap point: %w", err)
		}
		smoothedPoints = append(smoothedPoints, snappedPoint)

		// Get route between current and next point
		routePoints, err := p.getRoutePoints(points[i], points[i+1])
		if err != nil {
			return nil, fmt.Errorf("failed to get route points: %w", err)
		}

		// Add interpolated points
		for _, routePoint := range routePoints {
			smoothedPoints = append(smoothedPoints, routePoint)
		}
	}

	// Add the last point (after snapping)
	snappedPoint, err := p.snapToRoad(points[len(points)-1])
	if err != nil {
		return nil, fmt.Errorf("failed to snap last point: %w", err)
	}
	smoothedPoints = append(smoothedPoints, snappedPoint)

	return smoothedPoints, nil
}

// snapToRoad snaps a GPS point to the nearest road using OSRM
func (p *GPSProcessor) snapToRoad(point ProcessedGPSData) (ProcessedGPSData, error) {
	url := fmt.Sprintf("%s/nearest/v1/driving/%f,%f", p.osrmURL, point.Longitude, point.Latitude)
	
	resp, err := http.Get(url)
	if err != nil {
		return ProcessedGPSData{}, err
	}
	defer resp.Body.Close()

	var result struct {
		Waypoints []struct {
			Location []float64 `json:"location"`
		} `json:"waypoints"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ProcessedGPSData{}, err
	}

	if len(result.Waypoints) == 0 {
		return point, nil
	}

	// Update the point with snapped coordinates
	point.Latitude = result.Waypoints[0].Location[1]
	point.Longitude = result.Waypoints[0].Location[0]
	return point, nil
}

// getRoutePoints gets intermediate points along a route between two GPS points
func (p *GPSProcessor) getRoutePoints(start, end ProcessedGPSData) ([]ProcessedGPSData, error) {
	url := fmt.Sprintf("%s/route/v1/driving/%f,%f;%f,%f?overview=full&geometries=geojson", 
		p.osrmURL, start.Longitude, start.Latitude, end.Longitude, end.Latitude)
	
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Routes []struct {
			Geometry struct {
				Coordinates [][]float64 `json:"coordinates"`
			} `json:"geometry"`
		} `json:"routes"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if len(result.Routes) == 0 {
		return nil, fmt.Errorf("no route found")
	}

	// Create interpolated points
	var points []ProcessedGPSData
	coordinates := result.Routes[0].Geometry.Coordinates
	for i := 1; i < len(coordinates)-1; i++ { // Skip first and last points as they're already included
		points = append(points, ProcessedGPSData{
			Latitude:    coordinates[i][1],
			Longitude:   coordinates[i][0],
			Speed:       (start.Speed + end.Speed) / 2, // Interpolate speed
			Timestamp:   start.Timestamp.Add(time.Duration(i) * time.Second), // Interpolate timestamp
			DeviceID:    start.DeviceID,
			VehicleID:   start.VehicleID,
		})
	}

	return points, nil
}

// ProcessBatch processes a batch of raw GPS messages
func (p *GPSProcessor) ProcessBatch(messages [][]byte) ([]ProcessedGPSData, []error) {
	processedBatch := make([]ProcessedGPSData, 0, len(messages))
	errors := make([]error, 0)

	for _, msg := range messages {
		processed, err := p.processMessage(msg)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		processedBatch = append(processedBatch, processed)
	}

	return processedBatch, errors
}

// processMessage processes a single GPS message
func (p *GPSProcessor) processMessage(msg []byte) (ProcessedGPSData, error) {
	var rawData RawGPSData
	if err := json.Unmarshal(msg, &rawData); err != nil {
		return ProcessedGPSData{}, err
	}

	// Convert string values to appropriate types
	latitude, err := strconv.ParseFloat(rawData.Latitude, 64)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	longitude, err := strconv.ParseFloat(rawData.Longitude, 64)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	speed, err := strconv.ParseFloat(rawData.Speed, 64)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	altitude, err := strconv.ParseFloat(rawData.Altitude, 64)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	signal, err := strconv.Atoi(rawData.Signal)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	satellites, err := strconv.Atoi(rawData.Satellites)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	ts, err := strconv.ParseInt(rawData.Timestamp, 10, 64)
	if err != nil {
		return ProcessedGPSData{}, err
	}

	return ProcessedGPSData{
		Latitude:    latitude,
		Longitude:   longitude,
		Speed:       speed,
		Altitude:    altitude,
		Signal:      signal,
		Satellites:  satellites,
		Timestamp:   time.Unix(0, ts*int64(time.Millisecond)),
		DeviceID:    rawData.DeviceID,
		VehicleID:   rawData.VehicleID,
		Status:      rawData.Status,
		InDepot:     rawData.InDepot,
		FormattedTS: rawData.FormattedTS,
	}, nil
} 