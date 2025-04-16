package service

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

// RoadSnapper handles snapping GPS points to roads
type RoadSnapper struct {
	logger  *logrus.Logger
	osrmURL string
}

// NewRoadSnapper creates a new RoadSnapper
func NewRoadSnapper(logger *logrus.Logger, osrmURL string) *RoadSnapper {
	return &RoadSnapper{
		logger:  logger,
		osrmURL: osrmURL,
	}
}

// SnapToRoad snaps a GPS point to the nearest road
func (s *RoadSnapper) SnapToRoad(point ProcessedGPSData) (ProcessedGPSData, error) {
	url := fmt.Sprintf("%s/nearest/v1/driving/%f,%f", s.osrmURL, point.Longitude, point.Latitude)
	
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

	point.Latitude = result.Waypoints[0].Location[1]
	point.Longitude = result.Waypoints[0].Location[0]
	return point, nil
}

// RouteFinder handles finding routes between GPS points
type RouteFinder struct {
	logger  *logrus.Logger
	osrmURL string
}

// NewRouteFinder creates a new RouteFinder
func NewRouteFinder(logger *logrus.Logger, osrmURL string) *RouteFinder {
	return &RouteFinder{
		logger:  logger,
		osrmURL: osrmURL,
	}
}

// GetRoutePoints gets intermediate points along a route between two GPS points
func (f *RouteFinder) GetRoutePoints(start, end ProcessedGPSData) ([]ProcessedGPSData, error) {
	url := fmt.Sprintf("%s/route/v1/driving/%f,%f;%f,%f?overview=full&geometries=geojson", 
		f.osrmURL, start.Longitude, start.Latitude, end.Longitude, end.Latitude)
	
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

	var points []ProcessedGPSData
	coordinates := result.Routes[0].Geometry.Coordinates
	for i := 1; i < len(coordinates)-1; i++ {
		points = append(points, ProcessedGPSData{
			Latitude:    coordinates[i][1],
			Longitude:   coordinates[i][0],
			Speed:       (start.Speed + end.Speed) / 2,
			Timestamp:   start.Timestamp.Add(time.Duration(i) * time.Second),
			DeviceID:    start.DeviceID,
			VehicleID:   start.VehicleID,
		})
	}

	return points, nil
}

// MessageProcessor handles processing raw GPS messages
type MessageProcessor struct {
	logger *logrus.Logger
}

// NewMessageProcessor creates a new MessageProcessor
func NewMessageProcessor(logger *logrus.Logger) *MessageProcessor {
	return &MessageProcessor{
		logger: logger,
	}
}

// ProcessMessage processes a single GPS message
func (p *MessageProcessor) ProcessMessage(msg []byte) (ProcessedGPSData, error) {
	var rawData RawGPSData
	if err := json.Unmarshal(msg, &rawData); err != nil {
		return ProcessedGPSData{}, err
	}

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

// GPSProcessor implements the GPSDataProcessor interface
type GPSProcessor struct {
	logger        *logrus.Logger
	roadSnapper   *RoadSnapper
	routeFinder   *RouteFinder
	msgProcessor  *MessageProcessor
}

// NewGPSProcessor creates a new GPS processor
func NewGPSProcessor(logger *logrus.Logger, osrmURL string) *GPSProcessor {
	return &GPSProcessor{
		logger:       logger,
		roadSnapper:  NewRoadSnapper(logger, osrmURL),
		routeFinder:  NewRouteFinder(logger, osrmURL),
		msgProcessor: NewMessageProcessor(logger),
	}
}

// ProcessBatch processes a batch of raw GPS messages
func (p *GPSProcessor) ProcessBatch(messages [][]byte) ([]ProcessedGPSData, []error) {
	processedBatch := make([]ProcessedGPSData, 0, len(messages))
	errors := make([]error, 0)

	for _, msg := range messages {
		processed, err := p.msgProcessor.ProcessMessage(msg)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		processedBatch = append(processedBatch, processed)
	}

	return processedBatch, errors
}

// SmoothGPSData applies data imputation and projection to GPS data
func (p *GPSProcessor) SmoothGPSData(points []ProcessedGPSData) ([]ProcessedGPSData, error) {
	if len(points) < 2 {
		return points, nil
	}

	var smoothedPoints []ProcessedGPSData
	for i := 0; i < len(points)-1; i++ {
		// Add the current point (after snapping)
		snappedPoint, err := p.roadSnapper.SnapToRoad(points[i])
		if err != nil {
			return nil, fmt.Errorf("failed to snap point: %w", err)
		}
		smoothedPoints = append(smoothedPoints, snappedPoint)

		// Only get route points if the distance is significant
		distance := calculateDistance(points[i], points[i+1])
		if distance > 100 { // Only get route for points more than 100m apart
			routePoints, err := p.routeFinder.GetRoutePoints(points[i], points[i+1])
			if err != nil {
				return nil, fmt.Errorf("failed to get route points: %w", err)
			}

			// Add at most 5 intermediate points per segment
			maxPoints := 5
			step := len(routePoints) / (maxPoints + 1)
			if step < 1 {
				step = 1
			}

			for j := step; j < len(routePoints); j += step {
				if j >= len(routePoints) {
					break
				}
				routePoint := routePoints[j]
				// Calculate distance from start and end points
				distFromStart := calculateDistance(routePoint, points[i])
				distFromEnd := calculateDistance(routePoint, points[i+1])
				
				// Only add point if it's at least 20 meters from both endpoints
				if distFromStart > 20 && distFromEnd > 20 {
					smoothedPoints = append(smoothedPoints, routePoint)
				}
			}
		}
	}

	// Add the last point (after snapping)
	snappedPoint, err := p.roadSnapper.SnapToRoad(points[len(points)-1])
	if err != nil {
		return nil, fmt.Errorf("failed to snap last point: %w", err)
	}
	smoothedPoints = append(smoothedPoints, snappedPoint)

	return smoothedPoints, nil
}

// calculateDistance calculates the distance between two GPS points in meters
func calculateDistance(p1, p2 ProcessedGPSData) float64 {
	// Haversine formula
	const R = 6371000 // Earth's radius in meters
	lat1 := p1.Latitude * math.Pi / 180
	lat2 := p2.Latitude * math.Pi / 180
	deltaLat := (p2.Latitude - p1.Latitude) * math.Pi / 180
	deltaLon := (p2.Longitude - p1.Longitude) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*
			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	distance := R * c

	return distance
} 