package service

import (
	"time"
)

// RawGPSData represents the raw GPS data received from Kafka
type RawGPSData struct {
	Latitude    string `json:"lat"`
	Longitude   string `json:"lon"`
	Timestamp   string `json:"ts"`
	DeviceID    string `json:"dev_id"`
	Speed       string `json:"speed"`
	Altitude    string `json:"alt"`
	Signal      string `json:"signal"`
	Satellites  string `json:"sats"`
	Status      string `json:"status"`
	InDepot     string `json:"in_depot"`
	VehicleID   string `json:"veh_id"`
	FormattedTS string `json:"formatted_ts"`
}

// ProcessedGPSData represents the processed GPS data with proper types
type ProcessedGPSData struct {
	Latitude    float64
	Longitude   float64
	Speed       float64
	Altitude    float64
	Signal      int
	Satellites  int
	Timestamp   time.Time
	DeviceID    string
	VehicleID   string
	Status      string
	InDepot     string
	FormattedTS string
} 