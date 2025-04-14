package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Kafka   KafkaConfig  `mapstructure:"kafka"`
	Logging LoggingConfig `mapstructure:"logging"`
}

type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Host string `mapstructure:"host"`
}

type KafkaConfig struct {
	Brokers         string `mapstructure:"brokers"`
	Topic           string `mapstructure:"topic"`
	GroupID         string `mapstructure:"group_id"`
	MinBytes        int    `mapstructure:"min_bytes"`
	MaxBytes        int    `mapstructure:"max_bytes"`
	Partition       int    `mapstructure:"partition"`
	ProtocolVersion string `mapstructure:"protocol_version"`
	BatchSize       int    `mapstructure:"batch_size"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	v.SetDefault("server.port", 8081)
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("kafka.brokers", "localhost:9092")
	v.SetDefault("kafka.topic", "gps-data")
	v.SetDefault("kafka.group_id", "gps-processor")
	v.SetDefault("kafka.min_bytes", 10000)
	v.SetDefault("kafka.max_bytes", 10000000)
	v.SetDefault("kafka.partition", 0)
	v.SetDefault("kafka.protocol_version", "2.5.0")
	v.SetDefault("kafka.batch_size", 10)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")

	// Set config file
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Look for config in the configs directory
		configDir := "configs"
		v.AddConfigPath(configDir)
		v.SetConfigName("config")
		v.SetConfigType("yaml")
	}

	// Read environment variables
	v.AutomaticEnv()
	v.SetEnvPrefix("PROCESSOR")

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if we have defaults
			fmt.Println("Config file not found, using defaults")
		} else {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &config, nil
}

func GetConfigPath() string {
	// First check for environment variable
	if path := os.Getenv("PROCESSOR_CONFIG_PATH"); path != "" {
		return path
	}

	// Then check for config in the configs directory
	configPath := filepath.Join("configs", "config.yaml")
	if _, err := os.Stat(configPath); err == nil {
		return configPath
	}

	// Return empty string if no config found
	return ""
} 