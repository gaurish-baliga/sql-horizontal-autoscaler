package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config represents the application configuration
type Config struct {
	Shards                     map[string]string `json:"shards"`
	TableShardKeys             map[string]string `json:"table_shard_keys"`
	ScalingThresholds          ScalingThresholds `json:"scaling_thresholds"`
	ScalingStrategy            string            `json:"scaling_strategy"`
	MonitoringIntervalSeconds  int               `json:"monitoring_interval_seconds"`
	Database                   DatabaseConfig    `json:"database"`
	Docker                     DockerConfig      `json:"docker"`
	Ports                      PortsConfig       `json:"ports"`
	Limits                     LimitsConfig      `json:"limits"`
}

// ScalingThresholds contains the thresholds for scaling decisions
type ScalingThresholds struct {
	CPUThresholdPercent         float64 `json:"cpu_threshold_percent"`
	MemoryThresholdPercent      float64 `json:"memory_threshold_percent"`
	ConnectionThreshold         int64   `json:"connection_threshold"`
	QPSThreshold                float64 `json:"qps_threshold"`
	TotalEntryThresholdPerShard int64   `json:"total_entry_threshold_per_shard"`
}

// DatabaseConfig contains database connection settings
type DatabaseConfig struct {
	Username     string `json:"username"`
	Password     string `json:"password"`
	RootPassword string `json:"root_password"`
}

// DockerConfig contains Docker-related settings
type DockerConfig struct {
	NetworkName     string `json:"network_name"`
	Image           string `json:"image"`
	ContainerPrefix string `json:"container_prefix"`
}

// PortsConfig contains port configuration
type PortsConfig struct {
	BasePort          int `json:"base_port"`
	QueryRouterPort   int `json:"query_router_port"`
	CoordinatorPort   int `json:"coordinator_port"`
}

// LimitsConfig contains system limits
type LimitsConfig struct {
	MaxShards                      int `json:"max_shards"`
	MaxConnectionAttempts          int `json:"max_connection_attempts"`
	ConnectionRetryIntervalSeconds int `json:"connection_retry_interval_seconds"`
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	// Validate configuration
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// validate checks if the configuration is valid
func (c *Config) validate() error {
	if len(c.Shards) == 0 {
		return fmt.Errorf("no shards configured")
	}

	if len(c.TableShardKeys) == 0 {
		return fmt.Errorf("no table shard keys configured")
	}

	if c.ScalingStrategy != "hot" && c.ScalingStrategy != "cold" {
		return fmt.Errorf("scaling strategy must be 'hot' or 'cold'")
	}

	if c.ScalingThresholds.CPUThresholdPercent <= 0 || c.ScalingThresholds.CPUThresholdPercent > 100 {
		return fmt.Errorf("CPU threshold must be between 0 and 100")
	}

	if c.ScalingThresholds.TotalEntryThresholdPerShard <= 0 {
		return fmt.Errorf("total entry threshold must be positive")
	}

	if c.MonitoringIntervalSeconds <= 0 {
		c.MonitoringIntervalSeconds = 60 // default to 60 seconds
	}

	// Set defaults for new configuration sections
	if c.Database.Username == "" {
		c.Database.Username = "testuser"
	}
	if c.Database.Password == "" {
		c.Database.Password = "testpass"
	}
	if c.Database.RootPassword == "" {
		c.Database.RootPassword = "rootpass"
	}
	if c.Docker.NetworkName == "" {
		c.Docker.NetworkName = "autoscaler-network"
	}
	if c.Docker.Image == "" {
		c.Docker.Image = "mysql:8.0"
	}
	if c.Docker.ContainerPrefix == "" {
		c.Docker.ContainerPrefix = "mysql"
	}
	if c.Ports.BasePort == 0 {
		c.Ports.BasePort = 3306
	}
	if c.Ports.QueryRouterPort == 0 {
		c.Ports.QueryRouterPort = 8080
	}
	if c.Ports.CoordinatorPort == 0 {
		c.Ports.CoordinatorPort = 9090
	}
	if c.Limits.MaxShards == 0 {
		c.Limits.MaxShards = 5
	}
	if c.Limits.MaxConnectionAttempts == 0 {
		c.Limits.MaxConnectionAttempts = 30
	}
	if c.Limits.ConnectionRetryIntervalSeconds == 0 {
		c.Limits.ConnectionRetryIntervalSeconds = 2
	}
	if c.ScalingThresholds.MemoryThresholdPercent == 0 {
		c.ScalingThresholds.MemoryThresholdPercent = 85.0
	}
	if c.ScalingThresholds.ConnectionThreshold == 0 {
		c.ScalingThresholds.ConnectionThreshold = 20
	}
	if c.ScalingThresholds.QPSThreshold == 0 {
		c.ScalingThresholds.QPSThreshold = 1000.0
	}

	return nil
}

// GetShardIDs returns a slice of all shard IDs
func (c *Config) GetShardIDs() []string {
	shardIDs := make([]string, 0, len(c.Shards))
	for shardID := range c.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	return shardIDs
}
