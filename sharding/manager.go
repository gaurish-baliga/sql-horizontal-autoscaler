package sharding

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"stathat.com/c/consistent"
)

// DynamicShardManager manages dynamic shard creation and consistent hashing
type DynamicShardManager struct {
	ring         *consistent.Consistent
	shards       map[string]*ShardInfo
	mutex        sync.RWMutex
	nextShardNum int
	config       *ShardManagerConfig
}

// ShardManagerConfig contains configuration for the shard manager
type ShardManagerConfig struct {
	BasePort                       int
	NetworkName                    string
	DatabaseUsername               string
	DatabasePassword               string
	DatabaseRootPassword           string
	DockerImage                    string
	ContainerPrefix                string
	MaxConnectionAttempts          int
	ConnectionRetryIntervalSeconds int
}

// ShardInfo contains information about a shard
type ShardInfo struct {
	ID          string    `json:"id"`
	Port        int       `json:"port"`
	DSN         string    `json:"dsn"`
	DatabaseName string   `json:"database_name"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// NewDynamicShardManager creates a new dynamic shard manager
func NewDynamicShardManager(initialShards map[string]string, config *ShardManagerConfig) *DynamicShardManager {
	ring := consistent.New()
	shards := make(map[string]*ShardInfo)

	// Add initial shards to the ring and track them
	nextShardNum := 1
	for shardID, dsn := range initialShards {
		ring.Add(shardID)

		// Extract port from DSN or calculate it
		port := config.BasePort + nextShardNum - 1
		dbName := fmt.Sprintf("shard%d_db", nextShardNum)

		shards[shardID] = &ShardInfo{
			ID:          shardID,
			Port:        port,
			DSN:         dsn,
			DatabaseName: dbName,
			Status:      "active",
			CreatedAt:   time.Now(),
		}
		nextShardNum++
	}

	return &DynamicShardManager{
		ring:         ring,
		shards:       shards,
		nextShardNum: nextShardNum,
		config:       config,
	}
}

// GetShard returns the shard ID for a given key using consistent hashing
func (dsm *DynamicShardManager) GetShard(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key cannot be empty")
	}

	shard, err := dsm.ring.Get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get shard for key %s: %w", key, err)
	}

	return shard, nil
}

// GetAllShards returns all active shards
func (dsm *DynamicShardManager) GetAllShards() []string {
	dsm.mutex.RLock()
	defer dsm.mutex.RUnlock()
	
	var activeShards []string
	for shardID, shardInfo := range dsm.shards {
		if shardInfo.Status == "active" {
			activeShards = append(activeShards, shardID)
		}
	}
	return activeShards
}

// GetShardInfo returns detailed information about a shard
func (dsm *DynamicShardManager) GetShardInfo(shardID string) (*ShardInfo, bool) {
	dsm.mutex.RLock()
	defer dsm.mutex.RUnlock()
	
	info, exists := dsm.shards[shardID]
	return info, exists
}

// GetAllShardInfo returns information about all shards
func (dsm *DynamicShardManager) GetAllShardInfo() map[string]*ShardInfo {
	dsm.mutex.RLock()
	defer dsm.mutex.RUnlock()
	
	result := make(map[string]*ShardInfo)
	for k, v := range dsm.shards {
		result[k] = v
	}
	return result
}

// AddNewShard dynamically creates and adds a new shard
func (dsm *DynamicShardManager) AddNewShard() (*ShardInfo, error) {
	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Generate new shard configuration
	newShardID := fmt.Sprintf("shard-%d", dsm.nextShardNum)
	newPort := dsm.config.BasePort + dsm.nextShardNum - 1
	newDBName := fmt.Sprintf("shard%d_db", dsm.nextShardNum)
	newDSN := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/%s",
		dsm.config.DatabaseUsername, dsm.config.DatabasePassword, newPort, newDBName)

	log.Printf("🚀 Creating new shard: %s on port %d", newShardID, newPort)

	// Create new shard info
	shardInfo := &ShardInfo{
		ID:          newShardID,
		Port:        newPort,
		DSN:         newDSN,
		DatabaseName: newDBName,
		Status:      "provisioning",
		CreatedAt:   time.Now(),
	}

	// Start Docker container for new shard
	if err := dsm.provisionDockerShard(shardInfo); err != nil {
		return nil, fmt.Errorf("failed to provision shard %s: %w", newShardID, err)
	}

	// Wait for shard to be ready
	if err := dsm.waitForShardReady(shardInfo); err != nil {
		return nil, fmt.Errorf("shard %s failed to become ready: %w", newShardID, err)
	}

	// Setup database schema and initial data
	if err := dsm.setupShardSchema(shardInfo); err != nil {
		log.Printf("Warning: Failed to setup schema for shard %s: %v", newShardID, err)
		// Don't fail completely, shard can still be used
	}

	// Add to consistent hash ring
	dsm.ring.Add(newShardID)
	
	// Update shard status and tracking
	shardInfo.Status = "active"
	dsm.shards[newShardID] = shardInfo
	dsm.nextShardNum++

	log.Printf("✅ Successfully created and activated shard: %s", newShardID)
	return shardInfo, nil
}

// provisionDockerShard creates a new Docker container for the shard
func (dsm *DynamicShardManager) provisionDockerShard(shardInfo *ShardInfo) error {
	containerName := fmt.Sprintf("%s-%s", dsm.config.ContainerPrefix, shardInfo.ID)

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"--network", dsm.config.NetworkName,
		"-p", fmt.Sprintf("%d:3306", shardInfo.Port),
		"-e", fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", dsm.config.DatabaseRootPassword),
		"-e", fmt.Sprintf("MYSQL_DATABASE=%s", shardInfo.DatabaseName),
		"-e", fmt.Sprintf("MYSQL_USER=%s", dsm.config.DatabaseUsername),
		"-e", fmt.Sprintf("MYSQL_PASSWORD=%s", dsm.config.DatabasePassword),
		dsm.config.DockerImage)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker run failed: %w, output: %s", err, string(output))
	}

	log.Printf("📦 Docker container created for shard %s: %s", shardInfo.ID, containerName)
	return nil
}

// waitForShardReady waits for the shard to be ready to accept connections
func (dsm *DynamicShardManager) waitForShardReady(shardInfo *ShardInfo) error {
	containerName := fmt.Sprintf("%s-%s", dsm.config.ContainerPrefix, shardInfo.ID)
	maxAttempts := dsm.config.MaxConnectionAttempts

	log.Printf("⏳ Waiting for shard %s to be ready...", shardInfo.ID)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		cmd := exec.Command("docker", "exec", containerName,
			"mysqladmin", "ping", "-h", "localhost", "-u", dsm.config.DatabaseUsername,
			fmt.Sprintf("-p%s", dsm.config.DatabasePassword))

		if err := cmd.Run(); err == nil {
			log.Printf("✅ Shard %s is ready after %d attempts", shardInfo.ID, attempt)
			return nil
		}

		if attempt%5 == 0 {
			log.Printf("   Attempt %d/%d - still waiting for shard %s...", attempt, maxAttempts, shardInfo.ID)
		}

		time.Sleep(time.Duration(dsm.config.ConnectionRetryIntervalSeconds) * time.Second)
	}

	return fmt.Errorf("shard %s failed to become ready within %d attempts", shardInfo.ID, maxAttempts)
}

// setupShardSchema creates tables and initial data for the new shard
func (dsm *DynamicShardManager) setupShardSchema(shardInfo *ShardInfo) error {
	containerName := fmt.Sprintf("%s-%s", dsm.config.ContainerPrefix, shardInfo.ID)
	
	// Create tables
	createTablesSQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shard_info VARCHAR(50) DEFAULT '%s'
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_name VARCHAR(100),
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shard_info VARCHAR(50) DEFAULT '%s'
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    category VARCHAR(50),
    shard_info VARCHAR(50) DEFAULT '%s'
);`, shardInfo.ID, shardInfo.ID, shardInfo.ID)

	cmd := exec.Command("docker", "exec", "-i", containerName,
		"mysql", "-u", dsm.config.DatabaseUsername,
		fmt.Sprintf("-p%s", dsm.config.DatabasePassword), shardInfo.DatabaseName)
	cmd.Stdin = strings.NewReader(createTablesSQL)
	
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create tables: %w, output: %s", err, string(output))
	}

	// Insert some initial data
	shardNum, _ := strconv.Atoi(shardInfo.ID[len("shard-"):])
	baseID := shardNum * 1000

	for i := 1; i <= 10; i++ {
		userID := baseID + i
		insertSQL := fmt.Sprintf("INSERT IGNORE INTO users (user_id, name, email) VALUES (%d, 'User %d', 'user%d@%s.com');", 
			userID, userID, userID, shardInfo.ID)
		
		cmd := exec.Command("docker", "exec", containerName,
			"mysql", "-u", dsm.config.DatabaseUsername,
			fmt.Sprintf("-p%s", dsm.config.DatabasePassword), shardInfo.DatabaseName, "-e", insertSQL)
		cmd.Run() // Ignore errors for INSERT IGNORE
	}

	log.Printf("📊 Schema and initial data setup complete for shard %s", shardInfo.ID)
	return nil
}

// RemoveShard removes a shard from the ring (for future use)
func (dsm *DynamicShardManager) RemoveShard(shardID string) error {
	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	if shardInfo, exists := dsm.shards[shardID]; exists {
		dsm.ring.Remove(shardID)
		shardInfo.Status = "removed"
		log.Printf("🗑️  Removed shard %s from consistent hash ring", shardID)
		return nil
	}

	return fmt.Errorf("shard %s not found", shardID)
}

// GetShardCount returns the current number of active shards
func (dsm *DynamicShardManager) GetShardCount() int {
	dsm.mutex.RLock()
	defer dsm.mutex.RUnlock()
	
	count := 0
	for _, shardInfo := range dsm.shards {
		if shardInfo.Status == "active" {
			count++
		}
	}
	return count
}
