package coordinator

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"sql-horizontal-autoscaler/config"
	"sql-horizontal-autoscaler/datastore"
	"sql-horizontal-autoscaler/metrics"
	"sql-horizontal-autoscaler/sharding"
)

// Coordinator manages the monitoring and scaling logic
type Coordinator struct {
	config       *config.Config
	dataStore    *datastore.DataStore
	shardManager *sharding.DynamicShardManager
	metrics      map[string]*metrics.ShardMetrics
	mutex        sync.RWMutex
	stopChan     chan struct{}
}

// NewCoordinator creates a new Coordinator instance
func NewCoordinator(cfg *config.Config, ds *datastore.DataStore, sm *sharding.DynamicShardManager) *Coordinator {
	return &Coordinator{
		config:       cfg,
		dataStore:    ds,
		shardManager: sm,
		metrics:      make(map[string]*metrics.ShardMetrics),
		stopChan:     make(chan struct{}),
	}
}

// Start starts both the HTTP server and the monitoring loop
func (c *Coordinator) Start() error {
	// Start HTTP server in a goroutine
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/shards", c.handleShards)
		mux.HandleFunc("/health", c.handleHealth)

		port := fmt.Sprintf(":%d", c.config.Ports.CoordinatorPort)
		log.Printf("Coordinator HTTP server starting on port %d...", c.config.Ports.CoordinatorPort)
		if err := http.ListenAndServe(port, mux); err != nil {
			log.Printf("Coordinator HTTP server error: %v", err)
		}
	}()

	// Start monitoring loop
	go c.monitoringLoop()

	return nil
}

// Stop stops the coordinator
func (c *Coordinator) Stop() {
	close(c.stopChan)
}

// handleShards handles GET /shards requests
func (c *Coordinator) handleShards(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mutex.RLock()
	shards := make([]*metrics.ShardMetrics, 0, len(c.metrics))
	for _, shardMetrics := range c.metrics {
		shards = append(shards, shardMetrics)
	}
	c.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(shards); err != nil {
		log.Printf("Failed to encode shards response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleHealth handles GET /health requests
func (c *Coordinator) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	health := map[string]interface{}{
		"status":  "healthy",
		"service": "coordinator",
		"strategy": c.config.ScalingStrategy,
		"monitoring_interval": c.config.MonitoringIntervalSeconds,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// monitoringLoop runs the continuous monitoring and scaling logic
func (c *Coordinator) monitoringLoop() {
	log.Printf("Starting monitoring loop with %s strategy (interval: %d seconds)", 
		c.config.ScalingStrategy, c.config.MonitoringIntervalSeconds)

	ticker := time.NewTicker(time.Duration(c.config.MonitoringIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			log.Println("Monitoring loop stopped")
			return
		case <-ticker.C:
			c.collectAndAnalyzeMetrics()
		}
	}
}

// collectAndAnalyzeMetrics collects metrics from all shards and analyzes them for scaling decisions
func (c *Coordinator) collectAndAnalyzeMetrics() {
	log.Println("Collecting metrics from all shards...")

	// Collect metrics from all shards concurrently
	var wg sync.WaitGroup
	metricsChan := make(chan *metrics.ShardMetrics, len(c.config.Shards))

	for shardID := range c.config.Shards {
		wg.Add(1)
		go func(sID string) {
			defer wg.Done()
			metrics, err := c.dataStore.GetShardMetrics(sID)
			if err != nil {
				log.Printf("Failed to get metrics for shard %s: %v", sID, err)
				return
			}
			metricsChan <- metrics
		}(shardID)
	}

	wg.Wait()
	close(metricsChan)

	// Update stored metrics
	c.mutex.Lock()
	for shardMetrics := range metricsChan {
		c.metrics[shardMetrics.ShardID] = shardMetrics
	}
	c.mutex.Unlock()

	// Analyze metrics for scaling decisions
	c.analyzeForScaling()
}

// analyzeForScaling analyzes the collected metrics and makes scaling decisions
func (c *Coordinator) analyzeForScaling() {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	switch c.config.ScalingStrategy {
	case "hot":
		c.analyzeHotScaling()
	case "cold":
		c.analyzeColdScaling()
	default:
		log.Printf("Unknown scaling strategy: %s", c.config.ScalingStrategy)
	}
}

// analyzeHotScaling implements hot scaling logic (individual shard thresholds)
func (c *Coordinator) analyzeHotScaling() {
	for shardID, shardMetrics := range c.metrics {
		// Check CPU threshold
		if shardMetrics.CPUPercent >= c.config.ScalingThresholds.CPUThresholdPercent {
			log.Printf("HOT SCALING TRIGGERED: Shard %s CPU at %.1f%% (threshold: %.1f%%)",
				shardID, shardMetrics.CPUPercent, c.config.ScalingThresholds.CPUThresholdPercent)
			c.triggerScaling(shardID, "cpu", shardMetrics.CPUPercent)
		}

		// Check memory threshold
		if shardMetrics.MemoryPercent >= c.config.ScalingThresholds.MemoryThresholdPercent {
			log.Printf("HOT SCALING TRIGGERED: Shard %s Memory at %.1f%% (threshold: %.1f%%)",
				shardID, shardMetrics.MemoryPercent, c.config.ScalingThresholds.MemoryThresholdPercent)
			c.triggerScaling(shardID, "memory", shardMetrics.MemoryPercent)
		}

		// Check entry count threshold
		if shardMetrics.TotalEntries >= c.config.ScalingThresholds.TotalEntryThresholdPerShard {
			log.Printf("HOT SCALING TRIGGERED: Shard %s has %d entries (threshold: %d)",
				shardID, shardMetrics.TotalEntries, c.config.ScalingThresholds.TotalEntryThresholdPerShard)
			c.triggerScaling(shardID, "entries", float64(shardMetrics.TotalEntries))
		}

		// Check connection count threshold
		if shardMetrics.ConnectionCount >= c.config.ScalingThresholds.ConnectionThreshold {
			log.Printf("HOT SCALING TRIGGERED: Shard %s has %d connections (threshold: %d)",
				shardID, shardMetrics.ConnectionCount, c.config.ScalingThresholds.ConnectionThreshold)
			c.triggerScaling(shardID, "connections", float64(shardMetrics.ConnectionCount))
		}

		// Check queries per second threshold
		if shardMetrics.QueriesPerSec >= c.config.ScalingThresholds.QPSThreshold {
			log.Printf("HOT SCALING TRIGGERED: Shard %s has %.1f QPS (threshold: %.1f)",
				shardID, shardMetrics.QueriesPerSec, c.config.ScalingThresholds.QPSThreshold)
			c.triggerScaling(shardID, "qps", shardMetrics.QueriesPerSec)
		}
	}
}

// analyzeColdScaling implements cold scaling logic (aggregate thresholds)
func (c *Coordinator) analyzeColdScaling() {
	var totalEntries int64
	var avgCPU, avgMemory float64
	var totalConnections int64
	var highCPUShards, highMemoryShards []string

	// Calculate aggregate metrics
	for shardID, shardMetrics := range c.metrics {
		totalEntries += shardMetrics.TotalEntries
		avgCPU += shardMetrics.CPUPercent
		avgMemory += shardMetrics.MemoryPercent
		totalConnections += shardMetrics.ConnectionCount

		if shardMetrics.CPUPercent >= c.config.ScalingThresholds.CPUThresholdPercent {
			highCPUShards = append(highCPUShards, shardID)
		}

		if shardMetrics.MemoryPercent >= c.config.ScalingThresholds.MemoryThresholdPercent {
			highMemoryShards = append(highMemoryShards, shardID)
		}
	}

	if len(c.metrics) > 0 {
		avgCPU /= float64(len(c.metrics))
		avgMemory /= float64(len(c.metrics))
	}

	// Check aggregate thresholds
	totalThreshold := c.config.ScalingThresholds.TotalEntryThresholdPerShard * int64(len(c.config.Shards))
	if totalEntries >= totalThreshold {
		log.Printf("COLD SCALING TRIGGERED: Total entries %d reached threshold %d across %d shards", 
			totalEntries, totalThreshold, len(c.config.Shards))
		c.triggerScaling("cluster", "total_entries", float64(totalEntries))
	}

	// Check if multiple shards have high CPU
	if len(highCPUShards) >= len(c.config.Shards)/2 {
		log.Printf("COLD SCALING TRIGGERED: %d out of %d shards have high CPU (avg: %.1f%%)", 
			len(highCPUShards), len(c.config.Shards), avgCPU)
		c.triggerScaling("cluster", "avg_cpu", avgCPU)
	}
}

// triggerScaling triggers actual scaling actions by creating new shards
func (c *Coordinator) triggerScaling(target string, reason string, value float64) {
	log.Printf("🚨 SCALING TRIGGERED: Target=%s, Reason=%s, Value=%.1f", target, reason, value)

	// Check if we should scale out (add new shard)
	currentShardCount := c.shardManager.GetShardCount()
	maxShards := c.config.Limits.MaxShards

	if currentShardCount >= maxShards {
		log.Printf("⚠️  Maximum shard count (%d) reached, cannot scale further", maxShards)
		return
	}

	// Trigger actual shard creation
	log.Printf("🚀 Initiating shard scale-out: %d → %d shards", currentShardCount, currentShardCount+1)

	go func() {
		if err := c.scaleOutShard(); err != nil {
			log.Printf("❌ Failed to scale out: %v", err)
		}
	}()
}

// scaleOutShard creates a new shard and integrates it into the system
func (c *Coordinator) scaleOutShard() error {
	log.Printf("📈 Starting shard scale-out process...")

	// 1. Create new shard
	newShardInfo, err := c.shardManager.AddNewShard()
	if err != nil {
		return fmt.Errorf("failed to create new shard: %w", err)
	}

	log.Printf("✅ New shard created: %s (port %d)", newShardInfo.ID, newShardInfo.Port)

	// 2. Add new shard to datastore connections
	tableNames := make([]string, 0, len(c.config.TableShardKeys))
	for tableName := range c.config.TableShardKeys {
		tableNames = append(tableNames, tableName)
	}

	if err := c.dataStore.AddShardConnection(newShardInfo.ID, newShardInfo.DSN, tableNames); err != nil {
		log.Printf("❌ Failed to add shard connection: %v", err)
		return fmt.Errorf("failed to add shard connection: %w", err)
	}

	log.Printf("✅ Shard %s integrated into datastore", newShardInfo.ID)

	// 3. Update configuration dynamically
	c.config.Shards[newShardInfo.ID] = newShardInfo.DSN

	log.Printf("🎉 Scale-out complete! New shard %s is active and ready", newShardInfo.ID)
	log.Printf("📊 Current cluster: %d shards active", c.shardManager.GetShardCount())

	return nil
}
