package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

// RealMetricsCollector collects actual system and database metrics
type RealMetricsCollector struct {
	connections map[string]*sql.DB
	tableNames  []string
}

// ShardMetrics represents real metrics for a single shard
type ShardMetrics struct {
	ShardID         string    `json:"shard_id"`
	CPUPercent      float64   `json:"cpu_percent"`
	MemoryPercent   float64   `json:"memory_percent"`
	DiskPercent     float64   `json:"disk_percent"`
	TotalEntries    int64     `json:"total_entries"`
	ConnectionCount int64     `json:"connection_count"`
	QueriesPerSec   float64   `json:"queries_per_second"`
	Status          string    `json:"status"`
	LastUpdated     time.Time `json:"last_updated"`
	DatabaseSize    int64     `json:"database_size_bytes"`
	TableCounts     map[string]int64 `json:"table_counts"`
}

// DatabaseStats represents database-specific metrics
type DatabaseStats struct {
	ConnectionsActive   int64
	ConnectionsIdle     int64
	QueriesTotal        int64
	SlowQueries         int64
	BufferPoolSize      int64
	BufferPoolUsed      int64
	InnodbRowsRead      int64
	InnodbRowsInserted  int64
	InnodbRowsUpdated   int64
	InnodbRowsDeleted   int64
}

// NewRealMetricsCollector creates a new real metrics collector
func NewRealMetricsCollector(connections map[string]*sql.DB, tableNames []string) *RealMetricsCollector {
	return &RealMetricsCollector{
		connections: connections,
		tableNames:  tableNames,
	}
}

// CollectShardMetrics collects real metrics for a specific shard
func (rmc *RealMetricsCollector) CollectShardMetrics(shardID string) (*ShardMetrics, error) {
	db, exists := rmc.connections[shardID]
	if !exists {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	// Test database connectivity first
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return &ShardMetrics{
			ShardID:     shardID,
			Status:      "unhealthy",
			LastUpdated: time.Now(),
		}, fmt.Errorf("shard %s is unhealthy: %w", shardID, err)
	}

	metrics := &ShardMetrics{
		ShardID:     shardID,
		Status:      "healthy",
		LastUpdated: time.Now(),
		TableCounts: make(map[string]int64),
	}

	// Collect system metrics
	if err := rmc.collectSystemMetrics(metrics); err != nil {
		log.Printf("Warning: Failed to collect system metrics for shard %s: %v", shardID, err)
	}

	// Collect database metrics
	if err := rmc.collectDatabaseMetrics(ctx, db, metrics); err != nil {
		log.Printf("Warning: Failed to collect database metrics for shard %s: %v", shardID, err)
		// Don't return error here, partial metrics are still useful
	}

	return metrics, nil
}

// collectSystemMetrics collects CPU, memory, and disk metrics
func (rmc *RealMetricsCollector) collectSystemMetrics(metrics *ShardMetrics) error {
	// CPU usage
	cpuPercents, err := cpu.Percent(time.Second, false)
	if err != nil {
		return fmt.Errorf("failed to get CPU metrics: %w", err)
	}
	if len(cpuPercents) > 0 {
		metrics.CPUPercent = cpuPercents[0]
	}

	// Memory usage
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return fmt.Errorf("failed to get memory metrics: %w", err)
	}
	metrics.MemoryPercent = memInfo.UsedPercent

	// Disk usage (root filesystem)
	diskInfo, err := disk.Usage("/")
	if err != nil {
		return fmt.Errorf("failed to get disk metrics: %w", err)
	}
	metrics.DiskPercent = diskInfo.UsedPercent

	return nil
}

// collectDatabaseMetrics collects database-specific metrics
func (rmc *RealMetricsCollector) collectDatabaseMetrics(ctx context.Context, db *sql.DB, metrics *ShardMetrics) error {
	// Get database connection stats
	stats := db.Stats()
	metrics.ConnectionCount = int64(stats.OpenConnections)

	// Get database size
	if err := rmc.getDatabaseSize(ctx, db, metrics); err != nil {
		log.Printf("Warning: Failed to get database size: %v", err)
	}

	// Get table row counts
	if err := rmc.getTableCounts(ctx, db, metrics); err != nil {
		log.Printf("Warning: Failed to get table counts: %v", err)
	}

	// Get MySQL status variables
	if err := rmc.getMySQLStatus(ctx, db, metrics); err != nil {
		log.Printf("Warning: Failed to get MySQL status: %v", err)
	}

	// Calculate total entries across all tables
	var totalEntries int64
	for _, count := range metrics.TableCounts {
		totalEntries += count
	}
	metrics.TotalEntries = totalEntries

	return nil
}

// getDatabaseSize gets the total size of the database in bytes
func (rmc *RealMetricsCollector) getDatabaseSize(ctx context.Context, db *sql.DB, metrics *ShardMetrics) error {
	query := `
		SELECT SUM(data_length + index_length) as size_bytes
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()
	`
	
	var sizeBytes sql.NullInt64
	err := db.QueryRowContext(ctx, query).Scan(&sizeBytes)
	if err != nil {
		return fmt.Errorf("failed to query database size: %w", err)
	}
	
	if sizeBytes.Valid {
		metrics.DatabaseSize = sizeBytes.Int64
	}
	
	return nil
}

// getTableCounts gets row counts for all configured tables
func (rmc *RealMetricsCollector) getTableCounts(ctx context.Context, db *sql.DB, metrics *ShardMetrics) error {
	for _, tableName := range rmc.tableNames {
		// Use EXPLAIN SELECT COUNT(*) for better performance on large tables
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
		
		var count int64
		err := db.QueryRowContext(ctx, query).Scan(&count)
		if err != nil {
			// Table might not exist in this shard, log but continue
			log.Printf("Warning: Failed to count rows in table %s: %v", tableName, err)
			metrics.TableCounts[tableName] = 0
			continue
		}
		
		metrics.TableCounts[tableName] = count
	}
	
	return nil
}

// getMySQLStatus gets MySQL server status variables
func (rmc *RealMetricsCollector) getMySQLStatus(ctx context.Context, db *sql.DB, metrics *ShardMetrics) error {
	// Get queries per second by checking Questions status
	query := "SHOW STATUS LIKE 'Questions'"
	
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query MySQL status: %w", err)
	}
	defer rows.Close()
	
	for rows.Next() {
		var variableName, value string
		if err := rows.Scan(&variableName, &value); err != nil {
			continue
		}
		
		switch strings.ToLower(variableName) {
		case "questions":
			if questions, err := strconv.ParseInt(value, 10, 64); err == nil {
				// This is cumulative, in a real system you'd track the delta
				// For now, we'll calculate a rough estimate
				uptime := rmc.getMySQLUptime(ctx, db)
				if uptime > 0 {
					metrics.QueriesPerSec = float64(questions) / uptime
				}
			}
		}
	}
	
	return rows.Err()
}

// getMySQLUptime gets MySQL server uptime in seconds
func (rmc *RealMetricsCollector) getMySQLUptime(ctx context.Context, db *sql.DB) float64 {
	query := "SHOW STATUS LIKE 'Uptime'"
	
	var variableName, value string
	err := db.QueryRowContext(ctx, query).Scan(&variableName, &value)
	if err != nil {
		return 0
	}
	
	uptime, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	
	return uptime
}

// GetDetailedDatabaseStats gets comprehensive database statistics
func (rmc *RealMetricsCollector) GetDetailedDatabaseStats(ctx context.Context, db *sql.DB) (*DatabaseStats, error) {
	stats := &DatabaseStats{}
	
	// Get connection pool stats
	poolStats := db.Stats()
	stats.ConnectionsActive = int64(poolStats.OpenConnections - poolStats.Idle)
	stats.ConnectionsIdle = int64(poolStats.Idle)
	
	// Get MySQL status variables
	statusQuery := `
		SHOW STATUS WHERE Variable_name IN (
			'Questions', 'Slow_queries', 'Innodb_buffer_pool_size',
			'Innodb_buffer_pool_pages_data', 'Innodb_rows_read',
			'Innodb_rows_inserted', 'Innodb_rows_updated', 'Innodb_rows_deleted'
		)
	`
	
	rows, err := db.QueryContext(ctx, statusQuery)
	if err != nil {
		return stats, fmt.Errorf("failed to query MySQL status: %w", err)
	}
	defer rows.Close()
	
	for rows.Next() {
		var variableName, value string
		if err := rows.Scan(&variableName, &value); err != nil {
			continue
		}
		
		intValue, _ := strconv.ParseInt(value, 10, 64)
		
		switch strings.ToLower(variableName) {
		case "questions":
			stats.QueriesTotal = intValue
		case "slow_queries":
			stats.SlowQueries = intValue
		case "innodb_buffer_pool_size":
			stats.BufferPoolSize = intValue
		case "innodb_buffer_pool_pages_data":
			stats.BufferPoolUsed = intValue
		case "innodb_rows_read":
			stats.InnodbRowsRead = intValue
		case "innodb_rows_inserted":
			stats.InnodbRowsInserted = intValue
		case "innodb_rows_updated":
			stats.InnodbRowsUpdated = intValue
		case "innodb_rows_deleted":
			stats.InnodbRowsDeleted = intValue
		}
	}
	
	return stats, rows.Err()
}
