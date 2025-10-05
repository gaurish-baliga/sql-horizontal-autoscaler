package datastore

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"sql-horizontal-autoscaler/metrics"
)

// DataStore manages database connections and query execution
type DataStore struct {
	connections     map[string]*sql.DB
	mutex           sync.RWMutex
	metricsCollector *metrics.RealMetricsCollector
}

// NewDataStore creates a new DataStore instance
func NewDataStore() *DataStore {
	return &DataStore{
		connections: make(map[string]*sql.DB),
	}
}

// InitializeConnections establishes connections to all configured shards
func (ds *DataStore) InitializeConnections(shards map[string]string, tableNames []string) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	for shardID, dsn := range shards {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("failed to open connection to shard %s: %w", shardID, err)
		}

		// Test the connection
		if err := db.Ping(); err != nil {
			db.Close()
			return fmt.Errorf("failed to ping shard %s: %w", shardID, err)
		}

		// Configure connection pool
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(5)

		ds.connections[shardID] = db
	}

	// Initialize metrics collector with real connections and table names
	ds.metricsCollector = metrics.NewRealMetricsCollector(ds.connections, tableNames)

	return nil
}

// AddShardConnection adds a new shard connection dynamically
func (ds *DataStore) AddShardConnection(shardID, dsn string, tableNames []string) error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// Check if shard already exists
	if _, exists := ds.connections[shardID]; exists {
		return fmt.Errorf("shard %s already exists", shardID)
	}

	// Create new database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open connection to shard %s: %w", shardID, err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping shard %s: %w", shardID, err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	// Add to connections map
	ds.connections[shardID] = db

	// Update metrics collector with new connection
	if ds.metricsCollector != nil {
		ds.metricsCollector = metrics.NewRealMetricsCollector(ds.connections, tableNames)
	}

	return nil
}

// ExecuteQuery executes a query on a specific shard
func (ds *DataStore) ExecuteQuery(query string, shardID string) ([]map[string]interface{}, error) {
	ds.mutex.RLock()
	db, exists := ds.connections[shardID]
	ds.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query on shard %s: %w", shardID, err)
	}
	defer rows.Close()

	return scanRows(rows)
}

// ExecuteQueryOnAllShards executes a query on all shards concurrently (scatter-gather)
func (ds *DataStore) ExecuteQueryOnAllShards(query string) ([]map[string]interface{}, error) {
	ds.mutex.RLock()
	shardIDs := make([]string, 0, len(ds.connections))
	for shardID := range ds.connections {
		shardIDs = append(shardIDs, shardID)
	}
	ds.mutex.RUnlock()

	// Channel to collect results from all shards
	type shardResult struct {
		shardID string
		data    []map[string]interface{}
		err     error
	}

	resultChan := make(chan shardResult, len(shardIDs))
	var wg sync.WaitGroup

	// Execute query on each shard concurrently
	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(sID string) {
			defer wg.Done()
			data, err := ds.ExecuteQuery(query, sID)
			resultChan <- shardResult{
				shardID: sID,
				data:    data,
				err:     err,
			}
		}(shardID)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(resultChan)

	// Collect and merge results
	var allResults []map[string]interface{}
	var errors []error

	for result := range resultChan {
		if result.err != nil {
			errors = append(errors, fmt.Errorf("shard %s: %w", result.shardID, result.err))
		} else {
			allResults = append(allResults, result.data...)
		}
	}

	// If there were any errors, return the first one
	if len(errors) > 0 {
		return nil, errors[0]
	}

	return allResults, nil
}

// GetShardMetrics returns real metrics for a shard
func (ds *DataStore) GetShardMetrics(shardID string) (*metrics.ShardMetrics, error) {
	if ds.metricsCollector == nil {
		return nil, fmt.Errorf("metrics collector not initialized")
	}

	return ds.metricsCollector.CollectShardMetrics(shardID)
}



// scanRows converts sql.Rows to a slice of maps
func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a map for this row
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			
			// Convert byte slices to strings for better JSON serialization
			if b, ok := val.([]byte); ok {
				val = string(b)
			}
			
			rowMap[col] = val
		}

		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// Close closes all database connections
func (ds *DataStore) Close() error {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	var errors []error
	for shardID, db := range ds.connections {
		if err := db.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close connection to shard %s: %w", shardID, err))
		}
	}

	if len(errors) > 0 {
		return errors[0]
	}

	return nil
}
