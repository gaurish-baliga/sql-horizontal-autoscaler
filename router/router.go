package router

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"sql-horizontal-autoscaler/config"
	"sql-horizontal-autoscaler/datastore"
	"sql-horizontal-autoscaler/parser"
	"sql-horizontal-autoscaler/sharding"
)

// QueryRouter handles HTTP requests for SQL query routing
type QueryRouter struct {
	config       *config.Config
	dataStore    *datastore.DataStore
	shardManager *sharding.DynamicShardManager
}

// QueryRequest represents the incoming query request
type QueryRequest struct {
	Query string `json:"query"`
}

// QueryResponse represents the response to a query
type QueryResponse struct {
	Data   []map[string]interface{} `json:"data"`
	Shard  string                   `json:"shard,omitempty"`
	Shards []string                 `json:"shards,omitempty"`
	Error  string                   `json:"error,omitempty"`
}

// NewQueryRouter creates a new QueryRouter instance
func NewQueryRouter(cfg *config.Config, ds *datastore.DataStore, sm *sharding.DynamicShardManager) *QueryRouter {
	return &QueryRouter{
		config:       cfg,
		dataStore:    ds,
		shardManager: sm,
	}
}

// Start starts the HTTP server for the query router
func (qr *QueryRouter) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", qr.handleQuery)
	mux.HandleFunc("/health", qr.handleHealth)

	port := fmt.Sprintf(":%d", qr.config.Ports.QueryRouterPort)
	log.Printf("Query Router starting on port %d...", qr.config.Ports.QueryRouterPort)
	return http.ListenAndServe(port, mux)
}

// handleQuery handles POST /query requests
func (qr *QueryRouter) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		qr.sendErrorResponse(w, "Invalid JSON request", http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		qr.sendErrorResponse(w, "Query cannot be empty", http.StatusBadRequest)
		return
	}

	log.Printf("Received query: %s", req.Query)

	// Parse the SQL query to extract shard key information
	parseResult, err := parser.Parse(req.Query, qr.config.TableShardKeys)
	if err != nil {
		log.Printf("Failed to parse query: %v", err)
		qr.sendErrorResponse(w, fmt.Sprintf("Failed to parse query: %v", err), http.StatusBadRequest)
		return
	}

	var response QueryResponse

	if parseResult.HasShardKey {
		// Single shard query - use consistent hashing to determine target shard
		shardKeyStr := fmt.Sprintf("%v", parseResult.ShardKeyValue)
		targetShard, err := qr.shardManager.GetShard(shardKeyStr)
		if err != nil {
			log.Printf("Failed to determine target shard: %v", err)
			qr.sendErrorResponse(w, fmt.Sprintf("Failed to determine target shard: %v", err), http.StatusInternalServerError)
			return
		}

		log.Printf("Routing query to single shard: %s (key: %s)", targetShard, shardKeyStr)

		// Execute query on the target shard
		data, err := qr.dataStore.ExecuteQuery(req.Query, targetShard)
		if err != nil {
			log.Printf("Failed to execute query on shard %s: %v", targetShard, err)
			qr.sendErrorResponse(w, fmt.Sprintf("Failed to execute query: %v", err), http.StatusInternalServerError)
			return
		}

		response = QueryResponse{
			Data:  data,
			Shard: targetShard,
		}
	} else {
		// Scatter-gather query - execute on all shards
		log.Printf("Performing scatter-gather query across all shards")

		data, err := qr.dataStore.ExecuteQueryOnAllShards(req.Query)
		if err != nil {
			log.Printf("Failed to execute scatter-gather query: %v", err)
			qr.sendErrorResponse(w, fmt.Sprintf("Failed to execute query: %v", err), http.StatusInternalServerError)
			return
		}

		response = QueryResponse{
			Data:   data,
			Shards: qr.shardManager.GetAllShards(),
		}
	}

	// Send successful response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
	}

	log.Printf("Query executed successfully, returned %d rows", len(response.Data))
}

// handleHealth handles GET /health requests
func (qr *QueryRouter) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	health := map[string]interface{}{
		"status": "healthy",
		"service": "query-router",
		"shards": qr.shardManager.GetAllShards(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// sendErrorResponse sends an error response
func (qr *QueryRouter) sendErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := QueryResponse{
		Error: message,
	}

	json.NewEncoder(w).Encode(response)
}
