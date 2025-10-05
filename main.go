package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"sql-horizontal-autoscaler/config"
	"sql-horizontal-autoscaler/coordinator"
	"sql-horizontal-autoscaler/datastore"
	"sql-horizontal-autoscaler/router"
	"sql-horizontal-autoscaler/sharding"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	log.Println("Starting SQL Horizontal Autoscaler...")
	log.Printf("Using configuration file: %s", *configFile)

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration with %d shards and %s scaling strategy", 
		len(cfg.Shards), cfg.ScalingStrategy)

	// Initialize datastore
	dataStore := datastore.NewDataStore()

	// Extract table names from configuration
	tableNames := make([]string, 0, len(cfg.TableShardKeys))
	for tableName := range cfg.TableShardKeys {
		tableNames = append(tableNames, tableName)
	}

	if err := dataStore.InitializeConnections(cfg.Shards, tableNames); err != nil {
		log.Fatalf("Failed to initialize database connections: %v", err)
	}
	defer func() {
		if err := dataStore.Close(); err != nil {
			log.Printf("Error closing datastore: %v", err)
		}
	}()

	log.Println("Database connections initialized successfully")

	// Initialize dynamic shard manager
	shardManagerConfig := &sharding.ShardManagerConfig{
		BasePort:                       cfg.Ports.BasePort,
		NetworkName:                    cfg.Docker.NetworkName,
		DatabaseUsername:               cfg.Database.Username,
		DatabasePassword:               cfg.Database.Password,
		DatabaseRootPassword:           cfg.Database.RootPassword,
		DockerImage:                    cfg.Docker.Image,
		ContainerPrefix:                cfg.Docker.ContainerPrefix,
		MaxConnectionAttempts:          cfg.Limits.MaxConnectionAttempts,
		ConnectionRetryIntervalSeconds: cfg.Limits.ConnectionRetryIntervalSeconds,
	}
	shardManager := sharding.NewDynamicShardManager(cfg.Shards, shardManagerConfig)
	log.Printf("Dynamic shard manager initialized with shards: %v", shardManager.GetAllShards())

	// Initialize services
	queryRouter := router.NewQueryRouter(cfg, dataStore, shardManager)
	coordinatorService := coordinator.NewCoordinator(cfg, dataStore, shardManager)

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Start Query Router
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := queryRouter.Start(); err != nil {
			log.Printf("Query Router error: %v", err)
		}
	}()

	// Start Coordinator Service
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := coordinatorService.Start(); err != nil {
			log.Printf("Coordinator Service error: %v", err)
		}
	}()

	log.Println("All services started successfully")
	log.Printf("Query Router available at: http://localhost:%d", cfg.Ports.QueryRouterPort)
	log.Printf("Coordinator Service available at: http://localhost:%d", cfg.Ports.CoordinatorPort)
	log.Println("Press Ctrl+C to shutdown...")

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutdown signal received, stopping services...")

	// Stop coordinator
	coordinatorService.Stop()

	log.Println("Services stopped. Exiting...")
}
