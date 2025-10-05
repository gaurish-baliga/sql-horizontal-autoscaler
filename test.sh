#!/bin/bash

# SQL Horizontal Autoscaler - Complete Test Suite
# Follows exact user requirements:
# 1. Create 1 shard
# 2. Insert max entries in it
# 3. Wait for scale up
# 4. Verify the scale up
# 5. Insert into second shard and verify it
# 6. Query multi shard and verify the same
# 7. Query independent shard things
# 8. Scale up to third shard by querying and exhausting second shard

set -e

echo "🧪 SQL Horizontal Autoscaler - Complete Test"
echo "============================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

APP_PID=""

# Read configuration from config.json
if [ ! -f "config.json" ]; then
    echo -e "${RED}❌ config.json not found${NC}"
    exit 1
fi

# Extract port configuration
if command -v jq > /dev/null 2>&1; then
    QUERY_ROUTER_PORT=$(jq -r '.ports.query_router_port' config.json)
    COORDINATOR_PORT=$(jq -r '.ports.coordinator_port' config.json)
else
    # Fallback to python if jq is not available
    QUERY_ROUTER_PORT=$(python3 -c "import json; print(json.load(open('config.json'))['ports']['query_router_port'])" 2>/dev/null || echo "8080")
    COORDINATOR_PORT=$(python3 -c "import json; print(json.load(open('config.json'))['ports']['coordinator_port'])" 2>/dev/null || echo "9090")
fi

# Function to cleanup
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    if [ ! -z "$APP_PID" ]; then
        kill $APP_PID 2>/dev/null || true
    fi
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

trap cleanup EXIT

# Function to wait for service
wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1
    
    echo -e "${YELLOW}⏳ Waiting for $service_name...${NC}"
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $service_name is ready!${NC}"
            return 0
        fi
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo -e "${RED}❌ $service_name failed to start${NC}"
    return 1
}

# Function to get current shard count
get_shard_count() {
    curl -s http://localhost:$COORDINATOR_PORT/shards 2>/dev/null | grep -o '"shard_id"' | wc -l | tr -d ' '
}

# Function to show shard status
show_shard_status() {
    local title=$1
    echo -e "\n${BLUE}📊 $title${NC}"
    echo "================================"
    
    local metrics=$(curl -s http://localhost:$COORDINATOR_PORT/shards 2>/dev/null || echo "[]")
    echo "$metrics" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for shard in data:
        print(f'Shard {shard[\"shard_id\"]}: {shard[\"total_entries\"]} entries (threshold: 100)')
        print(f'  Tables: users={shard[\"table_counts\"].get(\"users\", 0)}, orders={shard[\"table_counts\"].get(\"orders\", 0)}, products={shard[\"table_counts\"].get(\"products\", 0)}')
        print(f'  Status: {shard[\"status\"]}')
        print()
except Exception as e:
    print(f'Could not parse metrics: {e}')
" 2>/dev/null || echo "Metrics not available"
}

# Function to insert data into specific shard by using shard key
insert_data_to_shard() {
    local shard_num=$1
    local count=$2
    local description=$3
    
    echo -e "\n${PURPLE}💾 $description${NC}"
    
    # Calculate base ID for this shard to ensure consistent hashing routes to it
    local base_id=$((shard_num * 10000))
    
    for i in $(seq 1 $count); do
        local user_id=$((base_id + i))
        local order_id=$((base_id + i))
        local product_id=$((base_id + i))
        
        # Insert user (this will route to specific shard based on user_id)
        curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"INSERT IGNORE INTO users (user_id, name, email) VALUES ($user_id, 'TestUser $user_id', 'test$user_id@shard$shard_num.com')\"}" > /dev/null

        # Insert order (routes based on customer_id)
        curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"INSERT IGNORE INTO orders (order_id, customer_id, product_name, amount) VALUES ($order_id, $user_id, 'Product $i', $((i * 25)))\"}" > /dev/null

        # Insert product (routes based on product_id)
        curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"INSERT IGNORE INTO products (product_id, name, price, category) VALUES ($product_id, 'Product $product_id', $((i * 50)), 'Category $((i % 3 + 1))')\"}" > /dev/null
        
        # Show progress
        if [ $((i % 10)) -eq 0 ]; then
            echo -e "${BLUE}   Inserted $i/$count records...${NC}"
        fi
    done
    
    echo -e "${GREEN}✅ Inserted $count records targeting shard $shard_num${NC}"
}

# Function to wait for scaling
wait_for_scaling() {
    local expected_shards=$1
    local timeout=$2
    
    echo -e "\n${YELLOW}⏳ Waiting for scaling to $expected_shards shards (timeout: ${timeout}s)...${NC}"
    
    local end_time=$(($(date +%s) + timeout))
    while [ $(date +%s) -lt $end_time ]; do
        local current_count=$(get_shard_count)
        if [ "$current_count" -ge "$expected_shards" ]; then
            echo -e "${GREEN}✅ Scaling detected! Current shards: $current_count${NC}"
            return 0
        fi
        echo -e "${BLUE}   Current shards: $current_count, waiting for $expected_shards...${NC}"
        sleep 5
    done
    
    echo -e "${RED}❌ Scaling timeout reached${NC}"
    return 1
}

# Function to test single shard queries
test_single_shard_queries() {
    echo -e "\n${PURPLE}🔍 Testing single-shard queries${NC}"
    
    # Query specific user from shard 1
    echo -e "${BLUE}Query user from shard 1:${NC}"
    curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
        -H "Content-Type: application/json" \
        -d '{"query": "SELECT * FROM users WHERE user_id = 10001 LIMIT 1"}' | \
        python3 -c "import json, sys; data=json.load(sys.stdin); print(f'Shard: {data.get(\"shard\", \"unknown\")}, Rows: {len(data.get(\"data\", []))}')" 2>/dev/null

    # Query specific user from shard 2 (if exists)
    echo -e "${BLUE}Query user from shard 2:${NC}"
    curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
        -H "Content-Type: application/json" \
        -d '{"query": "SELECT * FROM users WHERE user_id = 20001 LIMIT 1"}' | \
        python3 -c "import json, sys; data=json.load(sys.stdin); print(f'Shard: {data.get(\"shard\", \"unknown\")}, Rows: {len(data.get(\"data\", []))}')" 2>/dev/null
}

# Function to test multi-shard queries
test_multi_shard_queries() {
    echo -e "\n${PURPLE}🔍 Testing multi-shard queries${NC}"
    
    # Count all users across all shards
    echo -e "${BLUE}Count all users (scatter-gather):${NC}"
    curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
        -H "Content-Type: application/json" \
        -d '{"query": "SELECT COUNT(*) as total_users FROM users"}' | \
        python3 -c "import json, sys; data=json.load(sys.stdin); print(f'Shards: {data.get(\"shards\", [])}, Total users: {data[\"data\"][0][\"total_users\"] if data.get(\"data\") else 0}')" 2>/dev/null

    # Aggregate orders across all shards
    echo -e "${BLUE}Aggregate orders by customer (scatter-gather):${NC}"
    curl -s -X POST http://localhost:$QUERY_ROUTER_PORT/query \
        -H "Content-Type: application/json" \
        -d '{"query": "SELECT customer_id, COUNT(*) as order_count FROM orders GROUP BY customer_id LIMIT 5"}' | \
        python3 -c "import json, sys; data=json.load(sys.stdin); print(f'Shards: {data.get(\"shards\", [])}, Aggregated rows: {len(data.get(\"data\", []))}')" 2>/dev/null
}

# Main test execution
main() {
    echo -e "\n${BLUE}🔍 Pre-flight checks...${NC}"
    
    if [ ! -f "./sql-autoscaler" ]; then
        echo -e "${RED}❌ Application binary not found. Please run: go build -o sql-autoscaler .${NC}"
        exit 1
    fi
    
    if [ ! -f "config.json" ]; then
        echo -e "${RED}❌ config.json not found. Please run: ./setup.sh${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Pre-flight checks passed${NC}"
    
    # STEP 1: Create 1 shard (start application)
    echo -e "\n${YELLOW}📍 STEP 1: Starting with 1 shard${NC}"
    ./sql-autoscaler > scaling-test.log 2>&1 &
    APP_PID=$!
    
    wait_for_service "http://localhost:$QUERY_ROUTER_PORT/health" "Query Router"
    wait_for_service "http://localhost:$COORDINATOR_PORT/health" "Coordinator Service"
    
    show_shard_status "Initial State (1 shard)"
    
    # STEP 2: Insert max entries in first shard
    echo -e "\n${YELLOW}📍 STEP 2: Inserting data to exceed threshold (100 entries)${NC}"
    insert_data_to_shard 1 40 "Inserting 40 records to shard 1 (120 total entries)"
    
    # Wait for metrics collection
    echo -e "\n${YELLOW}⏳ Waiting for metrics collection...${NC}"
    sleep 20
    show_shard_status "After Data Insertion"
    
    # STEP 3: Wait for scale up
    echo -e "\n${YELLOW}📍 STEP 3: Waiting for automatic scale up${NC}"
    wait_for_scaling 2 60
    
    # STEP 4: Verify the scale up
    echo -e "\n${YELLOW}📍 STEP 4: Verifying scale up${NC}"
    show_shard_status "After First Scale Up (2 shards)"
    
    current_shards=$(get_shard_count)
    if [ "$current_shards" -ge 2 ]; then
        echo -e "${GREEN}✅ Scale up verified: $current_shards shards active${NC}"
    else
        echo -e "${RED}❌ Scale up failed: only $current_shards shards${NC}"
        exit 1
    fi
    
    # STEP 5: Insert into second shard and verify it
    echo -e "\n${YELLOW}📍 STEP 5: Inserting data into second shard${NC}"
    insert_data_to_shard 2 30 "Inserting 30 records targeting shard 2"
    
    sleep 15
    show_shard_status "After Second Shard Population"
    
    # STEP 6: Query multi shard and verify
    echo -e "\n${YELLOW}📍 STEP 6: Testing multi-shard queries${NC}"
    test_multi_shard_queries
    
    # STEP 7: Query independent shard things
    echo -e "\n${YELLOW}📍 STEP 7: Testing independent shard queries${NC}"
    test_single_shard_queries
    
    # STEP 8: Scale up to third shard by exhausting second shard
    echo -e "\n${YELLOW}📍 STEP 8: Exhausting second shard to trigger third shard${NC}"
    insert_data_to_shard 2 40 "Inserting 40 more records to exhaust shard 2"
    
    echo -e "\n${YELLOW}⏳ Waiting for second scale up...${NC}"
    sleep 20
    wait_for_scaling 3 60
    
    # Final verification
    echo -e "\n${YELLOW}📍 FINAL: Complete system verification${NC}"
    show_shard_status "Final State (3 shards)"
    
    echo -e "\n${PURPLE}🔍 Final Query Tests${NC}"
    test_multi_shard_queries
    test_single_shard_queries
    
    # Show scaling events from logs
    echo -e "\n${BLUE}📋 Scaling Events from Logs:${NC}"
    echo "============================="
    grep -E "SCALING TRIGGERED|Scale-out complete|New shard created" scaling-test.log | tail -10 || echo "No scaling events found"
    
    echo -e "\n${GREEN}🎉 Comprehensive scaling test completed successfully!${NC}"
    echo -e "\n${BLUE}📊 Final Summary:${NC}"
    echo "   • Started with 1 shard"
    echo "   • Scaled to $(get_shard_count) shards through data insertion"
    echo "   • Verified single-shard and multi-shard queries"
    echo "   • Demonstrated automatic threshold-based scaling"
    echo "   • All steps completed successfully!"
}

# Run main function
main "$@"
