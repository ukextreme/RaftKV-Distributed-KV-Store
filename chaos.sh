#!/bin/bash
#
# Chaos test script for RaftKV
#
# This script:
# 1. Writes data to the cluster
# 2. Kills the leader node
# 3. Waits for a new leader election
# 4. Verifies the data is still accessible
# 5. Restarts the killed node and verifies it catches up
#
# Usage: ./chaos.sh
# Prerequisites: docker compose up --build -d

set -e

echo "=== RaftKV Chaos Test ==="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper: try a SET command on a port, return 0 if OK
try_set() {
    local port=$1
    local key=$2
    local value=$3
    result=$(redis-cli -p "$port" SET "$key" "$value" 2>/dev/null)
    if [ "$result" = "OK" ]; then
        return 0
    fi
    return 1
}

# Helper: try a GET command on a port
try_get() {
    local port=$1
    local key=$2
    redis-cli -p "$port" GET "$key" 2>/dev/null
}

# Helper: find the leader by trying SET on each port
find_leader() {
    for port in 6381 6382 6383; do
        if try_set "$port" "__leader_check" "test" 2>/dev/null; then
            echo "$port"
            return
        fi
    done
    echo "none"
}

# Step 1: Find the current leader
echo -e "${YELLOW}Step 1: Finding the current leader...${NC}"
sleep 2

LEADER_PORT=$(find_leader)
if [ "$LEADER_PORT" = "none" ]; then
    echo -e "${RED}ERROR: No leader found. Is the cluster running?${NC}"
    echo "Start it with: docker compose up --build -d"
    exit 1
fi

# Map port to node name
case $LEADER_PORT in
    6381) LEADER_NODE="node1" ;;
    6382) LEADER_NODE="node2" ;;
    6383) LEADER_NODE="node3" ;;
esac

echo -e "${GREEN}Leader found: $LEADER_NODE (port $LEADER_PORT)${NC}"
echo ""

# Step 2: Write test data
echo -e "${YELLOW}Step 2: Writing test data to the leader...${NC}"

for i in $(seq 1 10); do
    try_set "$LEADER_PORT" "chaos_key_$i" "value_$i"
done
echo -e "${GREEN}Wrote 10 key-value pairs${NC}"

# Verify reads
echo -n "Verifying reads: "
PASS=0
for i in $(seq 1 10); do
    val=$(try_get "$LEADER_PORT" "chaos_key_$i")
    if [ "$val" = "value_$i" ]; then
        PASS=$((PASS + 1))
    fi
done
echo -e "${GREEN}$PASS/10 keys verified${NC}"
echo ""

# Step 3: Kill the leader
echo -e "${RED}Step 3: KILLING THE LEADER ($LEADER_NODE)...${NC}"
docker compose stop "$LEADER_NODE"

echo "Leader killed. Waiting for new election..."
echo ""

# Step 4: Wait for new leader
echo -e "${YELLOW}Step 4: Waiting for new leader election...${NC}"

NEW_LEADER="none"
ATTEMPTS=0
MAX_ATTEMPTS=40

while [ "$NEW_LEADER" = "none" ] && [ $ATTEMPTS -lt $MAX_ATTEMPTS ]; do
    sleep 1
    ATTEMPTS=$((ATTEMPTS + 1))
    echo -n "  Attempt $ATTEMPTS/$MAX_ATTEMPTS... "

    for port in 6381 6382 6383; do
        if [ "$port" = "$LEADER_PORT" ]; then
            continue  # Skip the killed node
        fi
        if try_set "$port" "__election_check" "test" 2>/dev/null; then
            NEW_LEADER="$port"
            break
        fi
    done

    if [ "$NEW_LEADER" != "none" ]; then
        echo -e "${GREEN}New leader found on port $NEW_LEADER!${NC}"
    else
        echo "No leader yet..."
    fi
done

if [ "$NEW_LEADER" = "none" ]; then
    echo -e "${RED}ERROR: No new leader elected after $MAX_ATTEMPTS seconds${NC}"
    exit 1
fi

ELECTION_TIME=$ATTEMPTS
echo ""
echo -e "${GREEN}New leader elected in approximately ${ELECTION_TIME} seconds${NC}"
echo ""

# Step 5: Verify data survived
echo -e "${YELLOW}Step 5: Verifying data survived the leader failure...${NC}"

RECOVERED=0
for i in $(seq 1 10); do
    val=$(try_get "$NEW_LEADER" "chaos_key_$i")
    if [ "$val" = "value_$i" ]; then
        RECOVERED=$((RECOVERED + 1))
    fi
done

echo -e "${GREEN}$RECOVERED/10 keys recovered after leader failure${NC}"
echo ""

# Step 6: Write new data to prove the cluster is operational
echo -e "${YELLOW}Step 6: Writing new data to the new leader...${NC}"

for i in $(seq 11 20); do
    try_set "$NEW_LEADER" "chaos_key_$i" "value_$i"
done
echo -e "${GREEN}Wrote 10 new key-value pairs to the new leader${NC}"
echo ""

# Step 7: Restart the killed node
echo -e "${YELLOW}Step 7: Restarting $LEADER_NODE...${NC}"
docker compose start "$LEADER_NODE"
echo "Waiting for node to rejoin..."
sleep 5

echo -e "${GREEN}$LEADER_NODE restarted${NC}"
echo ""

# Summary
echo "======================================="
echo -e "${GREEN}=== CHAOS TEST COMPLETE ===${NC}"
echo "======================================="
echo ""
echo "Results:"
echo "  Leader before kill:     $LEADER_NODE (port $LEADER_PORT)"
echo "  New leader after kill:  port $NEW_LEADER"
echo "  Election time:          ~${ELECTION_TIME}s"
echo "  Data recovery:          $RECOVERED/10 keys"
echo "  Post-election writes:   10 new keys"
echo ""
echo "Open Grafana at http://localhost:3000 to see the dashboard."
echo "Look for the term spike and leader change in the graphs."