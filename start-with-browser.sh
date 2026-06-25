#!/bin/bash

# Start docker compose in detached mode
docker compose up -d

# Wait for Neo4j to be ready
echo "Waiting for Neo4j to start..."
sleep 10

# Check if Neo4j is responding
while ! curl -s http://localhost:7474 > /dev/null; do
  echo "Neo4j not ready yet, waiting..."
  sleep 2
done

echo "Neo4j is ready! Opening browser..."

# Open Neo4j browser in default browser with pre-filled username
open "http://localhost:7474/browser/?cmd=connect&arg=neo4j://neo4j:your_new_password@localhost:7687"

# Keep compose running in foreground
docker compose logs -f
