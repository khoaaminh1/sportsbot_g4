#!/usr/bin/env bash
# Create the SportsBot directory layout in HDFS. Idempotent: safe to run on every start.
# Run by the hdfs-init service (make hdfs-init).
set -euo pipefail

echo "Waiting for the NameNode to leave safe mode..."
hdfs dfsadmin -safemode wait

echo "Waiting for 2 live DataNodes..."
for _ in $(seq 1 60); do
  live=$(hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p' | head -n1)
  if [ "${live:-0}" -ge 2 ]; then break; fi
  sleep 2
done
echo "Live DataNodes: ${live:-0}"
if [ "${live:-0}" -lt 2 ]; then
  echo "ERROR: expected 2 live DataNodes" >&2
  exit 1
fi

hdfs dfs -mkdir -p /sportsbot/bronze /sportsbot/silver /sportsbot/gold /sportsbot/_smoke
# Development only: permissions are disabled in hdfs-site, this keeps the intent explicit.
hdfs dfs -chmod -R 777 /sportsbot
hdfs dfs -ls /sportsbot
echo "HDFS layout ready."
