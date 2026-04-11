#!/bin/bash
# Wait for NY ingest to complete, then run PA and CA
NY_PID=$(cat /c/Users/msanc/mxre/logs/ny-ingest.pid 2>/dev/null)
if [ -n "$NY_PID" ]; then
  echo "$(date): Waiting for NY ingest (PID $NY_PID) to complete..."
  wait $NY_PID
  echo "$(date): NY ingest complete."
fi

echo "$(date): Starting PA ingest..."
cd /c/Users/msanc/mxre
npx tsx scripts/ingest-pa-statewide.ts > /c/Users/msanc/mxre/logs/ingest-pa-$(date +%Y%m%d-%H%M%S).log 2>&1
echo "$(date): PA ingest complete."

echo "$(date): Starting CA ingest..."
npx tsx scripts/ingest-ca-parcels.ts > /c/Users/msanc/mxre/logs/ingest-ca-$(date +%Y%m%d-%H%M%S).log 2>&1
echo "$(date): CA ingest complete."
