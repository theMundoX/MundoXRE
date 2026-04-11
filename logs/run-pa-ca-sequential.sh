#!/bin/bash
NY_LOG="/c/Users/msanc/mxre/logs/ingest-ny-20260330-002021.log"

echo "$(date): Polling for NY ingest completion..."
while true; do
  if grep -q "^Done!" "$NY_LOG" 2>/dev/null; then
    echo "$(date): NY ingest DONE. Starting PA ingest..."
    break
  fi
  sleep 60
done

cd /c/Users/msanc/mxre
PA_LOG="/c/Users/msanc/mxre/logs/ingest-pa-$(date +%Y%m%d-%H%M%S).log"
echo "PA log: $PA_LOG"
npx tsx scripts/ingest-pa-statewide.ts > "$PA_LOG" 2>&1

echo "$(date): PA ingest complete. Starting CA ingest..."
CA_LOG="/c/Users/msanc/mxre/logs/ingest-ca-$(date +%Y%m%d-%H%M%S).log"
echo "CA log: $CA_LOG"
npx tsx scripts/ingest-ca-parcels.ts > "$CA_LOG" 2>&1

echo "$(date): All ingests complete."
