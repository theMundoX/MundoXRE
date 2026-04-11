#!/bin/bash
# MXRE Progress Watcher - writes to file every minute

PROGRESS_FILE="/tmp/mxre-progress.txt"

echo "Starting progress watcher - writing to: $PROGRESS_FILE"
echo ""

while true; do
  {
    echo "=== MXRE INGESTION PROGRESS ==="
    echo "Updated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""

    # Query database
    RESPONSE=$(curl -s -m 5 \
      -H "apikey: eyJhbGciOiAiSFMyNTYiLCAidHlwIjogInNlcnZpY2Vfcm9sZSIsICJpc3MiOiAic3VwYWJhc2UiLCAiaWF0IjogMTc3NDUyNDk2MiwgImV4cCI6IDIwODk4ODQ5NjJ9.Ex_u9UIYpPmJ0G8H3deic-zRulLOmgNZJS3hw7azoKU" \
      "http://207.244.225.239:8000/rest/v1/properties?select=id&limit=1" 2>/dev/null)

    if echo "$RESPONSE" | grep -q '"id"'; then
      COUNT=$(echo "$RESPONSE" | wc -c)
      echo "✓ Database responding"
      echo "✓ Response size: $COUNT bytes"
    else
      echo "✗ Database connection issue"
    fi

    echo ""
    echo "Ingest running: $(ps aux | grep 'run-parallel-ingest' | grep -v grep | wc -l) process(es)"
    echo "Dashboard running: $(ps aux | grep 'command-center' | grep -v grep | wc -l) process(es)"

  } > "$PROGRESS_FILE"

  sleep 60
done
