#!/bin/bash
# Overnight ingest runner with auto-recovery
set -e

cd /c/Users/msanc/mxre

LOG_DIR="logs"
SESSION_LOG="$LOG_DIR/overnight-ingest-$(date +%Y%m%d-%H%M%S).log"
INGEST_TIMEOUT=3600  # 60 minutes per county before restart

echo "🌙 Starting overnight ingest session at $(date)"
echo "Session log: $SESSION_LOG"

ATTEMPT=1
MAX_ATTEMPTS=10

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
  echo ""
  echo "=== INGEST ATTEMPT $ATTEMPT ===" | tee -a "$SESSION_LOG"
  echo "Started: $(date)" | tee -a "$SESSION_LOG"

  # Run ingest with timeout - if it hangs >1 hour, kill and restart
  timeout $INGEST_TIMEOUT npx tsx stable-ingest-v3-optimized.ts >> "$SESSION_LOG" 2>&1 &
  INGEST_PID=$!

  # Wait for it to complete or timeout
  wait $INGEST_PID 2>/dev/null
  EXIT_CODE=$?

  if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Ingest completed successfully!" | tee -a "$SESSION_LOG"
    echo "Completed: $(date)" | tee -a "$SESSION_LOG"
    break
  elif [ $EXIT_CODE -eq 124 ]; then
    echo "⏱️ Ingest timed out after ${INGEST_TIMEOUT}s, restarting..." | tee -a "$SESSION_LOG"
  else
    echo "❌ Ingest exited with code $EXIT_CODE, restarting..." | tee -a "$SESSION_LOG"
  fi

  # Brief pause before restart
  sleep 10
  ATTEMPT=$((ATTEMPT + 1))
done

echo ""
echo "🌅 Overnight ingest session ended at $(date)" | tee -a "$SESSION_LOG"
echo "Total attempts: $ATTEMPT" | tee -a "$SESSION_LOG"

# Final stats
echo ""
echo "=== FINAL STATUS ===" | tee -a "$SESSION_LOG"
curl -s http://localhost:3350/api/stats 2>/dev/null | head -c 200 | tee -a "$SESSION_LOG"
echo "" | tee -a "$SESSION_LOG"
