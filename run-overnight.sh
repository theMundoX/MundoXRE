#!/bin/bash
# Overnight ingest loop - runs curated-ingest repeatedly until morning
# Kills itself at 8am or after 20 rounds

set -e
cd /c/Users/msanc/mxre

LOG="logs/overnight-ingest-$(date +%Y%m%d-%H%M%S).log"
ROUND=0
MAX_ROUNDS=20
STOP_EPOCH=$(date -d "tomorrow 08:00" +%s 2>/dev/null || date -v+1d -j -f "%H:%M" "08:00" +%s 2>/dev/null || echo $(($(date +%s) + 46800)))

echo "=== OVERNIGHT INGEST STARTED $(date) ===" | tee -a "$LOG"
echo "Max rounds: $MAX_ROUNDS | Will stop at 8:00am tomorrow" | tee -a "$LOG"

while [ $ROUND -lt $MAX_ROUNDS ]; do
  # Stop if it's past 8am tomorrow
  NOW=$(date +%s)
  if [ "$NOW" -ge "$STOP_EPOCH" ]; then
    echo "=== 8am reached, stopping overnight ingest ===" | tee -a "$LOG"
    break
  fi

  ROUND=$((ROUND + 1))
  echo "" | tee -a "$LOG"
  echo "=== ROUND $ROUND START $(date) ===" | tee -a "$LOG"

  npx tsx curated-ingest.ts >> "$LOG" 2>&1
  EXIT=$?

  echo "=== ROUND $ROUND DONE (exit $EXIT) $(date) ===" | tee -a "$LOG"

  # Quick pause between rounds
  sleep 5
done

echo "" | tee -a "$LOG"
echo "=== OVERNIGHT INGEST COMPLETE $(date) ===" | tee -a "$LOG"
echo "Total rounds completed: $ROUND" | tee -a "$LOG"
