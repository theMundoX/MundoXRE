#!/bin/bash
# Run curated ingest in a loop all night — restarts automatically on completion or crash

cd /c/Users/msanc/mxre

PASS=1
while true; do
  echo "========================================" >> logs/allnight.log
  echo "PASS $PASS started at $(date)" >> logs/allnight.log
  echo "========================================" >> logs/allnight.log

  node_modules/.bin/tsx.cmd curated-ingest.ts >> logs/allnight.log 2>&1
  EXIT=$?

  echo "Pass $PASS ended at $(date) (exit $EXIT)" >> logs/allnight.log
  PASS=$((PASS + 1))

  # Brief pause between passes
  sleep 30
done
