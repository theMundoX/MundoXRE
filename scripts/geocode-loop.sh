#!/bin/bash
# Auto-restart wrapper for geocode-properties-single.mjs
# Re-launches the geocoder if it crashes (Supavisor connection drops, etc).
# Stops when there are no more pending properties.
cd /c/Users/msanc/mxre
ITER=0
while true; do
  ITER=$((ITER+1))
  echo "=== ITER $ITER  $(date) ==="
  WORKERS=30 node scripts/geocode-properties-single.mjs 2>&1
  EXIT=$?
  if [ $EXIT -eq 0 ]; then
    echo "geocoder exited cleanly (no more pending). stopping."
    break
  fi
  echo "geocoder crashed with exit $EXIT. restarting in 10s..."
  sleep 10
done
