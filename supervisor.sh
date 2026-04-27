#!/bin/bash
# MXRE Supervisor — runs pipelines, logs all failures, moves on to next task automatically.
# Never silently stops. Every failure is logged to logs/supervisor.log.

cd /c/Users/msanc/mxre
LOG="logs/supervisor.log"
TSX="node_modules/.bin/tsx.cmd"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG"
}

run_task() {
  local name="$1"
  local script="$2"
  shift 2
  local args=("$@")

  log "START  $name"
  "$TSX" "$script" "${args[@]}" >> "logs/${name}.log" 2>&1
  local exit_code=$?

  if [ $exit_code -eq 0 ]; then
    log "OK     $name (exit 0)"
  else
    log "FAIL   $name (exit $exit_code) — moving on"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] FAIL $name exit=$exit_code" >> logs/failures.log
  fi
}

log "=========================================="
log "MXRE SUPERVISOR STARTED"
log "=========================================="

# ── PIPELINE A: Statewide parcel ingestion ──────────────────────────
# 18 states, ~40M net new parcels, runs sequentially inside the script
run_task "statewide-parcels" "scripts/ingest-all-states.ts"

# If statewide finishes or fails, continue with ArcGIS bulk (NJ, CO, WA extras)
log "Statewide done — starting ArcGIS bulk for NJ/CO/WA"
run_task "arcgis-bulk-nj" "scripts/ingest-arcgis-bulk.ts" "NJ"
run_task "arcgis-bulk-co" "scripts/ingest-arcgis-bulk.ts" "CO"
run_task "arcgis-bulk-wa" "scripts/ingest-arcgis-bulk.ts" "WA"

# Then backfill any missing county FIPS codes
log "Running FIPS backfill"
run_task "backfill-fips" "scripts/backfill-county-fips.ts"

log "Pipeline A complete"
