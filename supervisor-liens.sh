#!/bin/bash
# MXRE Supervisor — Lien/Mortgage pipeline. Never silently stops.

cd /c/Users/msanc/mxre
LOG="logs/supervisor-liens.log"
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
log "MXRE LIEN SUPERVISOR STARTED"
log "=========================================="

# ── PIPELINE B: Mortgage / lien data ───────────────────────────────
# Fidlar AVA — 28 counties, actual recorded docs, 60-day lookback
run_task "fidlar-fast" "scripts/ingest-fidlar-fast.ts"

# If Fidlar finishes/fails, run daily recorder pull (broader set)
log "Fidlar done — starting daily recorder pull"
run_task "daily-recorder" "scripts/daily-recorder-pull.ts"

# Then link mortgages to properties
log "Linking mortgages to properties"
run_task "link-mortgages" "scripts/link-mortgages-v3.ts"

# Then verify liens
log "Verifying liens"
run_task "verify-liens" "scripts/verify-liens.ts"

log "Pipeline B complete"
