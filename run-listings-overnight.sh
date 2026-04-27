#!/bin/bash
# Overnight listings ingestion - all major counties we have assessor data for
# Runs each county sequentially, skips broken ones, loops until 8am

cd /c/Users/msanc/mxre

LOG="logs/listings-overnight-$(date +%Y%m%d-%H%M%S).log"
STOP_EPOCH=$(date -d "tomorrow 08:00" +%s 2>/dev/null || echo $(($(date +%s) + 46800)))

echo "=== LISTINGS OVERNIGHT STARTED $(date) ===" | tee -a "$LOG"

# All counties with confirmed assessor data, grouped by state
COUNTIES=(
  "TX Harris"
  "TX Dallas"
  "TX Tarrant"
  "TX Travis"
  "TX El Paso"
  "OH Franklin"
  "OH Cuyahoga"
  "OH Montgomery"
  "NC Mecklenburg"
  "NC Wake"
  "NC Guilford"
  "NC Durham"
  "FL Hillsborough"
  "FL Orange"
  "FL Palm Beach"
  "FL Broward"
  "AZ Maricopa"
  "AZ Pima"
  "CA Los Angeles"
  "CA Sacramento"
  "CA Alameda"
  "CA San Diego"
  "CA Riverside"
  "MN Hennepin"
  "MN Ramsey"
  "MN Dakota"
  "PA Allegheny"
  "CO Denver"
  "CO Arapahoe"
  "CO Jefferson"
  "CO El Paso"
  "CO Adams"
  "CO Larimer"
  "CO Boulder"
  "NV Clark"
  "WA King"
  "TN Shelby"
  "TN Davidson"
  "GA Fulton"
  "GA DeKalb"
  "IL Cook"
  "IL DuPage"
  "IN Marion"
)

ROUND=0
while true; do
  NOW=$(date +%s)
  if [ "$NOW" -ge "$STOP_EPOCH" ]; then
    echo "=== 8am reached, stopping ===" | tee -a "$LOG"
    break
  fi

  ROUND=$((ROUND + 1))
  echo "" | tee -a "$LOG"
  echo "=== ROUND $ROUND $(date) ===" | tee -a "$LOG"

  for state_county in "${COUNTIES[@]}"; do
    NOW=$(date +%s)
    if [ "$NOW" -ge "$STOP_EPOCH" ]; then break; fi

    STATE=$(echo $state_county | cut -d' ' -f1)
    COUNTY=$(echo $state_county | cut -d' ' -f2)

    echo "  → $STATE $COUNTY" | tee -a "$LOG"
    npx tsx /c/Users/msanc/mxre/scripts/ingest-listings-fast.ts \
      --state "$STATE" --county "$COUNTY" --skip-match \
      >> "$LOG" 2>&1 || true

    # Brief pause between counties
    sleep 3
  done

  echo "=== ROUND $ROUND DONE $(date) ===" | tee -a "$LOG"

  # After each full round, run the address match
  echo "  Running address match..." | tee -a "$LOG"
  ssh -i /tmp/mxre_db_key -o StrictHostKeyChecking=no -o ConnectTimeout=30 root@${DB_HOST:?DB_HOST must be set} \
    'docker exec supabase-db psql -U postgres -d postgres -c "UPDATE listing_signals ls SET property_id = p.id FROM properties p JOIN counties c ON c.id = p.county_id WHERE ls.property_id IS NULL AND ls.state_code = c.state_code AND UPPER(TRIM(ls.city)) = UPPER(TRIM(p.city)) AND UPPER(TRIM(ls.address)) = UPPER(TRIM(p.address));"' \
    >> "$LOG" 2>&1 || echo "  Address match failed (non-fatal)" | tee -a "$LOG"

  sleep 5
done

echo "=== LISTINGS OVERNIGHT COMPLETE $(date) ===" | tee -a "$LOG"
