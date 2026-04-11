#!/bin/bash
# Full MXRE Pipeline - All 3 Layers

echo "=========================================="
echo "FULL MXRE PIPELINE - NATIONWIDE COVERAGE"
echo "=========================================="
echo ""
echo "LAYER 1: Assessor/Parcel Data (182 counties, 50 concurrent)"
echo "LAYER 2: Rental Data (RentCafe, property websites)"
echo "LAYER 3: Mortgage/Lien Data (County recorder filings)"
echo ""

# Layer 1: Assessor data
echo "[$(date '+%H:%M:%S')] Starting LAYER 1: Assessor ingest..."
nohup npx tsx run-parallel-ingest.ts > /tmp/mxre-layer1-assessor.log 2>&1 &
LAYER1_PID=$!
echo "Layer 1 PID: $LAYER1_PID"
echo $LAYER1_PID > /tmp/mxre-layer1.pid

sleep 10

# Layer 2: Rental data
echo "[$(date '+%H:%M:%S')] Starting LAYER 2: Rental data scrape..."
nohup npx tsx scripts/scrape-rents.ts --discover > /tmp/mxre-layer2-rentals.log 2>&1 &
LAYER2_PID=$!
echo "Layer 2 PID: $LAYER2_PID"
echo $LAYER2_PID > /tmp/mxre-layer2.pid

sleep 5

# Layer 3: Mortgage data
echo "[$(date '+%H:%M:%S')] Starting LAYER 3: Mortgage/lien linking..."
nohup npx tsx scripts/link-mortgages-v3.ts > /tmp/mxre-layer3-mortgages.log 2>&1 &
LAYER3_PID=$!
echo "Layer 3 PID: $LAYER3_PID"
echo $LAYER3_PID > /tmp/mxre-layer3.pid

echo ""
echo "=========================================="
echo "FULL PIPELINE RUNNING - 3 CONCURRENT LAYERS"
echo "=========================================="
echo ""
echo "Monitor progress:"
echo "  Layer 1 (Assessor): tail -f /tmp/mxre-layer1-assessor.log"
echo "  Layer 2 (Rentals):  tail -f /tmp/mxre-layer2-rentals.log"
echo "  Layer 3 (Mortgages): tail -f /tmp/mxre-layer3-mortgages.log"
echo ""
