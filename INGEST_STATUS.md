# MXRE Ingest Status

**Last Updated:** 2026-04-06 05:38 UTC

## Current Status
✅ **Process Running** - stable-ingest-v2.ts
🔄 **Progress:** 30,000 records processed (0.08% of 40M target)
⏱️ **Elapsed:** ~150 seconds (2.5 minutes)
📈 **Rate:** ~200 records/second

## Process Details
- **PID:** See `/tmp/stable-ingest-v2.pid`
- **Log:** `/tmp/stable-ingest-v2.log`
- **Concurrency:** 3 counties (ultra-conservative for Windows stability)
- **Batch Size:** 100 properties (reduced from 500 for stability)
- **Configuration:** BATCH_SIZE=100, CONCURRENCY=3, 50ms delay between batches

## Performance Characteristics
- **Stable:** No crashes yet (unlike v1 which crashed at 250k)
- **Moderate Rate:** ~200 rec/sec is 2x slower than v1 peak, but consistent
- **High Error Rate on APIs:** Many county endpoints return 404/400 errors (may need registry updates)

## Estimated Timeline at Current Rate
- Current: 30,000 records
- Rate: 200 rec/second = 0.2M per hour
- **For 40M target:** ~200 hours (8.3 days)

## Next Steps
1. ✅ Process running stably - continue monitoring
2. 🔧 Investigate ArcGIS endpoint failures (404/400 errors)
3. 📊 Consider increasing CONCURRENCY to 5-8 after stability confirms
4. 🎯 If can fix API issues, could reach 40M in 24-48 hours

## Previous Attempts
- **v1:** Processed 250k+ in ~3 min (83k rec/sec peak) but crashed due to memory exhaustion
- **Cause:** Windows fork/VirtualAlloc limits at CONCURRENCY=10, BATCH_SIZE=500
- **Fix:** Reduced to CONCURRENCY=3, BATCH_SIZE=100 for stability
