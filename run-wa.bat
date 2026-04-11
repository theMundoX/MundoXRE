@echo off
cd /d C:\Users\msanc\mxre
npx tsx scripts\ingest-arcgis-bulk.ts WA > data\wa-retry.log 2>&1
