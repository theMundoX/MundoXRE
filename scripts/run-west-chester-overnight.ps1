$ErrorActionPreference = "Continue"

$Repo = Split-Path -Parent $PSScriptRoot
Set-Location $Repo

$RunId = Get-Date -Format "yyyyMMdd-HHmmss"
$LogDir = Join-Path $Repo "logs\west-chester-overnight\$RunId"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Deadline = (Get-Date).Date.AddDays(1).AddHours(7)
if ((Get-Date) -gt $Deadline) {
  $Deadline = (Get-Date).AddHours(10)
}

function Run-Step {
  param(
    [string]$Name,
    [string]$Command
  )

  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $safeName = ($Name -replace "[^a-zA-Z0-9_-]", "-").ToLowerInvariant()
  $logPath = Join-Path $LogDir "$stamp-$safeName.log"

  "[$(Get-Date -Format o)] RUN $Name" | Tee-Object -FilePath $logPath -Append
  "COMMAND: $Command" | Tee-Object -FilePath $logPath -Append

  cmd.exe /d /s /c $Command *>> $logPath
  $code = $LASTEXITCODE

  "[$(Get-Date -Format o)] EXIT $code $Name" | Tee-Object -FilePath $logPath -Append
  return $code
}

"MXRE West Chester overnight coverage run" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append
"RunId: $RunId" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append
"Deadline: $($Deadline.ToString('o'))" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append

$cycle = 0
while ((Get-Date) -lt $Deadline) {
  $cycle++
  "[$(Get-Date -Format o)] Starting cycle $cycle" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append

  Run-Step "classify-market-assets" 'npx tsx scripts/classify-market-assets.ts --state=PA "--city=WEST CHESTER" --county_id=817175 --batch-size=2500'
  Run-Step "discover-free-websites" 'npx tsx scripts/discover-market-websites-free.ts "--city=West Chester" --state=PA --county_id=817175 --county-slug=chester-county --bbox=39.90,-75.72,40.05,-75.48 --limit=500'
  Run-Step "scrape-rents" 'npx tsx scripts/scrape-rents-bulk.ts --state=PA "--city=WEST CHESTER" --county_id=817175 --stale_days=0 --limit=500'
  Run-Step "redfin-listings-zips" 'npx tsx scripts/ingest-listings-fast.ts --state PA --zips 19380,19381,19382,19383,19388 --concurrency 3'
  Run-Step "daily-listing-scan" 'npx tsx scripts/daily-listing-scan.ts --state PA --cities "West Chester"'
  Run-Step "redfin-detail-pages" 'npx tsx scripts/enrich-redfin-detail-pages.ts --state=PA "--city=WEST CHESTER" --limit=500 --delay-ms=800'
  Run-Step "raw-agent-contact-backfill" 'npx tsx scripts/enrich-listing-agent-contacts.ts --state=PA "--city=WEST CHESTER" --limit=10000'
  Run-Step "public-agent-email-verification" 'npx tsx scripts/enrich-agent-emails-public.ts --state=PA "--city=WEST CHESTER" --limit=500 --delay-ms=900'
  Run-Step "creative-finance-scoring" 'npx tsx scripts/score-creative-finance-signals.ts --state=PA "--city=WEST CHESTER" --limit=10000'
  Run-Step "agent-coverage-audit" 'npx tsx scripts/audit-on-market-agent-coverage.ts --state=PA "--city=WEST CHESTER"'
  Run-Step "readiness-summary" 'npx tsx scripts/market-readiness-summary.ts --state=PA "--city=WEST CHESTER" --county_id=817175'

  "[$(Get-Date -Format o)] Finished cycle $cycle" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append
  Start-Sleep -Seconds 300
}

"[$(Get-Date -Format o)] Overnight run complete" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append
