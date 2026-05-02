$ErrorActionPreference = "Continue"

$Repo = Split-Path -Parent $PSScriptRoot
Set-Location $Repo

$RunId = Get-Date -Format "yyyyMMdd-HHmmss"
$LogDir = Join-Path $Repo "logs\market-refresh\indianapolis-daily-$RunId"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Run-Step {
  param(
    [string]$Name,
    [string]$Command
  )

  $safeName = ($Name -replace "[^a-zA-Z0-9_-]", "-").ToLowerInvariant()
  $logPath = Join-Path $LogDir "$safeName.log"

  "[$(Get-Date -Format o)] START $Name" | Tee-Object -FilePath $logPath -Append
  "COMMAND: $Command" | Tee-Object -FilePath $logPath -Append
  cmd.exe /d /s /c $Command *>> $logPath
  $code = $LASTEXITCODE
  "[$(Get-Date -Format o)] END $Name exit=$code" | Tee-Object -FilePath $logPath -Append
  return $code
}

"MXRE Indianapolis daily refresh" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append
"RunId: $RunId" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append

Run-Step "classify-assets" 'npx tsx scripts/classify-indy-assets.ts'
Run-Step "public-signals" 'npx tsx scripts/ingest-indy-public-signals.ts'
Run-Step "transit" 'npx tsx scripts/ingest-indygo-transit.ts'
Run-Step "crime" 'npx tsx scripts/ingest-impd-crime.ts'
Run-Step "location-scores" 'npx tsx scripts/update-indy-location-scores.ts'
Run-Step "redfin-detail-pages" 'npx tsx scripts/enrich-redfin-detail-pages.ts --state=IN --city=INDIANAPOLIS --limit=2500 --delay-ms=700'
Run-Step "listing-agent-contacts" 'npx tsx scripts/enrich-listing-agent-contacts.ts --state=IN --city=INDIANAPOLIS --limit=10000'
Run-Step "public-agent-emails" 'npx tsx scripts/enrich-agent-emails-public.ts --state=IN --city=INDIANAPOLIS --limit=500 --delay-ms=900'
Run-Step "creative-finance" 'npx tsx scripts/score-creative-finance-signals.ts --state=IN --city=INDIANAPOLIS --limit=10000'
Run-Step "rent-scrape" 'npx tsx scripts/scrape-rents-bulk.ts --state=IN --city=INDIANAPOLIS --county_id=797583 --stale_days=0 --limit=500'
Run-Step "agent-coverage-audit" 'npx tsx scripts/audit-on-market-agent-coverage.ts --state=IN --city=INDIANAPOLIS'
Run-Step "multifamily-rent-audit" 'npx tsx scripts/audit-indy-multifamily-rent-coverage.ts --state=IN --city=INDIANAPOLIS --county_id=797583'
Run-Step "readiness-summary" 'npx tsx scripts/market-readiness-summary.ts --state=IN --city=INDIANAPOLIS --county_id=797583'

"[$(Get-Date -Format o)] Indianapolis daily refresh complete" | Tee-Object -FilePath (Join-Path $LogDir "README.log") -Append
