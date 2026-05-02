$ErrorActionPreference = "Continue"

$repo = "C:\Users\msanc\mxre"
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logDir = Join-Path $repo "logs\market-refresh"
New-Item -ItemType Directory -Force $logDir | Out-Null

Set-Location $repo

function Run-Step {
  param(
    [string] $Name,
    [string] $Command
  )

  $safeName = $Name.ToLowerInvariant().Replace(" ", "-")
  $logPath = Join-Path $logDir "columbus-overnight-$safeName-$stamp.log"
  "[$(Get-Date -Format o)] START $Name" | Tee-Object -FilePath $logPath -Append
  cmd /c "$Command" 2>&1 | Tee-Object -FilePath $logPath -Append
  "[$(Get-Date -Format o)] END $Name exit=$LASTEXITCODE" | Tee-Object -FilePath $logPath -Append
}

Run-Step "redfin-detail-tail" "npx tsx scripts/enrich-redfin-detail-pages.ts --state=OH --city=COLUMBUS --limit=300 --delay-ms=750"
Run-Step "creative-score" "npx tsx scripts/score-creative-finance-signals.ts --state=OH --city=COLUMBUS --limit=5000"
Run-Step "public-agent-email" "npx tsx scripts/enrich-agent-emails-public.ts --state=OH --city=COLUMBUS --limit=100 --delay-ms=1000"
Run-Step "rent-scrape" "npx tsx scripts/scrape-rents-bulk.ts --state=OH --city=Columbus --county_id=1698985 --stale_days=0 --limit=120"
Run-Step "agent-audit" "npx tsx scripts/audit-on-market-agent-coverage.ts --state=OH --city=COLUMBUS"
Run-Step "rent-audit" "npx tsx scripts/audit-indy-multifamily-rent-coverage.ts --state=OH --city=COLUMBUS --county_id=1698985"
Run-Step "readiness" "npx tsx scripts/market-readiness-summary.ts --state=OH --city=COLUMBUS --county_id=1698985"
