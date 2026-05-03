param(
  [string]$Port = "3101",
  [string]$TunnelName = "mundox-gateway",
  [string]$OriginHostname = "mxre-origin.mundox.ai",
  [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

function Stop-MatchingProcess {
  param([string]$Pattern)
  Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match $Pattern } |
    ForEach-Object {
      Write-Host "Stopping process $($_.ProcessId): $($_.CommandLine)"
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Wait-For-HttpOk {
  param(
    [string]$Url,
    [hashtable]$Headers = @{},
    [int]$Attempts = 30
  )
  for ($i = 0; $i -lt $Attempts; $i++) {
    try {
      $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -Headers $Headers -TimeoutSec 8
      if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) { return $response }
    } catch {}
    Start-Sleep -Seconds 2
  }
  throw "Timed out waiting for $Url"
}

function Read-DotEnvValue {
  param([string]$Name)
  $line = Get-Content ".env" -ErrorAction SilentlyContinue | Where-Object { $_ -match "^$Name=" } | Select-Object -First 1
  if (!$line) { return $null }
  return ($line -replace "^$Name=", "").Trim('"')
}

Push-Location (Split-Path $PSScriptRoot -Parent)
try {
  New-Item -ItemType Directory -Force "logs" | Out-Null

  $cloudflared = Join-Path $env:APPDATA "xdg.config\.wrangler\cloudflared\2026.3.0\cloudflared.exe"
  if (!(Test-Path $cloudflared)) {
    $cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
    if ($cmd) { $cloudflared = $cmd.Source }
  }
  if (!(Test-Path $cloudflared)) {
    throw "cloudflared was not found. Run npx wrangler tunnel list once to install the bundled cloudflared runtime."
  }

  $configPath = Join-Path $env:USERPROFILE ".cloudflared\config.yml"
  if (!(Test-Path $configPath)) {
    throw "Cloudflare tunnel config not found at $configPath"
  }

  if (!$NoBuild) {
    Write-Host "Building MXRE API..."
    npm run build
  }

  Stop-MatchingProcess "dist/api/server.js"
  Stop-MatchingProcess "cloudflared.*$TunnelName|cloudflared.*trycloudflare|cloudflared.*tunnel --url"

  $apiOut = "logs\api-named-origin.out.log"
  $apiErr = "logs\api-named-origin.err.log"
  Remove-Item $apiOut, $apiErr -ErrorAction SilentlyContinue

  $env:PORT = $Port
  Write-Host "Starting MXRE Node API on port $Port..."
  Start-Process -FilePath "node" -ArgumentList "dist/api/server.js" -WorkingDirectory (Get-Location) -WindowStyle Hidden -RedirectStandardOutput $apiOut -RedirectStandardError $apiErr
  Wait-For-HttpOk "http://127.0.0.1:$Port/health" | Out-Null

  $tunnelOut = "logs\cloudflared-named.out.log"
  $tunnelErr = "logs\cloudflared-named.err.log"
  Remove-Item $tunnelOut, $tunnelErr -ErrorAction SilentlyContinue

  Write-Host "Starting named Cloudflare tunnel $TunnelName..."
  Start-Process -FilePath $cloudflared -ArgumentList @("tunnel", "--config", $configPath, "run", $TunnelName) -WorkingDirectory (Get-Location) -WindowStyle Hidden -RedirectStandardOutput $tunnelOut -RedirectStandardError $tunnelErr
  Wait-For-HttpOk "https://$OriginHostname/health" | Out-Null

  $upstreamKey = Read-DotEnvValue "MXRE_API_KEY"
  if ($upstreamKey) {
    $headers = @{ "x-client-id" = "legacy"; "x-api-key" = $upstreamKey }
    Wait-For-HttpOk "https://$OriginHostname/v1/bbc/markets/indianapolis/creative-finance-listings?status=positive&limit=1" $headers | Out-Null
  }

  Write-Host "MXRE named tunnel origin is healthy: https://$OriginHostname"
} finally {
  Pop-Location
}
