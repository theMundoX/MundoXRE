param(
  [string]$Port = "3101",
  [string]$ClientId = "buy_box_club_prod",
  [string]$WorkerUrl = "https://mxre-api-gateway.munsanco.workers.dev",
  [string]$SmokePath = "/v1/markets/indianapolis/reports/creative-finance?limit=1"
)

$ErrorActionPreference = "Stop"

function Read-DotEnvValue {
  param([string]$Name)
  $line = Get-Content ".env" -ErrorAction SilentlyContinue | Where-Object { $_ -match "^$Name=" } | Select-Object -First 1
  if (!$line) { return $null }
  return $line -replace "^$Name=", ""
}

function Stop-Existing {
  Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match "dist/api/server.js|cloudflared.*127.0.0.1:$Port|cloudflared.*localhost:$Port" } |
    ForEach-Object {
      Write-Host "Stopping existing process $($_.ProcessId): $($_.CommandLine)"
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Get-CloudflaredPath {
  $candidate = Join-Path $env:APPDATA "xdg.config\.wrangler\cloudflared\2026.3.0\cloudflared.exe"
  if (Test-Path $candidate) { return $candidate }

  $cmd = Get-Command cloudflared -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  throw "cloudflared was not found. Run `npx wrangler tunnel quick-start http://127.0.0.1:$Port` once so Wrangler downloads it."
}

function Wait-For-Health {
  $health = "http://127.0.0.1:$Port/health"
  for ($i = 0; $i -lt 20; $i++) {
    try {
      $response = Invoke-WebRequest -UseBasicParsing -Uri $health -TimeoutSec 5
      if ($response.StatusCode -eq 200) { return }
    } catch {}
    Start-Sleep -Seconds 1
  }
  throw "Local MXRE API did not become healthy at $health"
}

function Wait-For-TunnelUrl {
  param([string]$LogPath)
  for ($i = 0; $i -lt 30; $i++) {
    $text = Get-Content $LogPath -Raw -ErrorAction SilentlyContinue
    if (!$text) {
      Start-Sleep -Seconds 1
      continue
    }
    $match = [regex]::Match($text, "https://[a-z0-9-]+\.trycloudflare\.com")
    if ($match.Success) { return $match.Value }
    Start-Sleep -Seconds 1
  }
  throw "Cloudflare tunnel URL was not found in $LogPath"
}

Push-Location (Split-Path $PSScriptRoot -Parent)
try {
  New-Item -ItemType Directory -Force "logs" | Out-Null

  $clientKey = $env:MXRE_BUY_BOX_CLUB_KEY
  if (!$clientKey) {
    $clientKey = Read-DotEnvValue "MXRE_BUY_BOX_CLUB_KEY"
  }
  if (!$clientKey) {
    $clientJson = Read-DotEnvValue "MXRE_CLIENT_API_KEYS"
    if ($clientJson) {
      try {
        $clientKey = (($clientJson | ConvertFrom-Json) | Where-Object { $_.id -eq $ClientId } | Select-Object -First 1).key
      } catch {}
    }
  }
  if (!$clientKey) {
    $clientKey = Read-DotEnvValue "MXRE_API_KEY"
  }
  if (!$clientKey) {
    throw "No API key found. Set MXRE_BUY_BOX_CLUB_KEY or MXRE_CLIENT_API_KEYS in the environment before running."
  }

  $upstreamKey = Read-DotEnvValue "MXRE_API_KEY"
  if (!$upstreamKey) {
    throw "No internal MXRE_API_KEY found. The Worker must use an origin-only upstream key, not a BBC-facing client key."
  }

  Write-Host "Starting MXRE gateway supervisor on local port $Port"
  Stop-Existing

  Write-Host "Building API..."
  npm run build

  $apiOut = "logs\api-local-origin.out.log"
  $apiErr = "logs\api-local-origin.err.log"
  Remove-Item $apiOut, $apiErr -ErrorAction SilentlyContinue

  $env:PORT = $Port
  $env:MXRE_CLIENT_API_KEYS = "[{`"id`":`"legacy`",`"key`":`"$upstreamKey`",`"environment`":`"origin`",`"monthlyQuota`":10000000},{`"id`":`"$ClientId`",`"key`":`"$clientKey`",`"environment`":`"production`",`"monthlyQuota`":10000000}]"
  Write-Host "Starting local Node API..."
  Start-Process -FilePath "node" -ArgumentList "dist/api/server.js" -WorkingDirectory (Get-Location) -WindowStyle Hidden -RedirectStandardOutput $apiOut -RedirectStandardError $apiErr
  Wait-For-Health

  $cloudflared = Get-CloudflaredPath
  $tunnelOut = "logs\cloudflared-mxre.out.log"
  $tunnelErr = "logs\cloudflared-mxre.err.log"
  $emptyConfig = "logs\mxre-cloudflared-temp.yml"
  Set-Content $emptyConfig ""
  Remove-Item $tunnelOut, $tunnelErr -ErrorAction SilentlyContinue
  Write-Host "Starting Cloudflare quick tunnel..."
  Start-Process -FilePath $cloudflared -ArgumentList "--config", $emptyConfig, "tunnel", "--url", "http://127.0.0.1:$Port", "--no-autoupdate" -WorkingDirectory (Get-Location) -WindowStyle Hidden -RedirectStandardOutput $tunnelOut -RedirectStandardError $tunnelErr

  $originUrl = Wait-For-TunnelUrl $tunnelOut
  Write-Host "Cloudflare tunnel origin: $originUrl"

  $originFile = "logs\mxre-origin.secret.txt"
  $keyFile = "logs\mxre-upstream.secret.txt"
  [System.IO.File]::WriteAllText((Join-Path (Get-Location) $originFile), $originUrl)
  [System.IO.File]::WriteAllText((Join-Path (Get-Location) $keyFile), $upstreamKey)
  Write-Host "Updating Worker origin secrets..."
  Get-Content $originFile -Raw | npx wrangler secret put MXRE_ORIGIN_URL
  Get-Content $keyFile -Raw | npx wrangler secret put MXRE_UPSTREAM_API_KEY
  Remove-Item $originFile, $keyFile -Force -ErrorAction SilentlyContinue

  Write-Host "Deploying Worker..."
  npx wrangler deploy

  $smokeUrl = "$WorkerUrl$SmokePath"
  Write-Host "Running gateway smoke test..."
  $smoke = Invoke-WebRequest -UseBasicParsing -Headers @{
    "x-api-key" = $clientKey
    "x-client-id" = $ClientId
    "x-request-id" = "mxre-local-gateway-startup"
  } -Uri $smokeUrl -TimeoutSec 60

  Write-Host "Gateway smoke test: $($smoke.StatusCode) $smokeUrl"
  Write-Host "MXRE API gateway is running."
} finally {
  Pop-Location
}
