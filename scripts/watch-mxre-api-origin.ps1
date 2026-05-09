param(
  [string]$Repo = (Split-Path $PSScriptRoot -Parent),
  [string]$Port = "3101",
  [string]$OriginHostname = "mxre-origin.mundox.ai",
  [string]$PublicApiHost = "api.mxre.mundox.ai",
  [switch]$RepairWorkerOrigin
)

$ErrorActionPreference = "Stop"

function Write-Log {
  param([string]$Message)
  $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  Add-Content -Path (Join-Path $Repo "logs\api-watchdog.log") -Value "[$stamp] $Message"
}

function Read-DotEnvValue {
  param([string]$Name)
  $line = Get-Content (Join-Path $Repo ".env") -ErrorAction SilentlyContinue |
    Where-Object { $_ -match "^$Name=" } |
    Select-Object -First 1
  if (!$line) { return $null }
  return ($line -replace "^$Name=", "").Trim('"').Trim("'")
}

function Test-HttpOk {
  param(
    [string]$Url,
    [hashtable]$Headers = @{},
    [string]$Method = "GET",
    [string]$Body = $null,
    [int]$TimeoutSec = 8
  )
  try {
    $args = @{
      Uri = $Url
      Method = $Method
      Headers = $Headers
      UseBasicParsing = $true
      TimeoutSec = $TimeoutSec
    }
    if ($PSBoundParameters.ContainsKey("Body")) {
      $args.Body = $Body
      $args.ContentType = "application/json"
    }
    $response = Invoke-WebRequest @args
    return @{ Ok = ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300); Status = $response.StatusCode; Body = $response.Content }
  } catch {
    $body = if ($_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { "" }
    $status = try { $_.Exception.Response.StatusCode.value__ } catch { $null }
    return @{ Ok = $false; Status = $status; Body = $body; Error = $_.Exception.Message }
  }
}

Push-Location $Repo
try {
  New-Item -ItemType Directory -Force "logs" | Out-Null
  $lockPath = Join-Path $Repo "logs\api-watchdog.lock"
  if (Test-Path $lockPath) {
    $lockAge = (Get-Date) - (Get-Item $lockPath).LastWriteTime
    if ($lockAge.TotalMinutes -lt 10) {
      Write-Log "Prior watchdog still active; skipping this run."
      exit 0
    }
    Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
  }
  Set-Content -Path $lockPath -Value (Get-Date).ToString("o") -Encoding ASCII

  $local = Test-HttpOk "http://127.0.0.1:$Port/health"
  $origin = Test-HttpOk "https://$OriginHostname/health"

  if (!$local.Ok -or !$origin.Ok) {
    Write-Log "Origin unhealthy. local=$($local.Status) localError=$($local.Error) origin=$($origin.Status) originError=$($origin.Error). Launching named origin restart."
    $restartLog = Join-Path $Repo "logs\api-watchdog-restart.log"
    $startScript = Join-Path $Repo "scripts\start-mxre-named-tunnel-origin.ps1"
    $restartCommand = "& `"$startScript`" -Port `"$Port`" -OriginHostname `"$OriginHostname`" -NoBuild *>> `"$restartLog`""
    Start-Process -FilePath "powershell.exe" `
      -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $restartCommand) `
      -WorkingDirectory $Repo `
      -WindowStyle Hidden | Out-Null
    exit 1
  }

  $sandboxKey = Read-DotEnvValue "MXRE_BUY_BOX_CLUB_SANDBOX_KEY"
  if ($sandboxKey) {
    $markets = Test-HttpOk "https://$PublicApiHost/v1/bbc/markets" @{
      "x-client-id" = "buy_box_club_sandbox"
      "x-api-key" = $sandboxKey
    } -TimeoutSec 12

    $originDnsError = ($markets.Status -eq 530) -or ($markets.Body -match "Origin DNS error|error code:\s*1016")
    if ($originDnsError -and $origin.Ok -and $RepairWorkerOrigin) {
      Write-Log "Public API returned 530/1016 while named origin is healthy. Repairing Worker MXRE_ORIGIN_URL."
      Write-Output "https://$OriginHostname" | npx.cmd wrangler secret put MXRE_ORIGIN_URL *>> (Join-Path $Repo "logs\api-watchdog-worker-repair.log")
      npx.cmd wrangler deploy *>> (Join-Path $Repo "logs\api-watchdog-worker-repair.log")
      $markets = Test-HttpOk "https://$PublicApiHost/v1/bbc/markets" @{
        "x-client-id" = "buy_box_club_sandbox"
        "x-api-key" = $sandboxKey
      } -TimeoutSec 12
      Write-Log "Worker repair result. publicMarkets=$($markets.Status)"
    }

    if (!$markets.Ok) {
      Write-Log "Public BBC markets unhealthy. status=$($markets.Status) error=$($markets.Error)"
      exit 1
    }
  } else {
    Write-Log "Sandbox key not available; skipped authenticated public markets check."
  }

  Write-Log "Healthy. local=$($local.Status) origin=$($origin.Status)"
} finally {
  Remove-Item (Join-Path $Repo "logs\api-watchdog.lock") -Force -ErrorAction SilentlyContinue
  Pop-Location
}
