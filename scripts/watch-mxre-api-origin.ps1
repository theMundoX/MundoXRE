param(
  [string]$Repo = (Split-Path $PSScriptRoot -Parent),
  [string]$Port = "3101",
  [string]$OriginHostname = "mxre-origin.mundox.ai",
  [string]$PublicApiHost = "api.mxre.mundox.ai",
  [string]$VpsHostName = "207.244.225.239",
  [string]$ContractProbeAddress = "9105 Kinlock Dr",
  [string]$ContractProbeCity = "Indianapolis",
  [string]$ContractProbeState = "IN",
  [switch]$AllowLocalOrigin,
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

function Stop-StaleLocalOriginTunnels {
  if ($AllowLocalOrigin) { return }

  $pattern = "cloudflared.*(mundox-gateway|$OriginHostname|127\.0\.0\.1:$Port|localhost:$Port)"
  $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object {
      $_.Name -match "^cloudflared(\.exe)?$" -and
      $_.CommandLine -match $pattern
    }

  foreach ($process in $processes) {
    try {
      Write-Log "Stopping stale local MXRE origin tunnel pid=$($process.ProcessId). Production origin should be VPS-backed."
      Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
    } catch {
      Write-Log "Failed to stop stale local tunnel pid=$($process.ProcessId): $($_.Exception.Message)"
    }
  }
}

function Invoke-VpsDeployRepair {
  param([string]$Reason)
  $deployScript = Join-Path $Repo "scripts\deploy-mxre-contabo-vps.ps1"
  if (!(Test-Path $deployScript)) {
    Write-Log "Cannot run VPS repair; missing deploy script. reason=$Reason"
    return
  }

  $repairLog = Join-Path $Repo "logs\api-watchdog-vps-repair.log"
  Write-Log "Launching VPS API repair deploy. reason=$Reason"
  $repairCommand = "& `"$deployScript`" -HostName `"$VpsHostName`" *>> `"$repairLog`""
  Start-Process -FilePath "powershell.exe" `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $repairCommand) `
    -WorkingDirectory $Repo `
    -WindowStyle Hidden | Out-Null
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

  Stop-StaleLocalOriginTunnels

  $local = Test-HttpOk "http://127.0.0.1:$Port/health"
  $origin = Test-HttpOk "https://$OriginHostname/health"

  if (!$origin.Ok) {
    Write-Log "Named origin unhealthy. origin=$($origin.Status) originError=$($origin.Error). Launching VPS repair."
    Invoke-VpsDeployRepair "named_origin_unhealthy"
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
      Invoke-VpsDeployRepair "public_markets_unhealthy_status_$($markets.Status)"
      exit 1
    }

    $probeQuery = "address=$([Uri]::EscapeDataString($ContractProbeAddress))&city=$([Uri]::EscapeDataString($ContractProbeCity))&state=$([Uri]::EscapeDataString($ContractProbeState))"
    $property = Test-HttpOk "https://$PublicApiHost/v1/bbc/property?$probeQuery" @{
      "x-client-id" = "buy_box_club_sandbox"
      "x-api-key" = $sandboxKey
    } -TimeoutSec 45
    if (!$property.Ok -or $property.Body -notmatch '"schemaVersion"\s*:\s*"mxre\.bbc\.property\.v1"') {
      $propertyBody = if ($null -ne $property.Body) { [string]$property.Body } else { "" }
      Write-Log "Public BBC exact-property contract unhealthy. status=$($property.Status) error=$($property.Error) body=$($propertyBody.Substring(0, [Math]::Min(300, $propertyBody.Length)))"
      Invoke-VpsDeployRepair "public_exact_property_contract_unhealthy_status_$($property.Status)"
      exit 1
    }

    $searchBody = '{"market":"indianapolis","filters":{"minPrice":1},"limit":1}'
    $searchRun = Test-HttpOk "https://$PublicApiHost/v1/bbc/search-runs" @{
      "x-client-id" = "buy_box_club_sandbox"
      "x-api-key" = $sandboxKey
    } -Method "POST" -Body $searchBody -TimeoutSec 45
    if (!$searchRun.Ok -or $searchRun.Body -notmatch '"schemaVersion"\s*:\s*"mxre\.bbc\.searchRun\.v1"') {
      $searchBodyText = if ($null -ne $searchRun.Body) { [string]$searchRun.Body } else { "" }
      Write-Log "Public BBC search-runs contract unhealthy. status=$($searchRun.Status) error=$($searchRun.Error) body=$($searchBodyText.Substring(0, [Math]::Min(300, $searchBodyText.Length)))"
      Invoke-VpsDeployRepair "public_search_runs_contract_unhealthy_status_$($searchRun.Status)"
      exit 1
    }
  } else {
    Write-Log "Sandbox key not available; skipped authenticated public markets check."
  }

  Write-Log "Healthy. local=$($local.Status) origin=$($origin.Status) publicContracts=ok"
} finally {
  Remove-Item (Join-Path $Repo "logs\api-watchdog.lock") -Force -ErrorAction SilentlyContinue
  Pop-Location
}
