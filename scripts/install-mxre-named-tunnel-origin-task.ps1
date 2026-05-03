param(
  [string]$TaskName = "MXRE Named Tunnel Origin",
  [string]$Port = "3101",
  [string]$TunnelName = "mundox-gateway",
  [string]$OriginHostname = "mxre-origin.mundox.ai"
)

$ErrorActionPreference = "Stop"

$repo = Split-Path $PSScriptRoot -Parent
$script = Join-Path $repo "scripts\start-mxre-named-tunnel-origin.ps1"
if (!(Test-Path $script)) {
  throw "Missing startup script: $script"
}

$argument = "-NoProfile -ExecutionPolicy Bypass -File `"$script`" -Port `"$Port`" -TunnelName `"$TunnelName`" -OriginHostname `"$OriginHostname`" -NoBuild"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $argument -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Days 30) `
  -RestartCount 3 `
  -RestartInterval (New-TimeSpan -Minutes 5)

try {
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Starts the MXRE local Node API origin and named Cloudflare Tunnel for api.mxre.mundox.ai." `
    -Force | Out-Null
  Write-Host "Installed scheduled task: $TaskName"
  Write-Host "Run now with:"
  Write-Host "Start-ScheduledTask -TaskName `"$TaskName`""
} catch {
  Write-Host "Register-ScheduledTask failed, falling back to schtasks.exe: $($_.Exception.Message)"
  $taskCommand = "powershell.exe $argument"
  schtasks.exe /Create /TN $TaskName /TR $taskCommand /SC ONLOGON /F | Out-Null
  if ($LASTEXITCODE -ne 0) {
    Write-Host "schtasks.exe failed; installing a user Startup launcher instead."
    $startup = [Environment]::GetFolderPath("Startup")
    $launcher = Join-Path $startup "Start MXRE Named Tunnel Origin.cmd"
    "@echo off`r`ncd /d `"$repo`"`r`npowershell.exe $argument >> `"$repo\logs\mxre-startup-launcher.log`" 2>&1`r`n" | Set-Content -Path $launcher -Encoding ASCII
    Write-Host "Installed Startup launcher: $launcher"
  } else {
    Write-Host "Installed scheduled task with schtasks.exe: $TaskName"
  }
}
