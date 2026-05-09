param(
  [string]$TaskName = "MXRE API Watchdog",
  [int]$IntervalMinutes = 5,
  [switch]$RepairWorkerOrigin
)

$ErrorActionPreference = "Stop"

$repo = Split-Path $PSScriptRoot -Parent
$script = Join-Path $repo "scripts\watch-mxre-api-origin.ps1"
if (!(Test-Path $script)) {
  throw "Missing watchdog script: $script"
}

$repairArg = if ($RepairWorkerOrigin) { " -RepairWorkerOrigin" } else { "" }
$argument = "-NoProfile -ExecutionPolicy Bypass -File `"$script`"$repairArg"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $argument -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -MultipleInstances IgnoreNew `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 4) `
  -StartWhenAvailable

try {
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Checks MXRE public API, named Cloudflare origin, and local Node API; restarts/repairs them if unhealthy." `
    -Force | Out-Null
  Write-Host "Installed scheduled task: $TaskName every $IntervalMinutes minutes"
} catch {
  Write-Host "Register-ScheduledTask failed, falling back to schtasks.exe: $($_.Exception.Message)"
  $taskCommand = "powershell.exe $argument"
  schtasks.exe /Create /TN $TaskName /TR $taskCommand /SC MINUTE /MO $IntervalMinutes /F | Out-Null
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to install watchdog task with Register-ScheduledTask and schtasks.exe."
  }
  Write-Host "Installed scheduled task with schtasks.exe: $TaskName every $IntervalMinutes minutes"
}

Write-Host "Run now with:"
Write-Host "Start-ScheduledTask -TaskName `"$TaskName`""
