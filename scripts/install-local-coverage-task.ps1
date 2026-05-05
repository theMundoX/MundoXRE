param(
  [string]$Market = "indianapolis",
  [string]$TaskName = "",
  [string]$Repo = "C:\Users\msanc\mxre",
  [string]$At = "02:00"
)

$ErrorActionPreference = "Stop"

$scriptPath = Join-Path $Repo "scripts\run-local-coverage-supervisor.ps1"
if (!(Test-Path $scriptPath)) {
  throw "Missing supervisor script: $scriptPath"
}

if ([string]::IsNullOrWhiteSpace($TaskName)) {
  $title = (Get-Culture).TextInfo.ToTitleCase($Market.Replace("-", " "))
  $TaskName = "MXRE Local Coverage - $title"
}

$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -Market `"$Market`" -Once -Repo `"$Repo`""

$trigger = New-ScheduledTaskTrigger -Daily -At $At
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -MultipleInstances IgnoreNew `
  -ExecutionTimeLimit (New-TimeSpan -Hours 10)

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger $trigger `
  -Settings $settings `
  -Description "Runs MXRE local coverage refresh for $Market on this computer and writes logs locally." `
  -Force | Out-Null

Write-Host "Installed scheduled task: $TaskName"
Write-Host "Market: $Market"
Write-Host "Schedule: daily at $At"
Write-Host "Run now:"
Write-Host "  Start-ScheduledTask -TaskName `"$TaskName`""
