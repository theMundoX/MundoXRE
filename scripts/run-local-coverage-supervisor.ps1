param(
  [string]$Market = "indianapolis",
  [int]$IntervalMinutes = 360,
  [switch]$Once,
  [switch]$DryRun,
  [switch]$PlanOnly,
  [string]$Repo = "C:\Users\msanc\mxre"
)

$ErrorActionPreference = "Continue"

$RunRoot = Join-Path $Repo "logs\local-supervisor"
$LockDir = Join-Path $Repo ".mxre-locks"
New-Item -ItemType Directory -Force -Path $RunRoot | Out-Null
New-Item -ItemType Directory -Force -Path $LockDir | Out-Null

function Write-Log {
  param([string]$Message, [string]$Path)
  $line = "[$(Get-Date -Format o)] $Message"
  $line | Tee-Object -FilePath $Path -Append
}

function Get-MarketCommand {
  param([string]$MarketName)

  switch ($MarketName.ToLowerInvariant()) {
    "indianapolis" {
      $args = @("tsx", "scripts/refresh-indianapolis-market.ts")
      if ($DryRun) { $args += "--dry-run" }
      return @{ File = "npx.cmd"; Args = $args }
    }
    "west-chester" {
      $args = @("tsx", "scripts/refresh-west-chester-market.ts")
      if ($DryRun) { $args += "--dry-run" }
      return @{ File = "npx.cmd"; Args = $args }
    }
    "columbus" {
      return @{ File = "powershell.exe"; Args = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "scripts\run-columbus-overnight.ps1") }
    }
    default {
      throw "Unsupported market '$MarketName'. Supported: indianapolis, west-chester, columbus."
    }
  }
}

function Invoke-MarketRun {
  param([string]$MarketName)

  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $safeMarket = $MarketName.ToLowerInvariant().Replace(" ", "-")
  $logPath = Join-Path $RunRoot "$safeMarket-$stamp.log"
  $lockPath = Join-Path $LockDir "$safeMarket.lock"
  $command = Get-MarketCommand $MarketName

  if ($PlanOnly) {
    Write-Log "PLAN market=$MarketName dryRun=$DryRun command=$($command.File) $($command.Args -join ' ')" $logPath
    Write-Host "Plan only: $($command.File) $($command.Args -join ' ')"
    Write-Host "Log: $logPath"
    return 0
  }

  if (Test-Path $lockPath) {
    $lockText = Get-Content $lockPath -Raw -ErrorAction SilentlyContinue
    Write-Log "SKIP $MarketName because lock exists: $lockText" $logPath
    return 0
  }

  Set-Content -Path $lockPath -Value "pid=$PID started=$(Get-Date -Format o) market=$MarketName"
  try {
    Write-Log "START market=$MarketName dryRun=$DryRun command=$($command.File) $($command.Args -join ' ')" $logPath

    $stdout = Join-Path $RunRoot "$safeMarket-$stamp.out.log"
    $stderr = Join-Path $RunRoot "$safeMarket-$stamp.err.log"
    $process = Start-Process -FilePath $command.File -ArgumentList $command.Args -WorkingDirectory $Repo -NoNewWindow -Wait -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr

    Get-Content $stdout -ErrorAction SilentlyContinue | Add-Content $logPath
    Get-Content $stderr -ErrorAction SilentlyContinue | Add-Content $logPath
    Write-Log "END market=$MarketName exit=$($process.ExitCode)" $logPath
    return $process.ExitCode
  } finally {
    Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
  }
}

Set-Location $Repo
Write-Host "MXRE local coverage supervisor"
Write-Host "Repo: $Repo"
Write-Host "Market: $Market"
Write-Host "Interval minutes: $IntervalMinutes"
Write-Host "Once: $Once"
Write-Host "Plan only: $PlanOnly"
Write-Host "Logs: $RunRoot"

do {
  Invoke-MarketRun $Market | Out-Null
  if ($Once) { break }
  Start-Sleep -Seconds ([Math]::Max(60, $IntervalMinutes * 60))
} while ($true)
