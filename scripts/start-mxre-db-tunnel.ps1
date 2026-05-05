param(
  [string]$HostName = "207.244.225.239",
  [string]$KeyPath = "$env:USERPROFILE\.ssh\mxre_contabo_ed25519",
  [int]$LocalPort = 8000,
  [int]$RemotePort = 8000
)

$ErrorActionPreference = "Stop"

$existing = Test-NetConnection 127.0.0.1 -Port $LocalPort -WarningAction SilentlyContinue
if ($existing.TcpTestSucceeded) {
  Write-Host "MXRE DB tunnel already available at http://127.0.0.1:$LocalPort"
  exit 0
}

if (-not (Test-Path $KeyPath)) {
  throw "SSH key not found: $KeyPath"
}

$args = @(
  "-i", $KeyPath,
  "-o", "StrictHostKeyChecking=no",
  "-o", "ExitOnForwardFailure=yes",
  "-N",
  "-L", "${LocalPort}:127.0.0.1:${RemotePort}",
  "root@$HostName"
)

Start-Process -FilePath "ssh" -ArgumentList $args -WindowStyle Hidden
Start-Sleep -Seconds 3

$started = Test-NetConnection 127.0.0.1 -Port $LocalPort -WarningAction SilentlyContinue
if (-not $started.TcpTestSucceeded) {
  throw "MXRE DB tunnel did not start on 127.0.0.1:$LocalPort"
}

Write-Host "MXRE DB tunnel ready at http://127.0.0.1:$LocalPort"
