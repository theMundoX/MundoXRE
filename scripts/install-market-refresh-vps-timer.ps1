param(
  [Parameter(Mandatory = $true)]
  [string]$HostName,
  [string]$User = "root",
  [string]$KeyPath = "$env:USERPROFILE\.ssh\mxre_contabo_ed25519",
  [string]$RepoDir = "/opt/mxre",
  [string]$OnCalendar = "08:15:00 UTC"
)

$ErrorActionPreference = "Stop"

function Run-Remote {
  param([string]$Command)
  ssh -i $KeyPath -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$User@$HostName" $Command
}

if (!(Test-Path $KeyPath)) {
  throw "SSH key missing: $KeyPath"
}

$OnCalendarSpec = if ($OnCalendar -match '^\d{2}:\d{2}$') { "${OnCalendar}:00 UTC" } else { $OnCalendar }

$remoteScript = @"
set -euo pipefail

cd "$RepoDir"
git config --global --add safe.directory "$RepoDir" || true
git fetch origin main
git pull --ff-only origin main
npm ci
npm run build

mkdir -p /var/log/mxre
chown mxre:mxre /var/log/mxre

cat >/usr/local/bin/mxre-run-daily-market-refresh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd /opt/mxre
mkdir -p logs/market-refresh
exec /usr/bin/flock -n /tmp/mxre-daily-market-refresh.lock \
  /usr/bin/env bash -lc 'npm exec -- tsx scripts/run-market-refresh-jobs.ts >> logs/market-refresh/systemd-daily.log 2>&1'
EOF
chmod +x /usr/local/bin/mxre-run-daily-market-refresh

cat >/etc/systemd/system/mxre-market-refresh.service <<'EOF'
[Unit]
Description=MXRE daily configured market refresh
After=network-online.target mxre-api.service
Wants=network-online.target

[Service]
Type=oneshot
User=mxre
Group=mxre
WorkingDirectory=/opt/mxre
Environment=NODE_ENV=production
ExecStart=/usr/local/bin/mxre-run-daily-market-refresh
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/opt/mxre /tmp
ProtectHome=true
PrivateDevices=true
TimeoutStartSec=12h
EOF

cat >/etc/systemd/system/mxre-market-refresh.timer <<EOF
[Unit]
Description=Run MXRE market refresh once daily

[Timer]
OnCalendar=*-*-* $OnCalendarSpec
Persistent=true
RandomizedDelaySec=20m
Unit=mxre-market-refresh.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now mxre-market-refresh.timer
systemctl list-timers mxre-market-refresh.timer --no-pager
"@

$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($remoteScript))
Run-Remote "printf '%s' '$encoded' | base64 -d >/tmp/install-mxre-market-refresh.sh && bash /tmp/install-mxre-market-refresh.sh"
