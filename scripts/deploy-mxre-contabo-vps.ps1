param(
  [Parameter(Mandatory = $true)]
  [string]$HostName,
  [string]$User = "root",
  [string]$KeyPath = "$env:USERPROFILE\.ssh\mxre_contabo_ed25519",
  [string]$AppDir = "/opt/mxre",
  [string]$TunnelName = "mundox-gateway",
  [string]$TunnelId = "0aeab8b3-1635-4a21-98c1-b7ce97748628",
  [string]$OriginHostname = "mxre-origin.mundox.ai",
  [string]$LocalTunnelCredential = "$env:USERPROFILE\.cloudflared\0aeab8b3-1635-4a21-98c1-b7ce97748628.json"
)

$ErrorActionPreference = "Stop"

function Run-Remote {
  param([string]$Command)
  ssh -i $KeyPath -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$User@$HostName" $Command
}

if (!(Test-Path $KeyPath)) {
  throw "SSH key missing: $KeyPath"
}
if (!(Test-Path ".env")) {
  throw "Local .env missing. Refusing to deploy without production env vars."
}
if (!(Test-Path $LocalTunnelCredential)) {
  throw "Cloudflare tunnel credential missing: $LocalTunnelCredential"
}

Write-Host "Testing SSH to $User@$HostName..."
Run-Remote "echo MXRE_SSH_OK && uname -a"

Write-Host "Preparing remote directories..."
Run-Remote "mkdir -p /etc/cloudflared"

Write-Host "Copying environment and Cloudflare tunnel credentials..."
scp -i $KeyPath -o StrictHostKeyChecking=accept-new ".env" "${User}@${HostName}:/tmp/mxre.env"
scp -i $KeyPath -o StrictHostKeyChecking=accept-new $LocalTunnelCredential "${User}@${HostName}:/etc/cloudflared/$TunnelId.json"

$remoteBootstrap = @"
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

if command -v apt-get >/dev/null 2>&1; then
  apt-get update
  apt-get install -y ca-certificates curl git build-essential
  if ! command -v node >/dev/null 2>&1 || ! node -v | grep -q '^v22'; then
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
    apt-get install -y nodejs
  fi
  if ! command -v cloudflared >/dev/null 2>&1; then
    curl -fsSL -o /tmp/cloudflared.deb https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb
    dpkg -i /tmp/cloudflared.deb
  fi
else
  echo 'This deploy script currently expects a Debian/Ubuntu VPS with apt-get.' >&2
  exit 1
fi

if [ ! -d "$AppDir/.git" ]; then
  rm -rf "$AppDir"
  git clone https://github.com/theMundoX/MundoXRE.git "$AppDir"
fi

cd "$AppDir"
git fetch origin
git checkout main
git reset --hard origin/main
mv /tmp/mxre.env "$AppDir/.env"
id -u mxre >/dev/null 2>&1 || useradd --system --home-dir "$AppDir" --shell /usr/sbin/nologin mxre
chown -R mxre:mxre "$AppDir"
chmod 750 "$AppDir"
chmod 600 "$AppDir/.env"

npm ci
npm run build
chown -R mxre:mxre "$AppDir"

cat >/etc/systemd/system/mxre-api.service <<'EOF'
[Unit]
Description=MXRE Node API
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/mxre
EnvironmentFile=/opt/mxre/.env
Environment=NODE_ENV=production
Environment=PORT=3101
Environment=HOST=127.0.0.1
User=mxre
Group=mxre
ExecStart=/usr/bin/node /opt/mxre/dist/api/server.js
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/opt/mxre
ProtectHome=true
PrivateDevices=true

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/cloudflared/config.yml <<EOF
tunnel: $TunnelId
credentials-file: /etc/cloudflared/$TunnelId.json

ingress:
  - hostname: $OriginHostname
    service: http://localhost:3101
  - service: http_status:404
EOF
chmod 600 /etc/cloudflared/$TunnelId.json /etc/cloudflared/config.yml

cat >/etc/systemd/system/cloudflared-mxre.service <<EOF
[Unit]
Description=Cloudflare Tunnel for MXRE API origin
After=network-online.target mxre-api.service
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/cloudflared --config /etc/cloudflared/config.yml tunnel run $TunnelName
Restart=always
RestartSec=5
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable mxre-api cloudflared-mxre
systemctl restart mxre-api
systemctl restart cloudflared-mxre
sleep 5
systemctl --no-pager --full status mxre-api | sed -n '1,18p'
systemctl --no-pager --full status cloudflared-mxre | sed -n '1,18p'
curl -fsS http://127.0.0.1:3101/health
"@

$tmp = New-TemporaryFile
[System.IO.File]::WriteAllText($tmp.FullName, $remoteBootstrap, [System.Text.UTF8Encoding]::new($false))
try {
  scp -i $KeyPath -o StrictHostKeyChecking=accept-new $tmp "${User}@${HostName}:/tmp/mxre-bootstrap.sh"
} finally {
  Remove-Item $tmp -Force -ErrorAction SilentlyContinue
}

Write-Host "Running remote bootstrap..."
Run-Remote "bash /tmp/mxre-bootstrap.sh"

Write-Host "Smoke testing public API through Cloudflare..."
$docsKey = ((Get-Content ".env" | Where-Object { $_ -match '^MXRE_DOCS_API_KEY=' } | Select-Object -First 1) -split '=',2)[1].Trim().Trim('"')
$headers = @{ "x-client-id" = "buy_box_club_docs"; "x-api-key" = $docsKey }
Invoke-WebRequest -UseBasicParsing -Uri "https://api.mxre.mundox.ai/docs" -Headers $headers -TimeoutSec 30 | Select-Object StatusCode

Write-Host "MXRE VPS deploy complete. Public API remains https://api.mxre.mundox.ai"
