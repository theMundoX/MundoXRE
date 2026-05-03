param(
  [Parameter(Mandatory = $true)]
  [string]$HostName,
  [string]$User = "root",
  [string]$KeyPath = "$env:USERPROFILE\.ssh\mxre_contabo_ed25519",
  [string]$SupabaseDir = "/opt/supabase/docker"
)

$ErrorActionPreference = "Stop"

function Run-Remote {
  param([string]$Command)
  ssh -i $KeyPath -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$User@$HostName" $Command
}

if (!(Test-Path $KeyPath)) {
  throw "SSH key missing: $KeyPath"
}

$remoteHardening = @'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

apt-get update >/dev/null
apt-get install -y fail2ban >/dev/null

mkdir -p /etc/ssh/sshd_config.d
cat >/etc/ssh/sshd_config.d/99-mxre-hardening.conf <<'EOF'
PermitRootLogin prohibit-password
PasswordAuthentication no
PubkeyAuthentication yes
KbdInteractiveAuthentication no
X11Forwarding no
MaxAuthTries 3
ClientAliveInterval 300
ClientAliveCountMax 2
EOF

if [ -f /etc/ssh/sshd_config.d/50-cloud-init.conf ]; then
  cp -a /etc/ssh/sshd_config.d/50-cloud-init.conf "/etc/ssh/sshd_config.d/50-cloud-init.conf.bak.$(date +%Y%m%d%H%M%S)"
  printf 'PasswordAuthentication no\n' >/etc/ssh/sshd_config.d/50-cloud-init.conf
fi

sshd -t
cat >/etc/fail2ban/jail.d/mxre-sshd.conf <<'EOF'
[sshd]
enabled = true
backend = systemd
port = ssh
maxretry = 4
findtime = 10m
bantime = 1h
EOF

systemctl enable fail2ban >/dev/null
systemctl restart ssh
systemctl restart fail2ban

cat >/usr/local/sbin/mxre-docker-firewall.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
iptables -N DOCKER-USER 2>/dev/null || true
iptables -F DOCKER-USER
iptables -A DOCKER-USER -s 172.87.18.13/32 -p tcp -m multiport --dports 8000,8443,5432,6543 -j RETURN
iptables -A DOCKER-USER -s 100.64.0.0/10 -p tcp -m multiport --dports 8000,8443,5432,6543 -j RETURN
iptables -A DOCKER-USER -p tcp -m multiport --dports 8000,8443,5432,6543 -j DROP
iptables -A DOCKER-USER -j RETURN
EOF
chmod +x /usr/local/sbin/mxre-docker-firewall.sh

cat >/etc/systemd/system/mxre-docker-firewall.service <<'EOF'
[Unit]
Description=MXRE Docker published-port firewall guard
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/mxre-docker-firewall.sh
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable mxre-docker-firewall.service >/dev/null
systemctl restart mxre-docker-firewall.service

if [ -d "__SUPABASE_DIR__" ]; then
  cd "__SUPABASE_DIR__"
  cp -a .env ".env.bak.$(date +%Y%m%d%H%M%S)"
  cp -a docker-compose.yml "docker-compose.yml.bak.$(date +%Y%m%d%H%M%S)"
  python3 - <<'PY'
from pathlib import Path

env_path = Path('.env')
env = env_path.read_text()
env = env.replace('POSTGRES_PORT=127.0.0.1:5432', 'POSTGRES_PORT=5432')
env = env.replace('POOLER_PROXY_PORT_TRANSACTION=127.0.0.1:6543', 'POOLER_PROXY_PORT_TRANSACTION=6543')
env = env.replace('KONG_HTTP_PORT=127.0.0.1:8000', 'KONG_HTTP_PORT=8000')
env = env.replace('KONG_HTTPS_PORT=127.0.0.1:8443', 'KONG_HTTPS_PORT=8443')
env = env.replace('API_EXTERNAL_URL=http://207.244.225.239:8000', 'API_EXTERNAL_URL=http://127.0.0.1:8000')
env = env.replace('SUPABASE_PUBLIC_URL=http://localhost:8000', 'SUPABASE_PUBLIC_URL=http://127.0.0.1:8000')
env_path.write_text(env)

compose_path = Path('docker-compose.yml')
compose = compose_path.read_text()
compose = compose.replace('- ${KONG_HTTP_PORT}:8000/tcp', '- 127.0.0.1:${KONG_HTTP_PORT}:8000/tcp')
compose = compose.replace('- ${KONG_HTTPS_PORT}:8443/tcp', '- 127.0.0.1:${KONG_HTTPS_PORT}:8443/tcp')
compose = compose.replace('- ${POSTGRES_PORT}:5432', '- 127.0.0.1:${POSTGRES_PORT}:5432')
compose = compose.replace('- ${POOLER_PROXY_PORT_TRANSACTION}:6543', '- 127.0.0.1:${POOLER_PROXY_PORT_TRANSACTION}:6543')
compose_path.write_text(compose)
PY
  chmod 600 .env
  if docker compose version >/dev/null 2>&1; then
    docker compose up -d db analytics supavisor studio kong
  else
    docker-compose up -d db analytics supavisor studio kong
  fi
fi

if [ -f /opt/mxre/.env ]; then
  cp -a /opt/mxre/.env "/opt/mxre/.env.bak.$(date +%Y%m%d%H%M%S)"
  python3 - <<'PY'
from pathlib import Path
p = Path('/opt/mxre/.env')
s = p.read_text()
s = s.replace('http://207.244.225.239:8000', 'http://127.0.0.1:8000')
s = s.replace('https://207.244.225.239:8443', 'https://127.0.0.1:8443')
s = s.replace('DB_HOST=207.244.225.239', 'DB_HOST=127.0.0.1')
p.write_text(s)
PY
  chmod 600 /opt/mxre/.env
  systemctl restart mxre-api || true
fi

echo '--- SSH effective settings ---'
sshd -T | egrep '^(permitrootlogin|passwordauthentication|pubkeyauthentication|kbdinteractiveauthentication|maxauthtries) '
echo '--- Docker published ports ---'
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | sed -n '1,14p'
echo '--- MXRE health ---'
curl -fsS http://127.0.0.1:3101/health
'@

$remoteHardening = $remoteHardening.Replace("__SUPABASE_DIR__", $SupabaseDir)
$tmp = New-TemporaryFile
Set-Content -Path $tmp -Value $remoteHardening -Encoding utf8NoBOM
try {
  scp -i $KeyPath -o StrictHostKeyChecking=accept-new $tmp "${User}@${HostName}:/tmp/mxre-hardening.sh"
} finally {
  Remove-Item $tmp -Force -ErrorAction SilentlyContinue
}

Run-Remote "bash /tmp/mxre-hardening.sh"
