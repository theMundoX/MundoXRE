#!/usr/bin/env bash
# MXRE VPS security check — run on 207.244.225.239 after connecting via SSH.
# Verifies what's exposed and locks down what shouldn't be.
set -euo pipefail

echo "=== Open ports on this host (external-facing) ==="
ss -tlnp 2>/dev/null | grep -E "0\.0\.0\.0|::" || echo "  (ss not available, trying netstat)"

echo ""
echo "=== UFW firewall status ==="
if command -v ufw &>/dev/null; then
  ufw status verbose || true
else
  echo "  ufw not installed. Install with: apt install ufw"
fi

echo ""
echo "=== Recommended minimum firewall rules ==="
cat <<'EOF'
# Run these on the VPS to lock down:

# Allow SSH from your IPs only (replace with your public IP)
ufw allow from YOUR.IP.HERE to any port 22

# Block public Supabase access — Kong should only be reachable from:
#   (a) your public IP(s) for admin
#   (b) Tailscale network for worker
ufw deny 8000/tcp                           # Kong (Supabase gateway)
ufw allow from 100.64.0.0/10 to any port 8000  # Tailscale /10
ufw allow from YOUR.IP.HERE to any port 8000

# Keep Postgres port private — should NOT be internet-reachable
ufw deny 5432/tcp

# Enable
ufw default deny incoming
ufw default allow outgoing
ufw enable
EOF

echo ""
echo "=== Check if Postgres is accessible from internet (should be NO) ==="
if command -v nc &>/dev/null; then
  timeout 2 bash -c 'echo > /dev/tcp/0.0.0.0/5432' 2>/dev/null \
    && echo "  ⚠️  Postgres listening on 0.0.0.0:5432 — should be 127.0.0.1 or Tailscale IP only" \
    || echo "  ✓ Postgres not on public 5432"
fi

echo ""
echo "=== Supabase service key rotation reminder ==="
echo "If any service key has been pasted in chat transcripts / screenshots / commits:"
echo "  1. cd ~/supabase (or wherever your self-hosted install lives)"
echo "  2. Edit docker-compose.yml → regenerate JWT_SECRET"
echo "  3. Regenerate anon + service_role keys from the new secret"
echo "  4. Update .env files and edge function secrets everywhere"
