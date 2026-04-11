/**
 * Proxy rotation with health tracking.
 * Supports separate pools for county sites vs property sites.
 * Never logs proxy credentials.
 */

interface ProxyEntry {
  url: string;
  failures: number;
  lastUsed: number;
  dead: boolean;
}

const MAX_FAILURES = 5;
const RECOVERY_MS = 30 * 60 * 1000; // 30 minutes — retry dead proxies

let residentialProxies: ProxyEntry[] = [];
let datacenterProxies: ProxyEntry[] = [];
let currentResidentialIndex = 0;
let currentDatacenterIndex = 0;

function parseProxyList(envVar: string | undefined): ProxyEntry[] {
  if (!envVar) return [];
  return envVar
    .split(",")
    .map((url) => url.trim())
    .filter(Boolean)
    .map((url) => ({ url, failures: 0, lastUsed: 0, dead: false }));
}

/**
 * Redact credentials from a proxy URL for safe logging.
 */
export function redactProxyUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.username) parsed.username = "***";
    if (parsed.password) parsed.password = "***";
    return parsed.toString();
  } catch {
    return "[invalid proxy]";
  }
}

export function initProxies() {
  residentialProxies = parseProxyList(process.env.PROXY_URL);
  datacenterProxies = parseProxyList(process.env.PROXY_DATACENTER_URL);
}

function nextAlive(pool: ProxyEntry[], startIndex: number): { proxy: ProxyEntry; index: number } | null {
  const len = pool.length;
  for (let i = 0; i < len; i++) {
    const idx = (startIndex + i) % len;
    const entry = pool[idx];

    // Recover dead proxies after timeout
    if (entry.dead && Date.now() - entry.lastUsed > RECOVERY_MS) {
      entry.dead = false;
      entry.failures = 0;
    }

    if (!entry.dead) {
      entry.lastUsed = Date.now();
      return { proxy: entry, index: (idx + 1) % len };
    }
  }
  return null;
}

export function getResidentialProxy(): string | null {
  if (residentialProxies.length === 0) return null;
  const result = nextAlive(residentialProxies, currentResidentialIndex);
  if (!result) return null;
  currentResidentialIndex = result.index;
  return result.proxy.url;
}

export function getDatacenterProxy(): string | null {
  if (datacenterProxies.length === 0) return getResidentialProxy();
  const result = nextAlive(datacenterProxies, currentDatacenterIndex);
  if (!result) return getResidentialProxy();
  currentDatacenterIndex = result.index;
  return result.proxy.url;
}

export function reportProxyFailure(proxyUrl: string) {
  const allProxies = [...residentialProxies, ...datacenterProxies];
  const entry = allProxies.find((p) => p.url === proxyUrl);
  if (entry) {
    entry.failures++;
    if (entry.failures >= MAX_FAILURES) {
      entry.dead = true;
    }
  }
}

export function reportProxySuccess(proxyUrl: string) {
  const allProxies = [...residentialProxies, ...datacenterProxies];
  const entry = allProxies.find((p) => p.url === proxyUrl);
  if (entry) {
    entry.failures = 0;
  }
}

export function getProxyStats() {
  return {
    residential: {
      total: residentialProxies.length,
      alive: residentialProxies.filter((p) => !p.dead).length,
    },
    datacenter: {
      total: datacenterProxies.length,
      alive: datacenterProxies.filter((p) => !p.dead).length,
    },
  };
}
