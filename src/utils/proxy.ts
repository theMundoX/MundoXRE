/**
 * Proxy rotation with health tracking.
 * Loads proxy URLs from environment variables.
 * Supports separate pools for county sites vs property sites.
 */

interface ProxyEntry {
  url: string;
  failures: number;
  lastUsed: number;
  dead: boolean;
}

const MAX_FAILURES = 5;

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

export function initProxies() {
  residentialProxies = parseProxyList(process.env.PROXY_URL);
  datacenterProxies = parseProxyList(process.env.PROXY_DATACENTER_URL);
}

function nextAlive(pool: ProxyEntry[], startIndex: number): { proxy: ProxyEntry; index: number } | null {
  const len = pool.length;
  for (let i = 0; i < len; i++) {
    const idx = (startIndex + i) % len;
    if (!pool[idx].dead) {
      pool[idx].lastUsed = Date.now();
      return { proxy: pool[idx], index: (idx + 1) % len };
    }
  }
  return null;
}

/**
 * Get the next residential proxy URL (for property websites).
 * Returns null if no proxies configured or all are dead.
 */
export function getResidentialProxy(): string | null {
  if (residentialProxies.length === 0) return null;
  const result = nextAlive(residentialProxies, currentResidentialIndex);
  if (!result) return null;
  currentResidentialIndex = result.index;
  return result.proxy.url;
}

/**
 * Get the next datacenter proxy URL (for county/government sites).
 * Falls back to residential if no datacenter proxies configured.
 */
export function getDatacenterProxy(): string | null {
  if (datacenterProxies.length === 0) return getResidentialProxy();
  const result = nextAlive(datacenterProxies, currentDatacenterIndex);
  if (!result) return getResidentialProxy();
  currentDatacenterIndex = result.index;
  return result.proxy.url;
}

/**
 * Report a proxy failure. After MAX_FAILURES consecutive failures, mark as dead.
 */
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

/**
 * Report a proxy success. Resets failure counter.
 */
export function reportProxySuccess(proxyUrl: string) {
  const allProxies = [...residentialProxies, ...datacenterProxies];
  const entry = allProxies.find((p) => p.url === proxyUrl);
  if (entry) {
    entry.failures = 0;
  }
}

/**
 * Get proxy health stats.
 */
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
