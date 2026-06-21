let cachedAllowed: boolean | null = null;

export async function preflightOutboundNetwork(opts?: { timeoutMs?: number; url?: string; urls?: string[] }): Promise<boolean> {
  if (cachedAllowed != null) return cachedAllowed;
  const timeoutMs = opts?.timeoutMs ?? 2_000;
  const urls = [
    ...(opts?.urls ?? (opts?.url ? [opts.url] : [])),
    "https://example.com",
    "https://www.arcgis.com/sharing/rest/info?f=json",
    "https://overpass-api.de/api/status",
  ];

  cachedAllowed = false;
  for (const url of urls) {
    try {
      // Any HTTP response indicates outbound reachability, even if it is 401/403/404.
      // Some environments block specific domains (e.g. example.com) but allow others.
      await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
      cachedAllowed = true;
      break;
    } catch {
      // keep trying next URL
    }
  }
  return cachedAllowed;
}
