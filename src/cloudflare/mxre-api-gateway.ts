export interface Env {
  MXRE_ORIGIN_URL?: string;
  MXRE_UPSTREAM_API_KEY?: string;
  MXRE_CLIENT_API_KEYS?: string;
  MXRE_BUY_BOX_CLUB_KEY?: string;
  MXRE_BUY_BOX_CLUB_SANDBOX_KEY?: string;
  CACHE_SECONDS?: string;
}

type ApiClient = {
  id: string;
  key: string;
  environment?: string;
  monthlyQuota?: number;
};

type RateBucket = {
  resetAt: number;
  count: number;
};

const rateBuckets = new Map<string, RateBucket>();

function json(body: unknown, status = 200, headers: HeadersInit = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      ...headers,
    },
  });
}

function securityHeaders(): HeadersInit {
  return {
    'x-content-type-options': 'nosniff',
    'referrer-policy': 'no-referrer',
    'x-frame-options': 'DENY',
    'permissions-policy': 'geolocation=(), microphone=(), camera=()',
  };
}

function rateLimit(key: string, limit: number, windowMs: number): { allowed: boolean; retryAfter: number; remaining: number } {
  const now = Date.now();
  const existing = rateBuckets.get(key);
  const bucket = !existing || existing.resetAt <= now
    ? { resetAt: now + windowMs, count: 0 }
    : existing;

  bucket.count += 1;
  rateBuckets.set(key, bucket);

  if (rateBuckets.size > 10000) {
    for (const [bucketKey, value] of rateBuckets.entries()) {
      if (value.resetAt <= now) rateBuckets.delete(bucketKey);
    }
  }

  return {
    allowed: bucket.count <= limit,
    retryAfter: Math.max(1, Math.ceil((bucket.resetAt - now) / 1000)),
    remaining: Math.max(0, limit - bucket.count),
  };
}

function getIp(request: Request): string {
  return request.headers.get('cf-connecting-ip')
    ?? request.headers.get('x-forwarded-for')?.split(',')[0]?.trim()
    ?? 'unknown';
}

function loadClients(env: Env): ApiClient[] {
  try {
    const parsed = JSON.parse(env.MXRE_CLIENT_API_KEYS || '[]') as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((client) => client as Partial<ApiClient>)
      .map((client) => ({ ...client, key: client.key?.trim() }))
      .filter((client): client is ApiClient => Boolean(client.id && client.key));
  } catch {
    return [];
  }
}

function authenticate(request: Request, env: Env): ApiClient | null {
  const credentials = getCredentials(request);
  const apiKey = credentials.apiKey;
  const clientId = credentials.clientId;
  if (!apiKey) return null;

  if (env.MXRE_BUY_BOX_CLUB_KEY?.trim() && apiKey === env.MXRE_BUY_BOX_CLUB_KEY.trim()) {
    if (clientId && clientId !== 'buy_box_club_prod') return null;
    return { id: 'buy_box_club_prod', key: apiKey, environment: 'production' };
  }

  if (env.MXRE_BUY_BOX_CLUB_SANDBOX_KEY?.trim() && apiKey === env.MXRE_BUY_BOX_CLUB_SANDBOX_KEY.trim()) {
    if (clientId && clientId !== 'buy_box_club_sandbox') return null;
    return { id: 'buy_box_club_sandbox', key: apiKey, environment: 'sandbox' };
  }

  const matches = loadClients(env).filter((client) => client.key === apiKey);
  if (matches.length === 0) return null;
  if (clientId) return matches.find((client) => client.id === clientId) ?? null;
  return matches[0] ?? null;
}

function getCredentials(request: Request): { apiKey?: string; clientId?: string } {
  const headerApiKey = request.headers.get('x-api-key')?.trim();
  const headerClientId = request.headers.get('x-client-id')?.trim();
  if (headerApiKey) return { apiKey: headerApiKey, clientId: headerClientId };

  const auth = request.headers.get('authorization');
  if (!auth?.toLowerCase().startsWith('basic ')) return {};
  try {
    const decoded = atob(auth.slice(6).trim());
    const separator = decoded.indexOf(':');
    if (separator < 0) return {};
    return {
      clientId: decoded.slice(0, separator),
      apiKey: decoded.slice(separator + 1),
    };
  } catch {
    return {};
  }
}

function rateLimitResponse(retryAfter: number) {
  return json(
    { error: 'Rate limit exceeded', retry_after_seconds: retryAfter },
    429,
    { ...securityHeaders(), 'retry-after': String(retryAfter) },
  );
}

function shouldCache(request: Request, url: URL): boolean {
  if (request.method !== 'GET') return false;
  if (url.pathname === '/health') return false;
  if (!url.pathname.startsWith('/v1/')) return false;
  return true;
}

async function proxyToOrigin(request: Request, env: Env, client: ApiClient | null): Promise<Response> {
  if (!env.MXRE_ORIGIN_URL || !env.MXRE_UPSTREAM_API_KEY) {
    return json({ error: 'Gateway origin not configured for this endpoint' }, 503);
  }
  const incomingUrl = new URL(request.url);
  const originUrl = new URL(incomingUrl.pathname + incomingUrl.search, env.MXRE_ORIGIN_URL.trim());
  const requestId = request.headers.get('x-request-id') ?? crypto.randomUUID();

  const headers = new Headers(request.headers);
  headers.set('x-api-key', env.MXRE_UPSTREAM_API_KEY.trim());
  headers.set('x-client-id', 'legacy');
  if (client) headers.set('x-mxre-external-client-id', client.id);
  headers.set('x-request-id', requestId);
  headers.set('x-forwarded-host', incomingUrl.host);
  headers.delete('cf-connecting-ip');

  const upstreamResponse = await fetch(originUrl, {
    method: request.method,
    headers,
    body: request.method === 'GET' || request.method === 'HEAD' ? undefined : request.body,
    redirect: 'manual',
  });

  const responseHeaders = new Headers(upstreamResponse.headers);
  responseHeaders.set('x-request-id', requestId);
  if (client) responseHeaders.set('x-mxre-client-id', client.id);

  if (shouldCache(request, incomingUrl) && upstreamResponse.ok) {
    const seconds = Number(env.CACHE_SECONDS || '60');
    responseHeaders.set('cache-control', `private, max-age=${seconds}`);
    responseHeaders.set('cdn-cache-control', `max-age=${seconds}`);
  } else {
    responseHeaders.set('cache-control', 'no-store');
  }

  return new Response(upstreamResponse.body, {
    status: upstreamResponse.status,
    statusText: upstreamResponse.statusText,
    headers: responseHeaders,
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/health') {
      return json({ status: 'ok', edge: 'cloudflare', timestamp: new Date().toISOString() }, 200, securityHeaders());
    }

    const isDocs = url.pathname === '/docs';
    if (!url.pathname.startsWith('/v1/') && !isDocs) {
      return json({ error: 'Not found' }, 404, securityHeaders());
    }

    const ip = getIp(request);
    const preAuthLimit = rateLimit(`preauth:${ip}`, 60, 60_000);
    if (!preAuthLimit.allowed) return rateLimitResponse(preAuthLimit.retryAfter);

    const client = authenticate(request, env);
    if (!client) {
      const failedAuthLimit = rateLimit(`authfail:${ip}`, 10, 10 * 60_000);
      if (!failedAuthLimit.allowed) return rateLimitResponse(failedAuthLimit.retryAfter);
      return json(
        { error: 'Unauthorized' },
        401,
        isDocs
          ? { ...securityHeaders(), 'www-authenticate': 'Basic realm="MXRE Private API Docs", charset="UTF-8"' }
          : securityHeaders(),
      );
    }

    const clientLimit = rateLimit(`client:${client.id}:${ip}`, 600, 60_000);
    if (!clientLimit.allowed) return rateLimitResponse(clientLimit.retryAfter);

    if (!env.MXRE_ORIGIN_URL || !env.MXRE_UPSTREAM_API_KEY) {
      return json({ error: 'Gateway origin not configured for this endpoint' }, 503, securityHeaders());
    }

    const response = await proxyToOrigin(request, env, client);
    for (const [name, value] of Object.entries(securityHeaders())) response.headers.set(name, value);
    response.headers.set('x-ratelimit-remaining', String(clientLimit.remaining));
    return response;
  },
};
