export interface Env {
  MXRE_ORIGIN_URL: string;
  MXRE_UPSTREAM_API_KEY: string;
  MXRE_CLIENT_API_KEYS: string;
  CACHE_SECONDS?: string;
}

type ApiClient = {
  id: string;
  key: string;
  environment?: string;
  monthlyQuota?: number;
};

function json(body: unknown, status = 200, headers: HeadersInit = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      ...headers,
    },
  });
}

function loadClients(env: Env): ApiClient[] {
  try {
    const parsed = JSON.parse(env.MXRE_CLIENT_API_KEYS || '[]') as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((client) => client as Partial<ApiClient>)
      .filter((client): client is ApiClient => Boolean(client.id && client.key));
  } catch {
    return [];
  }
}

function authenticate(request: Request, env: Env): ApiClient | null {
  const apiKey = request.headers.get('x-api-key');
  const clientId = request.headers.get('x-client-id');
  if (!apiKey) return null;

  const matches = loadClients(env).filter((client) => client.key === apiKey);
  if (matches.length === 0) return null;
  if (clientId) return matches.find((client) => client.id === clientId) ?? null;
  return matches[0] ?? null;
}

function shouldCache(request: Request, url: URL): boolean {
  if (request.method !== 'GET') return false;
  if (url.pathname === '/health') return false;
  if (!url.pathname.startsWith('/v1/')) return false;
  return true;
}

async function proxyToOrigin(request: Request, env: Env, client: ApiClient | null): Promise<Response> {
  const incomingUrl = new URL(request.url);
  const originUrl = new URL(incomingUrl.pathname + incomingUrl.search, env.MXRE_ORIGIN_URL);
  const requestId = request.headers.get('x-request-id') ?? crypto.randomUUID();

  const headers = new Headers(request.headers);
  headers.set('x-api-key', env.MXRE_UPSTREAM_API_KEY);
  if (client) headers.set('x-client-id', client.id);
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
      return json({ status: 'ok', edge: 'cloudflare', timestamp: new Date().toISOString() });
    }

    if (!env.MXRE_ORIGIN_URL || !env.MXRE_UPSTREAM_API_KEY) {
      return json({ error: 'Gateway misconfigured' }, 500);
    }

    if (!url.pathname.startsWith('/v1/')) {
      return json({ error: 'Not found' }, 404);
    }

    const client = authenticate(request, env);
    if (!client) {
      return json({ error: 'Unauthorized' }, 401);
    }

    return proxyToOrigin(request, env, client);
  },
};
