#!/usr/bin/env tsx
import "dotenv/config";
import { createReadStream, existsSync } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, join, normalize } from "node:path";

const PORT = Number(process.argv.find(a => a.startsWith("--port="))?.split("=")[1] ?? "8765");
const HOST = process.argv.find(a => a.startsWith("--host="))?.split("=")[1] ?? "127.0.0.1";
const ROOT = join(process.cwd(), "logs", "market-refresh");
const MXRE_API = process.env.MXRE_API_BASE_URL ?? "https://api.mxre.mundox.ai";

const mime: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
};

function send(res: Parameters<Parameters<typeof createServer>[0]>[1], status: number, body: unknown) {
  const text = typeof body === "string" ? body : JSON.stringify(body, null, 2);
  res.writeHead(status, {
    "content-type": typeof body === "string" ? "text/plain; charset=utf-8" : "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(text);
}

function safePath(pathname: string): string | null {
  const requested = pathname === "/" ? "/dallas-coverage-dashboard.html" : pathname;
  const decoded = decodeURIComponent(requested.split("?")[0] ?? "");
  const full = normalize(join(ROOT, decoded));
  return full.startsWith(ROOT) ? full : null;
}

createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", `http://${HOST}:${PORT}`);
    if (url.pathname === "/api/bbc/property") {
      const apiKey = req.headers["x-api-key"];
      if (!apiKey || Array.isArray(apiKey)) return send(res, 401, { error: "Missing x-api-key" });
      const upstream = new URL("/v1/bbc/property", MXRE_API);
      for (const key of ["address", "city", "state", "zip"]) {
        const value = url.searchParams.get(key);
        if (value) upstream.searchParams.set(key, value);
      }
      const response = await fetch(upstream, {
        headers: {
          "x-client-id": "buy_box_club_sandbox",
          "x-api-key": apiKey,
        },
        signal: AbortSignal.timeout(60_000),
      });
      const text = await response.text();
      res.writeHead(response.status, {
        "content-type": response.headers.get("content-type") ?? "application/json; charset=utf-8",
        "cache-control": "no-store",
      });
      res.end(text);
      return;
    }

    const file = safePath(url.pathname);
    if (!file || !existsSync(file)) return send(res, 404, "Not found");
    const info = await stat(file);
    if (!info.isFile()) return send(res, 404, "Not found");
    res.writeHead(200, {
      "content-type": mime[extname(file).toLowerCase()] ?? "application/octet-stream",
      "cache-control": "no-store",
    });
    createReadStream(file).pipe(res);
  } catch (error) {
    send(res, 500, { error: error instanceof Error ? error.message : String(error) });
  }
}).listen(PORT, HOST, () => {
  console.log(`MXRE market dashboard server ready at http://${HOST}:${PORT}/dallas-coverage-dashboard.html`);
});
