import { Client } from "pg";
import { firstEnv, hydrateWindowsUserEnv } from "./env.js";

export type DbResult<T = Record<string, unknown>> = {
  rows: T[];
  rowCount: number;
};

export type DbClient = {
  query<T = Record<string, unknown>>(query: string, params?: unknown[]): Promise<DbResult<T>>;
  end(): Promise<void>;
};

function dollarQuotedString(value: string): string {
  let tag = "mxre";
  while (value.includes(`$${tag}$`)) tag = `${tag}x`;
  return `$${tag}$${value}$${tag}$`;
}

function sqlLiteral(value: unknown): string {
  if (value == null) return "null";
  if (Array.isArray(value)) return `array[${value.map(sqlLiteral).join(",")}]`;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  return dollarQuotedString(String(value));
}

function bindSql(query: string, params: unknown[] = []): string {
  return params.reduceRight((sql, value, index) => {
    const token = new RegExp(`\\$${index + 1}(?!\\d)`, "g");
    return sql.replace(token, sqlLiteral(value));
  }, query);
}

export async function makeDbClient(): Promise<DbClient> {
  hydrateWindowsUserEnv();
  const databaseUrl = firstEnv("MXRE_DIRECT_PG_URL", "DATABASE_URL", "POSTGRES_URL", "MXRE_PG_URL");
  if (!databaseUrl) {
    throw new Error("Set MXRE_DIRECT_PG_URL, DATABASE_URL, POSTGRES_URL, or MXRE_PG_URL.");
  }

  if (/^https?:\/\//i.test(databaseUrl)) {
    const endpoint = databaseUrl.replace(/\/$/, "");
    const key = firstEnv("SUPABASE_SERVICE_KEY") ?? "";
    return {
      async query<T = Record<string, unknown>>(query: string, params: unknown[] = []) {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: {
            apikey: key,
            Authorization: key ? `Bearer ${key}` : "",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ query: bindSql(query, params) }),
          signal: AbortSignal.timeout(180_000),
        });
        if (!response.ok) throw new Error(`pg/query ${response.status}: ${await response.text()}`);
        const body = await response.json();
        const rows = Array.isArray(body) ? body as T[] : [];
        return { rows, rowCount: rows.length };
      },
      async end() {},
    };
  }

  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  return client as unknown as DbClient;
}
