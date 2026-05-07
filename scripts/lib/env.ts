import { execFileSync } from "node:child_process";

const WINDOWS_USER_ENV_NAMES = [
  "REALESTATEAPI_KEY",
  "REALESTATE_API_KEY",
  "REALESTATEAPI_API_KEY",
  "RAPIDAPI_KEY",
  "ZILLOW_RAPIDAPI_KEY",
  "ZILLOW_RAPIDAPI_PROVIDER",
  "ZILLOW_RAPIDAPI_HOST",
  "MXRE_DIRECT_PG_URL",
  "MXRE_PG_URL",
  "DATABASE_URL",
  "POSTGRES_URL",
  "SUPABASE_URL",
  "SUPABASE_SERVICE_KEY",
  "MXRE_BUY_BOX_CLUB_SANDBOX_KEY",
];

const userEnvCache = new Map<string, string | undefined>();

export function firstEnv(...names: string[]): string | undefined {
  for (const name of names) {
    const value = process.env[name] ?? readWindowsUserEnv(name);
    if (value) return value;
  }
  return undefined;
}

export function hydrateWindowsUserEnv(names = WINDOWS_USER_ENV_NAMES): void {
  if (process.platform !== "win32") return;
  for (const name of names) {
    if (process.env[name]) continue;
    const value = readWindowsUserEnv(name);
    if (value) process.env[name] = value;
  }
}

export function readWindowsUserEnv(name: string): string | undefined {
  if (process.platform !== "win32") return undefined;
  if (userEnvCache.has(name)) return userEnvCache.get(name);
  try {
    const output = execFileSync("reg.exe", ["query", "HKCU\\Environment", "/v", name], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      windowsHide: true,
    });
    const line = output.split(/\r?\n/).find((row) => row.includes(name));
    const match = line?.match(/\sREG_(?:SZ|EXPAND_SZ)\s+(.+)$/);
    const value = match?.[1]?.trim() || undefined;
    userEnvCache.set(name, value);
    return value;
  } catch {
    userEnvCache.set(name, undefined);
    return undefined;
  }
}
