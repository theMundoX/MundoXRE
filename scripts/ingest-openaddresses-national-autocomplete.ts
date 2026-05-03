#!/usr/bin/env tsx

import "dotenv/config";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { spawn } from "node:child_process";

const args = process.argv.slice(2);
const valueArg = (name: string) => {
  const prefix = `--${name}=`;
  return args.find((arg) => arg.startsWith(prefix))?.slice(prefix.length) ?? null;
};

const configPath = valueArg("config") ?? join("config", "openaddresses-us-regions.json");
const onlyRegion = valueArg("region");
const onlyState = valueArg("state")?.toUpperCase() ?? null;
const limit = valueArg("limit");

const config = JSON.parse(await readFile(configPath, "utf8")) as {
  regions: Array<{
    id: string;
    label: string;
    states: string[];
    urlEnv: string;
    fallbackUrl: string;
  }>;
};

for (const region of config.regions) {
  if (onlyRegion && onlyRegion !== region.id) continue;
  if (onlyState && !region.states.includes(onlyState)) continue;

  const url = process.env[region.urlEnv] || region.fallbackUrl;
  const commandArgs = [
    "tsx",
    "scripts/ingest-openaddresses-autocomplete.ts",
    `--url=${url}`,
    `--source=openaddresses_${region.id}`,
  ];
  if (onlyState) commandArgs.push(`--state=${onlyState}`);
  if (limit) commandArgs.push(`--limit=${limit}`);

  console.log(`\nImporting ${region.label} from ${url}`);
  await exec("npx", commandArgs);
}

function exec(command: string, commandArgs: string[]) {
  return new Promise<void>((resolve, reject) => {
    const executable = process.platform === "win32" && command === "npx" ? "npx.cmd" : command;
    const child = spawn(executable, commandArgs, { stdio: "inherit", shell: false });
    child.on("close", (code) => code === 0 ? resolve() : reject(new Error(`${command} exited ${code}`)));
    child.on("error", reject);
  });
}
