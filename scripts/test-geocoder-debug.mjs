/**
 * Standalone debug: pull 100 real rows, submit to Census, print full response.
 */
import pkg from "pg";
import { writeFileSync } from "fs";
import { spawn } from "child_process";
const { Pool } = pkg;

const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 1,
});

function csvCell(v) {
  if (v == null) return "";
  const s = String(v).replace(/"/g, '""');
  return /[",\n]/.test(s) ? `"${s}"` : s;
}

const c = await pool.connect();
const r = await c.query(
  `SELECT id, address, city, state_code, zip
     FROM properties
    WHERE latitude IS NULL AND address IS NOT NULL AND state_code IS NOT NULL
    LIMIT 100`
);
console.log(`Got ${r.rows.length} rows. First row:`, r.rows[0]);

const csv = r.rows
  .map((row) => `${row.id},${csvCell(row.address)},${csvCell(row.city)},${csvCell(row.state_code)},${csvCell(row.zip)}`)
  .join("\n");
writeFileSync("/tmp/dbg.csv", csv);
console.log(`Wrote /tmp/dbg.csv (${csv.length} bytes)`);
console.log(`First 3 lines:\n${csv.split("\n").slice(0, 3).join("\n")}`);

console.log("\nCalling Census batch endpoint...");
const t0 = Date.now();
const resp = await new Promise((resolve, reject) => {
  const child = spawn("curl", [
    "-s",
    "-F",
    "addressFile=@/tmp/dbg.csv",
    "-F",
    "benchmark=Public_AR_Current",
    "https://geocoding.geo.census.gov/geocoder/locations/addressbatch",
  ]);
  let out = "";
  child.stdout.on("data", (d) => (out += d.toString()));
  child.stderr.on("data", (d) => process.stderr.write("[curl-err] " + d));
  child.on("exit", (code) => {
    if (code !== 0) reject(new Error(`curl exit ${code}`));
    else resolve(out);
  });
});
const elapsed = Date.now() - t0;
console.log(`Census response in ${elapsed}ms (${resp.length} bytes)`);
console.log(`First 1000 chars:\n${resp.slice(0, 1000)}`);
const lines = resp.split(/\r?\n/).filter((l) => l);
console.log(`\nTotal lines: ${lines.length}`);
const matches = lines.filter((l) => l.includes('"Match"'));
const nomatches = lines.filter((l) => l.includes('"No_Match"'));
console.log(`Matches: ${matches.length}, No_Match: ${nomatches.length}, Other: ${lines.length - matches.length - nomatches.length}`);

c.release();
await pool.end();
