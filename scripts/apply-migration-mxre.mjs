#!/usr/bin/env node
// Apply a SQL migration to the self-hosted MXRE Supabase via pg-meta.
// Handles dollar-quoted function bodies properly.
import fs from "node:fs";

import "dotenv/config";
const MXRE_PG  = (process.env.SUPABASE_URL ?? "${process.env.SUPABASE_URL}") + "/pg/query";
const MXRE_SVC = process.env.SUPABASE_SERVICE_KEY;
if (!MXRE_SVC) { console.error("Set SUPABASE_SERVICE_KEY in mxre/.env"); process.exit(1); }

function splitSql(sql) {
  const out = [];
  let cur = "", i = 0;
  let dollarTag = null, inSingle = false, inLine = false, inBlock = false;
  while (i < sql.length) {
    const c = sql[i], rest = sql.slice(i);
    if (inLine) { cur += c; if (c === "\n") inLine = false; i++; continue; }
    if (inBlock) { cur += c; if (c === "*" && sql[i+1] === "/") { cur += "/"; i += 2; inBlock = false; continue; } i++; continue; }
    if (!dollarTag && !inSingle) {
      if (c === "-" && sql[i+1] === "-") { inLine = true; cur += c; i++; continue; }
      if (c === "/" && sql[i+1] === "*") { inBlock = true; cur += c; i++; continue; }
    }
    if (dollarTag) {
      if (rest.startsWith(dollarTag)) { cur += dollarTag; i += dollarTag.length; dollarTag = null; continue; }
      cur += c; i++; continue;
    }
    if (inSingle) {
      cur += c;
      if (c === "'" && sql[i+1] !== "'") inSingle = false;
      else if (c === "'" && sql[i+1] === "'") { cur += sql[i+1]; i++; }
      i++; continue;
    }
    const dq = rest.match(/^\$([A-Za-z_]*)\$/);
    if (dq) { dollarTag = dq[0]; cur += dollarTag; i += dollarTag.length; continue; }
    if (c === "'") { inSingle = true; cur += c; i++; continue; }
    if (c === ";") { if (cur.trim()) out.push(cur.trim()); cur = ""; i++; continue; }
    cur += c; i++;
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

const file = process.argv[2] ?? "migrations/002_knowledge_graph_and_events.sql";
const sql = fs.readFileSync(file, "utf8");
const statements = splitSql(sql);

console.log(`Applying ${file}: ${statements.length} statements`);
let ok = 0, skipped = 0, failed = 0;
for (let i = 0; i < statements.length; i++) {
  const stmt = statements[i] + ";";
  const preview = stmt.replace(/\s+/g, " ").slice(0, 70);
  try {
    const res = await fetch(MXRE_PG, {
      method: "POST",
      headers: { apikey: MXRE_SVC, Authorization: `Bearer ${MXRE_SVC}`, "Content-Type": "application/json" },
      body: JSON.stringify({ query: stmt }),
    });
    if (res.ok) { console.log(`  ✓ [${i}] ${preview}`); ok++; }
    else {
      const text = await res.text();
      if (text.includes("already exists")) { console.log(`  ○ [${i}] (exists) ${preview}`); skipped++; }
      else { console.log(`  ✗ [${i}] ${res.status} ${text.slice(0, 120)} :: ${preview}`); failed++; }
    }
  } catch (err) {
    console.log(`  ✗ [${i}] ${err.message} :: ${preview}`); failed++;
  }
}
console.log(`\nResult: ${ok} applied · ${skipped} existing · ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
