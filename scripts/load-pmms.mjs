/**
 * Load Freddie Mac PMMS weekly history into pmms_weekly.
 * CSV cols: date,pmms30,pmms30p,pmms15,pmms15p,pmms51,pmms51p,pmms51m,pmms51spread
 */
import pkg from "pg";
import { createReadStream } from "fs";
import { parse } from "csv-parse";
const { Pool } = pkg;

const pool = new Pool({
  host: "207.244.225.239",
  port: 5432,
  database: "postgres",
  user: "postgres.your-tenant-id",
  password: "d6168ff6e8d9559d62642418bafb3d17",
  max: 2,
});

function parseDate(mdy) {
  const [m, d, y] = mdy.split("/").map(Number);
  return `${y}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}
function num(v) {
  const s = String(v ?? "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

async function main() {
  const rows = [];
  await new Promise((res, rej) => {
    createReadStream("C:/Users/msanc/mxre/data/hmda/pmms_history.csv")
      .pipe(parse({ columns: true, skip_empty_lines: true, trim: true }))
      .on("data", (r) => rows.push(r))
      .on("end", res)
      .on("error", rej);
  });
  console.log(`Parsed ${rows.length} PMMS weeks`);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("TRUNCATE pmms_weekly");
    const BATCH = 500;
    let loaded = 0;
    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const values = [];
      const params = [];
      let p = 1;
      for (const r of batch) {
        values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
        params.push(
          parseDate(r.date),
          num(r.pmms30),
          num(r.pmms15),
          num(r.pmms51),
          num(r.pmms30p),
          num(r.pmms15p)
        );
      }
      await client.query(
        `INSERT INTO pmms_weekly
         (week_ending, rate_30yr_fixed, rate_15yr_fixed, rate_5_1_arm, points_30yr, points_15yr)
         VALUES ${values.join(",")}
         ON CONFLICT (week_ending) DO NOTHING`,
        params
      );
      loaded += batch.length;
    }
    await client.query("COMMIT");
    const latest = await client.query(
      "SELECT week_ending, rate_30yr_fixed FROM pmms_weekly ORDER BY week_ending DESC LIMIT 5"
    );
    console.log(`Loaded ${loaded} weeks. Most recent:`);
    for (const r of latest.rows) console.log(" ", r.week_ending, r.rate_30yr_fixed);
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
    await pool.end();
  }
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
