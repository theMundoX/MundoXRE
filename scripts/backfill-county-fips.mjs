// Backfill mortgage_records.county_fips (CHAR(5) = state_fips || county_fips)
// Source of truth: existing `counties` table, joined via properties.county_id.
// No Census download needed — `counties` already has complete FIPS for all 717 rows.
import pg from 'pg';

const client = new pg.Client({
  host: '207.244.225.239',
  port: 5432,
  database: 'postgres',
  user: 'postgres.your-tenant-id',
  password: 'd6168ff6e8d9559d62642418bafb3d17',
  statement_timeout: 0,
});

async function main() {
  await client.connect();

  console.log('[1] ALTER TABLE + CREATE INDEX');
  await client.query(`ALTER TABLE mortgage_records ADD COLUMN IF NOT EXISTS county_fips CHAR(5)`);
  await client.query(`CREATE INDEX IF NOT EXISTS idx_mortgage_records_county_fips ON mortgage_records(county_fips)`);

  console.log('[2] before counts');
  let r = await client.query(`SELECT COUNT(*)::bigint total, COUNT(county_fips)::bigint with_fips FROM mortgage_records`);
  console.log(r.rows[0]);

  console.log('[3] running backfill UPDATE');
  const t0 = Date.now();
  const up = await client.query(`
    UPDATE mortgage_records m
    SET county_fips = (c.state_fips || c.county_fips)
    FROM properties p, counties c
    WHERE m.property_id = p.id
      AND p.county_id = c.id
      AND m.county_fips IS NULL
  `);
  console.log(`updated rows: ${up.rowCount} in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

  console.log('[4] after counts');
  r = await client.query(`SELECT COUNT(*)::bigint total, COUNT(county_fips)::bigint with_fips, (COUNT(*) - COUNT(county_fips))::bigint null_fips FROM mortgage_records`);
  console.log(r.rows[0]);

  console.log('[5] residual null breakdown: has property_id?');
  r = await client.query(`SELECT (property_id IS NULL) AS no_property_id, COUNT(*)::bigint FROM mortgage_records WHERE county_fips IS NULL GROUP BY 1`);
  console.log(r.rows);

  console.log('[6] residual nulls with property_id, by properties.state_code');
  r = await client.query(`
    SELECT p.state_code, COUNT(*)::bigint
    FROM mortgage_records m
    JOIN properties p ON p.id = m.property_id
    WHERE m.county_fips IS NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 20
  `);
  console.log(r.rows);

  await client.end();
}

main().catch((e) => { console.error(e); process.exit(1); });
