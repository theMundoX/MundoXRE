#!/usr/bin/env node
// Simple probe: get docs and show full JSON to check considerationAmount
import https from 'node:https';
import WebSocket from 'ws';
import crypto from 'node:crypto';

function getSession(host) {
  return new Promise((resolve, reject) => {
    https.get('https://' + host + '/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
    }, (res) => {
      let body = '';
      const cookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
      res.on('data', d => body += d);
      res.on('end', () => {
        const ort = body.match(/__ort="([^"]+)"/)?.[1];
        if (!ort) return reject(new Error('no __ort'));
        resolve({ cookies, ort });
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

const host = 'cuyahoga.oh.publicsearch.us';
const session = await getSession(host);
console.log('session ok, ort:', session.ort);

const ws = new WebSocket('wss://' + host + '/ws', {
  headers: { 'Cookie': session.cookies, 'Origin': 'https://' + host, 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
});

ws.on('open', () => {
  console.log('ws open, waiting 2s...');
  setTimeout(() => {
    const msg = {
      type: '@kofile/FETCH_DOCUMENTS/v4',
      payload: {
        query: {
          limit: '5', offset: '0',
          department: 'RP',
          recordedDateRange: '2025-01-01,2025-01-31',
          searchOcrText: false,
          searchType: 'quickSearch'
          // no docType filter — get all
        },
        workspaceID: crypto.randomUUID().substring(0, 20)
      },
      authToken: session.ort,
      ip: '',
      correlationId: crypto.randomUUID(),
      sync: true
    };
    console.log('Sending query...');
    ws.send(JSON.stringify(msg));
  }, 2000);
});

ws.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('type:', msg.type);
  if (msg.type?.includes('FULFILLED')) {
    const { byOrder, byHash } = msg.payload?.data || {};
    console.log('total:', msg.payload?.meta?.numRecords, 'docs:', byOrder?.length);

    // Show first 3 docs with all fields
    byOrder?.slice(0, 3).forEach((id, i) => {
      const doc = byHash[id];
      console.log(`\n=== DOC ${i+1} (id=${id}) ===`);
      console.log('  docTypeCode:', doc?.docTypeCode);
      console.log('  docType:', doc?.docType);
      console.log('  considerationAmount:', doc?.considerationAmount);
      console.log('  ALL keys:', Object.keys(doc || {}).sort().join(', '));
      // Check for any amount-related fields
      Object.entries(doc || {}).forEach(([k, v]) => {
        if (k.toLowerCase().includes('amount') || k.toLowerCase().includes('consider') || k.toLowerCase().includes('price')) {
          console.log('  AMOUNT FIELD:', k, '=', v);
        }
      });
    });

    ws.close();
    process.exit(0);
  }
  if (msg.type?.includes('REJECTED')) {
    console.log('REJECTED:', JSON.stringify(msg.payload));
    process.exit(1);
  }
});

ws.on('error', err => { console.error('ws error:', err.message); process.exit(1); });
ws.on('close', code => console.log('close:', code));

setTimeout(() => { console.log('timeout'); process.exit(1); }, 45000);
