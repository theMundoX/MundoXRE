#!/usr/bin/env node
// Probe publicsearch.us to check if considerationAmount is in search results
import https from 'node:https';
import WebSocket from 'ws';
import crypto from 'node:crypto';

function getSession(host) {
  return new Promise((resolve, reject) => {
    https.get('https://' + host + '/', (res) => {
      let body = '';
      const cookies = (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
      res.on('data', d => body += d);
      res.on('end', () => {
        const ort = body.match(/__ort="([^"]+)"/)?.[1];
        if (!ort) return reject(new Error('no __ort in page. body len=' + body.length));
        resolve({ cookies, ort });
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function probe(host, dateRange) {
  console.log(`\nProbing ${host} for ${dateRange}...`);
  const session = await getSession(host);
  console.log('session ok');

  return new Promise((resolve) => {
    const ws = new WebSocket('wss://' + host + '/ws', {
      headers: { Cookie: session.cookies, Origin: 'https://' + host }
    });
    const timeout = setTimeout(() => {
      console.log('WebSocket timeout — no FULFILLED message after 30s');
      ws.close();
      resolve(null);
    }, 30000);

    ws.on('open', () => {
      console.log('ws connected, sending query...');
      ws.send(JSON.stringify({
        type: '@kofile/FETCH_DOCUMENTS/v4',
        payload: {
          query: {
            limit: '5', offset: '0',
            department: 'RP',
            recordedDateRange: dateRange,
            searchOcrText: false,
            searchType: 'quickSearch'
          },
          workspaceID: crypto.randomUUID().substring(0, 20)
        },
        authToken: session.ort,
        ip: '',
        correlationId: crypto.randomUUID(),
        sync: true
      }));
    });

    ws.on('message', data => {
      const msg = JSON.parse(data.toString());
      console.log('  received:', msg.type);
      if (msg.type?.includes('FULFILLED') || msg.type?.includes('REJECTED')) {
        clearTimeout(timeout);
        const byOrder = msg.payload?.data?.byOrder || [];
        const byHash = msg.payload?.data?.byHash || {};
        console.log('  total records:', msg.payload?.meta?.numRecords);
        byOrder.slice(0, 3).forEach(id => {
          const d = byHash[id];
          if (!d) return;
          console.log(`  doc ${id}: docType=${d.docTypeCode} | considerationAmount=${d.considerationAmount} | date=${d.recordedDate}`);
          console.log(`    all keys: ${Object.keys(d).join(', ')}`);
        });
        ws.close();
        resolve(byOrder.length > 0 ? byHash[byOrder[0]] : null);
      }
    });

    ws.on('error', err => {
      clearTimeout(timeout);
      console.error('ws error:', err.message);
      resolve(null);
    });
    ws.on('close', code => {
      if (code !== 1000 && code !== 1005) console.log('ws close code:', code);
    });
  });
}

await probe('cuyahoga.oh.publicsearch.us', '2025-01-01,2025-01-31');
await probe('franklin.oh.publicsearch.us', '2025-01-01,2025-01-31');
console.log('\nDone.');
