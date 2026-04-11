#!/usr/bin/env node
// Check consideration amounts in publicsearch results — search mortgage doc types specifically
import https from 'node:https';
import WebSocket from 'ws';
import crypto from 'node:crypto';

function getSession(host) {
  return new Promise((resolve, reject) => {
    https.get('https://' + host + '/', {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36' }
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

function wsQuery(host, session, query, delayMs = 2000) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket('wss://' + host + '/ws', {
      headers: { 'Cookie': session.cookies, 'Origin': 'https://' + host, 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
    });
    const timeout = setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 40000);

    ws.on('open', () => {
      setTimeout(() => {
        ws.send(JSON.stringify({
          type: '@kofile/FETCH_DOCUMENTS/v4',
          payload: { query, workspaceID: crypto.randomUUID().substring(0, 20) },
          authToken: session.ort,
          ip: '',
          correlationId: crypto.randomUUID(),
          sync: true
        }));
      }, delayMs);
    });

    ws.on('message', (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.type?.includes('FULFILLED') || msg.type?.includes('REJECTED')) {
        clearTimeout(timeout);
        ws.close();
        resolve(msg.payload);
      }
    });

    ws.on('error', err => { clearTimeout(timeout); reject(err); });
    ws.on('close', (code) => { if (code !== 1000 && code !== 1005) reject(new Error('ws close ' + code)); });
  });
}

const host = 'cuyahoga.oh.publicsearch.us';
const session = await getSession(host);
console.log('session ok');

// Search for mortgage documents
const payload = await wsQuery(host, session, {
  limit: '10',
  offset: '0',
  department: 'RP',
  recordedDateRange: '2025-01-01,2025-01-31',
  searchOcrText: false,
  searchType: 'quickSearch',
  _docTypes: ['MORT', 'MTG', 'M', 'MO', 'MORTGAGE', 'DOT']
});

const { byOrder, byHash } = payload?.data || {};
console.log('total:', payload?.meta?.numRecords, 'returned:', byOrder?.length);

if (byOrder?.length) {
  console.log('\nFull document sample (first 3):');
  byOrder.slice(0, 3).forEach(id => {
    const doc = byHash[id];
    console.log('\n---');
    console.log(JSON.stringify(doc, null, 2));
  });
}
