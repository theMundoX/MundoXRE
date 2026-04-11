#!/usr/bin/env node
// Probe publicsearch document detail API for loan amounts
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

function wsCall(host, session, type, payload) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket('wss://' + host + '/ws', {
      headers: { 'Cookie': session.cookies, 'Origin': 'https://' + host, 'User-Agent': 'Mozilla/5.0' }
    });
    const timeout = setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 40000);

    ws.on('open', () => {
      setTimeout(() => {
        ws.send(JSON.stringify({
          type,
          payload: { ...payload, workspaceID: payload.workspaceID || crypto.randomUUID().substring(0, 20) },
          authToken: session.ort,
          ip: '',
          correlationId: crypto.randomUUID(),
          sync: true
        }));
      }, 2000);
    });

    ws.on('message', (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.type?.includes('FULFILLED') || msg.type?.includes('REJECTED')) {
        clearTimeout(timeout);
        ws.close();
        resolve(msg);
      }
    });

    ws.on('error', err => { clearTimeout(timeout); reject(err); });
    ws.on('close', code => { if (code !== 1000 && code !== 1005) reject(new Error('ws close ' + code)); });
  });
}

const host = 'cuyahoga.oh.publicsearch.us';
const session = await getSession(host);

// First get a list of mortgage docs
console.log('Fetching mortgage docs...');
const searchResult = await wsCall(host, session, '@kofile/FETCH_DOCUMENTS/v4', {
  query: {
    limit: '3', offset: '0',
    department: 'RP',
    recordedDateRange: '2025-01-01,2025-01-31',
    searchOcrText: false,
    searchType: 'quickSearch',
    _docTypes: ['MORT']
  }
});

const { byOrder, byHash } = searchResult.payload?.data || {};
console.log('Got', byOrder?.length, 'mortgage docs');

if (!byOrder?.length) { console.log('no docs'); process.exit(0); }

const sampleDocId = byOrder[0];
const sampleDoc = byHash[sampleDocId];
console.log('\nSample doc:', sampleDoc?.instrumentNumber, 'docId:', sampleDocId);

// Try to fetch document detail
// The message type for detail is likely @kofile/FETCH_DOCUMENT_DETAIL or similar
// Let's try common patterns

const detailTypes = [
  '@kofile/FETCH_DOCUMENT_DETAIL/v0',
  '@kofile/FETCH_DOCUMENT_DETAIL/v1',
  '@kofile/FETCH_DOCUMENT/v0',
  '@kofile/FETCH_DOCUMENT/v1',
];

for (const type of detailTypes) {
  try {
    console.log('\nTrying', type, '...');
    const result = await wsCall(host, session, type, {
      docId: sampleDocId,
      id: sampleDocId,
      documentId: sampleDocId,
      rsId: sampleDoc?.rsId,
      workspaceID: crypto.randomUUID().substring(0, 20)
    });
    console.log('Result type:', result.type);
    const doc = result.payload;
    if (doc && typeof doc === 'object') {
      Object.entries(doc).forEach(([k, v]) => {
        if (k.toLowerCase().includes('amount') || k.toLowerCase().includes('consider') || k.toLowerCase().includes('loan') || k.toLowerCase().includes('principal')) {
          console.log('  AMOUNT FIELD:', k, '=', v);
        }
      });
      console.log('  Keys:', Object.keys(doc).sort().join(', '));
    }
  } catch (e) {
    console.log('  Error:', e.message);
  }
}
