#!/usr/bin/env node
// Debug all WS messages including REJECTED types
import https from 'node:https';
import WebSocket from 'ws';
import crypto from 'node:crypto';

function getSession(host) {
  return new Promise((resolve, reject) => {
    const req = https.get('https://' + host + '/', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
      }
    }, (res) => {
      let body = '';
      const rawCookies = res.headers['set-cookie'] || [];
      const cookies = rawCookies.map(c => c.split(';')[0]).join('; ');
      res.on('data', d => body += d);
      res.on('end', () => {
        const ort = body.match(/__ort="([^"]+)"/)?.[1];
        if (!ort) return reject(new Error('no __ort. cookies=' + cookies));
        resolve({ cookies, ort });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
  });
}

async function probe(host) {
  console.log(`\n=== ${host} ===`);
  const session = await getSession(host);
  console.log('ort:', session.ort);
  console.log('cookies:', session.cookies);

  return new Promise((resolve) => {
    const ws = new WebSocket('wss://' + host + '/ws', {
      headers: {
        'Cookie': session.cookies,
        'Origin': 'https://' + host,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
      },
      perMessageDeflate: false,
    });

    let msgCount = 0;
    const timeout = setTimeout(() => {
      console.log('Timeout — received', msgCount, 'messages');
      ws.close();
      resolve();
    }, 40000);

    ws.on('open', () => {
      console.log('ws open (readyState:', ws.readyState, ')');
      const msg = {
        type: '@kofile/FETCH_DOCUMENTS/v4',
        payload: {
          query: {
            limit: '3',
            offset: '0',
            department: 'RP',
            recordedDateRange: '2025-01-01,2025-01-31',
            searchOcrText: false,
            searchType: 'quickSearch'
          },
          workspaceID: crypto.randomUUID().substring(0, 20)
        },
        authToken: session.ort,
        ip: '',
        correlationId: crypto.randomUUID(),
        sync: true
      };
      console.log('Sending:', JSON.stringify(msg).substring(0, 300));
      ws.send(JSON.stringify(msg));
    });

    ws.on('message', (data, isBinary) => {
      msgCount++;
      if (isBinary) {
        console.log('BINARY MSG #' + msgCount + ' len=' + data.length);
        return;
      }
      const str = data.toString();
      console.log('MSG #' + msgCount + ' len=' + str.length + ':', str.substring(0, 1000));
      // Check for any interesting type
      try {
        const parsed = JSON.parse(str);
        if (parsed.type) {
          console.log('  -> type:', parsed.type);
          console.log('  -> payload keys:', Object.keys(parsed.payload || {}).join(', '));
        }
        if (msgCount >= 3 || str.includes('FULFILLED') || str.includes('REJECTED')) {
          clearTimeout(timeout);
          ws.close();
          resolve();
        }
      } catch {}
    });

    ws.on('ping', (data) => {
      console.log('PING received, sending pong');
      ws.pong(data);
    });

    ws.on('pong', () => console.log('PONG received'));

    ws.on('error', err => {
      clearTimeout(timeout);
      console.error('WS ERROR:', err.message, 'code:', err.code);
      resolve();
    });

    ws.on('close', (code, reason) => {
      console.log('WS CLOSE code=' + code + ' reason=' + reason?.toString());
    });

    ws.on('unexpected-response', (req, res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        console.log('UNEXPECTED HTTP', res.statusCode, ':', body.substring(0, 500));
        clearTimeout(timeout);
        resolve();
      });
    });
  });
}

await probe('cuyahoga.oh.publicsearch.us');
console.log('\nDone.');
