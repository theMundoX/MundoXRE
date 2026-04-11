#!/usr/bin/env node
// Wait for initial WS state message before sending query
import https from 'node:https';
import WebSocket from 'ws';
import crypto from 'node:crypto';

function getSession(host) {
  return new Promise((resolve, reject) => {
    https.get('https://' + host + '/', {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      }
    }, (res) => {
      let body = '';
      const rawCookies = res.headers['set-cookie'] || [];
      const cookies = rawCookies.map(c => c.split(';')[0]).join('; ');
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

async function probe(host) {
  console.log(`\n=== ${host} ===`);
  const session = await getSession(host);
  console.log('ort:', session.ort);

  return new Promise((resolve) => {
    const ws = new WebSocket('wss://' + host + '/ws', {
      headers: {
        'Cookie': session.cookies,
        'Origin': 'https://' + host,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      }
    });

    let msgCount = 0;
    let querySent = false;
    const timeout = setTimeout(() => {
      console.log('Timeout — received', msgCount, 'messages, querySent:', querySent);
      ws.close();
      resolve();
    }, 45000);

    ws.on('open', () => {
      console.log('ws open — waiting for initial server message before querying...');
      // Wait 2 seconds for any initial message, then send query regardless
      setTimeout(() => {
        if (!querySent) {
          console.log('No initial message after 2s — sending query anyway');
          sendQuery();
        }
      }, 2000);
    });

    function sendQuery() {
      querySent = true;
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
      console.log('Sending query...');
      ws.send(JSON.stringify(msg));
    }

    ws.on('message', (data, isBinary) => {
      msgCount++;
      const str = isBinary ? '[BINARY len=' + data.length + ']' : data.toString();
      console.log(`MSG #${msgCount}: ${str.substring(0, 500)}`);
      try {
        const parsed = JSON.parse(str);
        console.log('  type:', parsed.type);
        // If this is initial connection message, send query now
        if (!querySent && parsed.type?.includes('CONNECT')) {
          sendQuery();
        }
        if (str.includes('FULFILLED') || str.includes('REJECTED') || msgCount >= 5) {
          clearTimeout(timeout);
          ws.close();
          resolve();
        }
      } catch {}
    });

    ws.on('ping', d => { console.log('ping'); ws.pong(d); });
    ws.on('error', err => { clearTimeout(timeout); console.error('WS ERROR:', err.message); resolve(); });
    ws.on('close', (code, reason) => console.log('close:', code, reason?.toString()));
    ws.on('unexpected-response', (req, res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => { console.log('HTTP', res.statusCode, ':', body.substring(0, 300)); clearTimeout(timeout); resolve(); });
    });
  });
}

await probe('cuyahoga.oh.publicsearch.us');
