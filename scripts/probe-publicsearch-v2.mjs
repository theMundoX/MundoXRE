#!/usr/bin/env node
// Probe all WS message types to debug publicsearch.us
import https from 'node:https';
import WebSocket from 'ws';
import crypto from 'node:crypto';

function getSession(host) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      }
    };
    https.get('https://' + host + '/', options, (res) => {
      let body = '';
      const rawCookies = res.headers['set-cookie'] || [];
      const cookies = rawCookies.map(c => c.split(';')[0]).join('; ');
      res.on('data', d => body += d);
      res.on('end', () => {
        const ort = body.match(/__ort="([^"]+)"/)?.[1];
        if (!ort) return reject(new Error('no __ort. cookies: ' + cookies + ' body snippet: ' + body.substring(0, 500)));
        resolve({ cookies, ort });
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function probe(host) {
  console.log(`\n=== ${host} ===`);
  const session = await getSession(host);
  console.log('ort:', session.ort.substring(0, 20) + '...');
  console.log('cookies:', session.cookies.substring(0, 100) + '...');

  return new Promise((resolve) => {
    const ws = new WebSocket('wss://' + host + '/ws', {
      headers: {
        Cookie: session.cookies,
        Origin: 'https://' + host,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Sec-WebSocket-Protocol': '',
      }
    });

    let msgCount = 0;
    const timeout = setTimeout(() => {
      console.log('timeout after 30s, received', msgCount, 'messages');
      ws.close();
      resolve();
    }, 30000);

    ws.on('open', () => {
      console.log('ws open');
      // Send the query
      const msg = {
        type: '@kofile/FETCH_DOCUMENTS/v4',
        payload: {
          query: {
            limit: '3', offset: '0',
            department: 'RP',
            recordedDateRange: '2025-03-01,2025-03-31',
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
      console.log('sending:', JSON.stringify(msg).substring(0, 200));
      ws.send(JSON.stringify(msg));
    });

    ws.on('message', (data, isBinary) => {
      msgCount++;
      const str = isBinary ? '[binary]' : data.toString();
      console.log('MSG #' + msgCount + ' (len=' + str.length + '):', str.substring(0, 500));
      try {
        const parsed = JSON.parse(str);
        if (parsed.type?.includes('FULFILLED') || parsed.type?.includes('REJECTED') || msgCount > 5) {
          clearTimeout(timeout);
          ws.close();
          resolve();
        }
      } catch {}
    });

    ws.on('ping', () => { console.log('ping received'); ws.pong(); });
    ws.on('pong', () => console.log('pong'));
    ws.on('error', err => { clearTimeout(timeout); console.error('ws error:', err.message, err.code); resolve(); });
    ws.on('close', (code, reason) => console.log('ws close:', code, reason?.toString()));
    ws.on('unexpected-response', (req, res) => {
      console.log('unexpected HTTP response:', res.statusCode, res.statusMessage);
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => { console.log('body:', body.substring(0, 500)); clearTimeout(timeout); resolve(); });
    });
  });
}

probe('cuyahoga.oh.publicsearch.us').catch(e => console.error(e.message));
