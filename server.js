const http = require('http');
const { Readable } = require('stream');

const HEV1 = new Uint8Array([0x68, 0x65, 0x76, 0x31]);
const HVC1 = new Uint8Array([0x68, 0x76, 0x63, 0x31]);
const SCAN_WINDOW = 512 * 1024;
const PORT = process.env.PORT || 8000;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': '*',
  'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Accept-Ranges',
};

function patchStream() {
  let buf = new Uint8Array(0);
  let done = false;

  return new TransformStream({
    transform(chunk, controller) {
      if (done) { controller.enqueue(chunk); return; }

      const merged = new Uint8Array(buf.length + chunk.length);
      merged.set(buf);
      merged.set(chunk, buf.length);
      buf = merged;

      outer: for (let i = Math.max(0, buf.length - chunk.length - 3); i <= buf.length - 4; i++) {
        for (let j = 0; j < 4; j++) {
          if (buf[i + j] !== HEV1[j]) continue outer;
        }
        buf.set(HVC1, i);
        done = true;
        break;
      }

      if (done || buf.length >= SCAN_WINDOW) {
        controller.enqueue(buf);
        buf = new Uint8Array(0);
        done = true;
      }
    },
    flush(controller) {
      if (buf.length) controller.enqueue(buf);
    },
  });
}

function sendText(res, status, text) {
  res.writeHead(status, { ...CORS, 'Content-Type': 'text/plain' });
  res.end(text);
}

http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    return res.end();
  }

  const url = new URL(req.url, 'http://localhost');
  const target = url.searchParams.get('url');
  const cookie = url.searchParams.get('cookie') ?? req.headers['x-stream-cookie'] ?? '';

  if (!target) return sendText(res, 400, 'Missing "url" param');

  const headers = {
    'User-Agent': 'ExoPlayerLib/2.19.1',
    'Accept': '*/*',
  };
  const range = req.headers['range'];
  if (range) headers['Range'] = range;
  if (cookie) headers['Cookie'] = cookie;

  const controller = new AbortController();
  res.on('close', () => controller.abort());

  let upstream;
  try {
    upstream = await fetch(target, { headers, redirect: 'follow', signal: controller.signal });
  } catch (err) {
    return sendText(res, 502, `Proxy error: ${err}`);
  }

  if (!upstream.ok) {
    const body = (await upstream.text()).slice(0, 500);
    return sendText(res, upstream.status, `Upstream ${upstream.status}\n${body}`);
  }

  const out = {};
  upstream.headers.forEach((v, k) => { out[k] = v; });
  // fetch already decompressed the body, so these would be wrong
  if (out['content-encoding']) delete out['content-length'];
  delete out['content-encoding'];
  delete out['transfer-encoding'];
  delete out['connection'];
  Object.assign(out, CORS);

  res.writeHead(upstream.status, out);

  if (!upstream.body) return res.end();

  const rangeStart = range ? parseInt(range.match(/bytes=(\d+)-/)?.[1] ?? '0', 10) : 0;
  const shouldPatch = rangeStart < SCAN_WINDOW;
  const stream = shouldPatch ? upstream.body.pipeThrough(patchStream()) : upstream.body;

  Readable.fromWeb(stream)
    .on('error', () => res.destroy())
    .pipe(res);
}).listen(PORT, () => console.log(`Proxy listening on port ${PORT}`));