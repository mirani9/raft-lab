// client/writer.js
const fetch = require('node-fetch');
const GATEWAY_URL = process.env.GATEWAY_URL || 'http://gateway:5000/write';
const INTERVAL_MS = Number(process.env.INTERVAL_MS) || 1000; // 1 sec
let counter = 0;
async function send() {
  const payload = {
    key: `key-${counter}`,
    value: `value-${Date.now()}`,
    requestId: `req-${counter}-${Date.now()}`
  };
  try {
    const res = await fetch(GATEWAY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await res.json().catch(() => ({}));
    console.log(`[Writer] ${payload.key} -> ${res.status}`, data);
  } catch (e) {
    console.error('[Writer] error', e.message);
  }
  counter++;
}
setInterval(send, INTERVAL_MS);
console.log(`[Writer] started, posting to ${GATEWAY_URL} every ${INTERVAL_MS}ms`);
