'use strict';

const mqtt = require('mqtt');
const { Client, CopySource } = require('minio');
const { v4: uuidv4 } = require('uuid');
const http = require('http');

const LOG_LEVEL = process.env.CONSUMER_LOG_LEVEL || 'info';
function log(level, msg, obj) {
  const levels = { error: 0, warn: 1, info: 2, debug: 3 };
  if ((levels[level] ?? 2) <= (levels[LOG_LEVEL] ?? 2)) {
    const base = { ts: new Date().toISOString(), level, msg };
    // Avoid noisy huge payloads
    console.log(JSON.stringify(obj ? { ...base, ...obj } : base));
  }
}

// ---------- MinIO Client ----------
const minioClient = new Client({
  endPoint: process.env.MINIO_END_POINT || 'minio',
  port: Number(process.env.MINIO_PORT || 9000),
  useSSL: String(process.env.MINIO_USE_SSL || 'false') === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
  region: process.env.MINIO_REGION || 'us-east-1',
});
const MINIO_BUCKET = process.env.MINIO_BUCKET;

function pStatObject(bucket, object) {
  return new Promise((resolve, reject) => {
    minioClient.statObject(bucket, object, (err, stat) => {
      if (err) return reject(err);
      resolve(stat);
    });
  });
}
function pPutObject(bucket, object, data) {
  return new Promise((resolve, reject) => {
    minioClient.putObject(bucket, object, data, (err, etag) => {
      if (err) return reject(err);
      resolve(etag);
    });
  });
}
function pComposeObject(bucket, object, sources) {
  return new Promise((resolve, reject) => {
    minioClient.composeObject(bucket, object, sources, (err, etag) => {
      if (err) return reject(err);
      resolve(etag);
    });
  });
}
function pCopyObject(bucket, object, source) {
  return new Promise((resolve, reject) => {
    minioClient.copyObject(bucket, object, source, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
}
function pRemoveObject(bucket, object) {
  return new Promise((resolve, reject) => {
    minioClient.removeObject(bucket, object, err => {
      if (err) return reject(err);
      resolve();
    });
  });
}

// ---------- Batching State ----------
const BATCH_MAX = Number(process.env.CONSUMER_BATCH_MAX_MESSAGES || 200);
const BATCH_MAX_INTERVAL_MS = Number(process.env.CONSUMER_BATCH_MAX_INTERVAL_MS || 5000);

// per key: `${truckId}/${yyyy-mm-dd}` => { lines: string[], timer: NodeJS.Timeout | null, chain: Promise<void> }
const buffers = new Map();

function dateFromIso(iso) {
  // returns YYYY-MM-DD in UTC
  try {
    return new Date(iso).toISOString().slice(0, 10);
  } catch (_) {
    return null;
  }
}

function keyFor(truckId, date) {
  return `${truckId}/${date}`;
}

function ensureState(key) {
  if (!buffers.has(key)) {
    buffers.set(key, { lines: [], timer: null, chain: Promise.resolve() });
  }
  return buffers.get(key);
}

function validatePayload(obj) {
  if (!obj || typeof obj !== 'object') return 'payload not an object';
  const { truck_id, timestamp, gps, metrics } = obj;
  if (!truck_id || typeof truck_id !== 'string') return 'truck_id missing/string';
  if (!timestamp || typeof timestamp !== 'string') return 'timestamp missing/string';
  const date = dateFromIso(timestamp);
  if (!date) return 'invalid timestamp';
  if (!gps || typeof gps !== 'object') return 'gps missing/object';
  if (typeof gps.lat !== 'number' || typeof gps.lon !== 'number') return 'gps.lat/lon must be number';
  if (!metrics || typeof metrics !== 'object') return 'metrics missing/object';
  const keys = ['co2_ppm', 'no2_ppm', 'so2_ppm', 'pm25_ug_m3'];
  for (const k of keys) {
    if (typeof metrics[k] !== 'number') return `metrics.${k} must be number`;
  }
  return null;
}

async function flushKey(truckId, date) {
  const key = keyFor(truckId, date);
  const state = ensureState(key);
  // Serialize flushes per key
  state.chain = state.chain.then(async () => {
    if (state.timer) {
      clearTimeout(state.timer);
      state.timer = null;
    }
    const lines = state.lines;
    if (!lines.length) return;
    state.lines = [];

    const partName = `${truckId}/${date}/parts/${Date.now()}-${uuidv4()}.jsonl`;
    const dailyName = `${truckId}/${date}/data.jsonl`;
    const body = Buffer.from(lines.join('\n') + '\n');

    try {
      await pPutObject(MINIO_BUCKET, partName, body);
      let dailyExists = true;
      try {
        await pStatObject(MINIO_BUCKET, dailyName);
      } catch (e) {
        dailyExists = false;
      }

      if (dailyExists) {
        const tmp = `${truckId}/${date}/.compose-${Date.now()}-${uuidv4()}`;
        const sources = [new CopySource(MINIO_BUCKET, dailyName), new CopySource(MINIO_BUCKET, partName)];
        await pComposeObject(MINIO_BUCKET, tmp, sources);
        await pCopyObject(MINIO_BUCKET, dailyName, `/${MINIO_BUCKET}/${tmp}`);
        await pRemoveObject(MINIO_BUCKET, tmp);
        await pRemoveObject(MINIO_BUCKET, partName);
      } else {
        await pCopyObject(MINIO_BUCKET, dailyName, `/${MINIO_BUCKET}/${partName}`);
        await pRemoveObject(MINIO_BUCKET, partName);
      }
      log('info', 'flushed', { key, lines: lines.length });
    } catch (err) {
      // Put lines back on failure to retry later
      state.lines.unshift(...lines);
      log('error', 'flush failed', { key, error: String(err) });
    }
  });

  try {
    await state.chain;
  } catch (e) {
    // Already logged, continue
  }
}

function scheduleFlush(truckId, date) {
  const key = keyFor(truckId, date);
  const state = ensureState(key);
  if (state.timer) return; // already scheduled
  state.timer = setTimeout(() => {
    state.timer = null;
    flushKey(truckId, date);
  }, BATCH_MAX_INTERVAL_MS);
}

// ---------- MQTT Subscription ----------
const brokerUrl = process.env.MQTT_BROKER_URL || 'mqtt://mosquitto:1883';
const mqttOptions = {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  reconnectPeriod: 2000,
  clean: true,
};

const client = mqtt.connect(brokerUrl, mqttOptions);

client.on('connect', () => {
  log('info', 'mqtt connected', { brokerUrl });
  client.subscribe('trucks/+/metrics', { qos: 1 }, (err) => {
    if (err) log('error', 'mqtt subscribe failed', { error: String(err) });
    else log('info', 'subscribed', { topic: 'trucks/+/metrics' });
  });
});

client.on('reconnect', () => log('info', 'mqtt reconnecting'));
client.on('error', (err) => log('error', 'mqtt error', { error: String(err) }));

client.on('message', async (topic, payload) => {
  let obj;
  try {
    obj = JSON.parse(payload.toString('utf8'));
  } catch (e) {
    return log('warn', 'invalid json', { topic });
  }

  const err = validatePayload(obj);
  if (err) return log('warn', 'invalid payload', { error: err });

  const truckId = obj.truck_id;
  const date = dateFromIso(obj.timestamp);
  const line = JSON.stringify(obj);

  const state = ensureState(keyFor(truckId, date));
  state.lines.push(line);
  if (state.lines.length >= BATCH_MAX) {
    flushKey(truckId, date);
  } else {
    scheduleFlush(truckId, date);
  }
});

// ---------- Health endpoint ----------
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    const counts = {};
    for (const [k, v] of buffers.entries()) counts[k] = v.lines.length;
    res.end(JSON.stringify({ status: 'ok', buffers: counts }));
  } else {
    res.writeHead(404); res.end();
  }
});
server.listen(3000, () => log('info', 'health server listening', { port: 3000 }));

// ---------- Graceful shutdown ----------
async function shutdown() {
  log('info', 'shutdown start');
  try {
    // Clear timers first to avoid rescheduling
    for (const [, state] of buffers.entries()) {
      if (state.timer) clearTimeout(state.timer);
      state.timer = null;
    }
    // Flush all keys sequentially
    for (const key of Array.from(buffers.keys())) {
      const [truckId, date] = key.split('/');
      await flushKey(truckId, date);
    }
  } catch (e) {
    log('error', 'shutdown flush error', { error: String(e) });
  }
  try { client.end(true); } catch (_) {}
  try { server.close(); } catch (_) {}
  log('info', 'shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);


