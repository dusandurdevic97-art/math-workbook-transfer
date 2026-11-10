const MINUTE = 60_000;
const TTL = 15 * MINUTE;
const CLAIM_TTL = 90_000;
const MAX_SIZE = 64 * 1024 * 1024;
const MAX_STORED = 128 * 1024 * 1024;
const CHUNK_SIZE = 512 * 1024;

function response(status, body = null, headers = {}) {
  return new Response(body, {status, headers: {'Cache-Control': 'no-store', ...headers}});
}

export default {
  fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (request.method === 'GET' && path === '/health') {
      return response(200, 'Math Workbook transfer relay');
    }
    if (request.method !== 'POST' || !/^\/v1\/(send|claim|complete)\/[A-Za-z0-9]{4,32}$/.test(path)) {
      return response(404);
    }
    return env.RELAY.get(env.RELAY.idFromName('workbook')).fetch(request);
  }
};

export class TransferRelay {
  constructor(ctx) {
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS transfers (
      code TEXT PRIMARY KEY, size INTEGER NOT NULL, expires INTEGER NOT NULL,
      receipt TEXT, lease INTEGER NOT NULL DEFAULT 0
    )`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS chunks (
      code TEXT NOT NULL,
      position INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY (code, position)
    )`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS attempts (
      ip TEXT NOT NULL, bucket INTEGER NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (ip,bucket)
    )`);
  }

  prune(now) {
    this.ctx.storage.transactionSync(() => {
      this.sql.exec('DELETE FROM chunks WHERE code IN (SELECT code FROM transfers WHERE expires <= ?)',now);
      this.sql.exec('DELETE FROM transfers WHERE expires <= ?',now);
    });
    this.sql.exec('DELETE FROM attempts WHERE bucket < ?',Math.floor(now/MINUTE)-1);
  }

  async schedule() {
    const {deadline} = this.sql.exec('SELECT min(expires) AS deadline FROM transfers').one();
    const {bucket} = this.sql.exec('SELECT min(bucket) AS bucket FROM attempts').one();
    const next = Math.min(deadline ?? Infinity,bucket === null ? Infinity : (bucket+2)*MINUTE);
    if (Number.isFinite(next)) await this.ctx.storage.setAlarm(next);
    else await this.ctx.storage.deleteAlarm();
  }

  async alarm() {
    await this.ctx.blockConcurrencyWhile(async () => {
      this.prune(Date.now());
      await this.schedule();
    });
  }

  async fetch(request) {
    const [, , action, suppliedCode] = new URL(request.url).pathname.split('/');
    const code = suppliedCode.toLowerCase();
    let bytes;
    if (action === 'send') {
      const size = Number(request.headers.get('Content-Length'));
      if (!Number.isSafeInteger(size) || size < 1 || size > MAX_SIZE) return response(413);
      const reader = request.body?.getReader();
      if (!reader) return response(400);
      if (this.uploading) {
        await reader.cancel();
        return response(429);
      }
      this.uploading = true;
      try {
        bytes = new Uint8Array(size);
        let offset = 0;
        while (true) {
          const {done,value} = await reader.read();
          if (done) break;
          if (value.length > size-offset) {
            await reader.cancel();
            return response(413);
          }
          bytes.set(value,offset);
          offset += value.length;
        }
        if (offset !== size) return response(400);
        return await this.process(action,code,request,bytes);
      } finally {
        this.uploading = false;
      }
    }
    return this.process(action,code,request);
  }

  process(action,code,request,bytes) {
    return this.ctx.blockConcurrencyWhile(async () => {
      const now = Date.now();
      this.prune(now);
      let result;
      if (action === 'send') result = this.send(code,bytes,now);
      else if (action === 'claim') result = this.claim(code,request,now);
      else result = this.complete(code,request,now);
      await this.schedule();
      return result;
    });
  }

  send(code,bytes,now) {
    if (this.sql.exec('SELECT 1 FROM transfers WHERE code = ?',code).toArray().length) return response(409);
    const {total,count} = this.sql.exec('SELECT sum(size) AS total,count(*) AS count FROM transfers').one();
    if (count >= 32 || (total ?? 0)+bytes.length > MAX_STORED) return response(429);
    this.ctx.storage.transactionSync(() => {
      this.sql.exec('INSERT INTO transfers(code,size,expires) VALUES(?,?,?)',code,bytes.length,now+TTL);
      for (let i = 0; i < bytes.length; i += CHUNK_SIZE) {
        this.sql.exec('INSERT INTO chunks VALUES(?,?,?)',code,i,bytes.slice(i,i+CHUNK_SIZE).buffer);
      }
    });
    return response(201);
  }

  claim(code,request,now) {
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    const bucket = Math.floor(now/MINUTE);
    const {count} = this.sql.exec(
      'INSERT INTO attempts VALUES(?,?,1) ON CONFLICT(ip,bucket) DO UPDATE SET count=count+1 RETURNING count',
      ip,bucket).one();
    if (count > 15) return response(429);
    const [transfer] = this.sql.exec('SELECT size,lease FROM transfers WHERE code=?',code).toArray();
    if (!transfer) return response(404);
    if (transfer.lease > now) return response(409);
    const receipt = crypto.randomUUID();
    this.sql.exec('UPDATE transfers SET receipt=?,lease=? WHERE code=?',receipt,now+CLAIM_TTL,code);
    let position = 0;
    const body = new ReadableStream({
      pull: controller => {
        if (position >= transfer.size) {
          controller.close();
          return;
        }
        const [chunk] = this.sql.exec('SELECT payload FROM chunks JOIN transfers USING(code) WHERE code=? AND position=? AND receipt=?',code,position,receipt).toArray();
        if (!chunk) {
          controller.error(new Error('Transfer expired'));
          return;
        }
        controller.enqueue(new Uint8Array(chunk.payload));
        position += chunk.payload.byteLength;
      }
    });
    return response(200,body,{'Content-Type':'application/json','X-Transfer-Receipt':receipt});
  }

  complete(code,request,now) {
    const receipt = request.headers.get('X-Transfer-Receipt');
    const deleted = this.ctx.storage.transactionSync(() => {
      const rows = this.sql.exec(
        'SELECT code FROM transfers WHERE code=? AND receipt=? AND lease>?',code,receipt,now).toArray();
      if (rows.length) {
        this.sql.exec('DELETE FROM chunks WHERE code=?',code);
        this.sql.exec('DELETE FROM transfers WHERE code=?',code);
      }
      return rows;
    });
    return response(deleted.length ? 204 : 404);
  }
}
