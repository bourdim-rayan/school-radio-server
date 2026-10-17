/* ===================================================================
   server.js — خادم الموقع + الإذاعة المدرسية المباشرة (بدون اعتماديات خارجية)
   التشغيل:   node server.js          (المنفذ الافتراضي 3000)
   المتغيرات: PORT=3000  RADIO_PIN=رمز-المذيع
   - يقدّم ملفات الموقع الثابتة (المجلد الأب).
   - يستقبل صوت المذيع على WebSocket ‏(/radio) ويرحّله فورًا إلى كل المستمعين.
   - المذيع يقدّم «رمز الإذاعة» (RADIO_PIN) الذي يعرفه المدير فقط ويسلّمه للمكلَّفين.
   =================================================================== */
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = Number(process.env.PORT) || 3000;
const ROOT = path.resolve(__dirname, "..");
const HAS_SITE = fs.existsSync(path.join(ROOT, "index.html"));   // false عند نشر مجلد server وحده على Render
const PIN = process.env.RADIO_PIN || String(crypto.randomInt(100000, 999999));
const MAX_FRAME = 256 * 1024;      // حد حجم الإطار الواحد
const MIME = { ".html": "text/html; charset=utf-8", ".css": "text/css; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".json": "application/json", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp", ".svg": "image/svg+xml", ".ico": "image/x-icon", ".md": "text/plain; charset=utf-8" };

/* ---------- حالة القناة ---------- */
const listeners = new Set();
let host = null;                    // اتصال المذيع الحالي
let channel = { live: false, title: "", host: "", startedAt: 0, sampleRate: 16000 };

function statusMsg() { return JSON.stringify({ type: "status", live: channel.live, title: channel.title, host: channel.host, startedAt: channel.startedAt, sampleRate: channel.sampleRate, listeners: listeners.size }); }
function broadcastStatus() { const m = statusMsg(); for (const l of listeners) l.sendText(m); if (host) host.sendText(m); }

/* ---------- WebSocket يدوي (RFC 6455) ---------- */
class Sock {
  constructor(socket) { this.s = socket; this.buf = Buffer.alloc(0); this.frag = null; this.alive = true; this.role = null; this.onmessage = null; this.onclose = null; this.limit = { t: Date.now(), n: 0 };
    socket.on("data", d => this.feed(d)); socket.on("close", () => this.close()); socket.on("error", () => this.close()); }
  feed(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1], fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; const hi = this.buf.readUInt32BE(2), lo = this.buf.readUInt32BE(6); if (hi) return this.close(); len = lo; off = 10; }
      if (!masked) return this.close();            // العميل يجب أن يقنّع الإطارات
      if (len > MAX_FRAME) return this.close();
      if (this.buf.length < off + 4 + len) return;
      const mask = this.buf.subarray(off, off + 4);
      const payload = Buffer.from(this.buf.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + 4 + len);
      if (op === 0x8) return this.close();
      if (op === 0x9) { this.send(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x1 || op === 0x2) this.frag = { op, parts: [payload] };
      else if (op === 0x0 && this.frag) this.frag.parts.push(payload);
      else continue;
      if (fin) { const full = Buffer.concat(this.frag.parts), o = this.frag.op; this.frag = null; if (this.onmessage) this.onmessage(o === 0x1 ? full.toString("utf8") : full, o === 0x2); }
    }
  }
  send(op, payload) {
    if (!this.alive) return;
    const n = payload.length; let head;
    if (n < 126) head = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(n, 6); }
    if (this.s.writableLength > 2 * 1024 * 1024) return;   // مستمع بطيء: نتخطى الإطار بدل تكديس الذاكرة
    this.s.write(Buffer.concat([head, payload]));
  }
  sendText(t) { this.send(0x1, Buffer.from(t)); }
  sendBin(b) { this.send(0x2, b); }
  close() { if (!this.alive) return; this.alive = false; try { this.s.destroy(); } catch (e) {} if (this.onclose) this.onclose(); }
}

function onConnection(ws) {
  ws.onmessage = (data, isBin) => {
    if (!isBin) {
      let m; try { m = JSON.parse(data); } catch (e) { return; }
      if (m.type === "listen") { ws.role = "listener"; listeners.add(ws); ws.sendText(statusMsg()); broadcastStatus(); return; }
      if (m.type === "host") {
        const ok = typeof m.pin === "string" && m.pin.length === PIN.length && crypto.timingSafeEqual(Buffer.from(m.pin), Buffer.from(PIN));
        if (!ok) { ws.sendText(JSON.stringify({ type: "error", error: "رمز الإذاعة غير صحيح." })); return; }
        if (host && host !== ws) { ws.sendText(JSON.stringify({ type: "error", error: "يوجد بث مباشر جارٍ بالفعل." })); return; }
        host = ws; ws.role = "host";
        channel = { live: true, title: String(m.title || "").slice(0, 120), host: String(m.host || "").slice(0, 60), startedAt: Date.now(), sampleRate: 16000 };
        ws.sendText(JSON.stringify({ type: "ok" })); broadcastStatus(); return;
      }
      if (m.type === "stop" && ws === host) { endBroadcast(); }
      return;
    }
    if (ws === host && channel.live) { for (const l of listeners) l.sendBin(data); }   // ترحيل الصوت
  };
  ws.onclose = () => { listeners.delete(ws); if (ws === host) endBroadcast(); else broadcastStatus(); };
}
function endBroadcast() {
  host = null; channel = { live: false, title: "", host: "", startedAt: 0, sampleRate: 16000 };
  broadcastStatus();
}

/* ---------- HTTP ---------- */
const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(new URL(req.url, "http://x").pathname); } catch (e) { res.writeHead(400); return res.end("Bad request"); }
  if (p === "/api/radio/status") { res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" }); return res.end(statusMsg()); }
  if (!HAS_SITE) { res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("خادم الإذاعة المدرسية يعمل ✅"); }
  if (p.endsWith("/")) p += "index.html";
  const file = path.resolve(ROOT, "." + p);
  const rel = path.relative(ROOT, file);
  if (rel.startsWith("..") || path.isAbsolute(rel) || rel.split(path.sep)[0] === "server") { res.writeHead(403); return res.end("Forbidden"); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("غير موجود"); }
    res.writeHead(200, { "Content-Type": MIME[path.extname(file).toLowerCase()] || "application/octet-stream", "X-Content-Type-Options": "nosniff", "Cache-Control": "no-cache" });
    res.end(buf);
  });
});
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (new URL(req.url, "http://x").pathname !== "/radio" || !key || String(req.headers.upgrade).toLowerCase() !== "websocket") { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  onConnection(new Sock(socket));
});
server.listen(PORT, () => {
  console.log("الموقع:   http://localhost:" + PORT + "/");
  console.log("الإذاعة:  رمز المذيع = " + PIN + (process.env.RADIO_PIN ? " (من RADIO_PIN)" : "  (عُيّن عشوائيًا؛ ثبّته بالمتغير RADIO_PIN)"));
});
