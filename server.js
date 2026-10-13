// Minimal signaling server for realtic: WebSocket groups, presence, a sendBeacon endpoint, a log
// endpoint for forwardLog, and static files for the demos. Delivered messages are not stored, so
// nothing is redelivered (doc §2.5). Test controls (down, delay, duplicate, redeliver) exist for
// the test harness.
//
// Usage: node server/node/server.js [port] [--log-dir <dir>]
//   then open http://localhost:<port>/examples/call/?server=node
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { appendFileSync, mkdirSync } from "node:fs";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";
import { formatTime, entryText, LEVELS } from "../../src/diag/log.js";
import { realClock } from "../../src/util/clock.js";

const ROOT = fileURLToPath(new URL("../../", import.meta.url));
const TYPES = { ".js": "text/javascript", ".html": "text/html", ".css": "text/css", ".json": "application/json" };
const NAME = /^[A-Za-z0-9_.-]{1,64}$/;
const MAX_LOG_BODY_BYTES = 262144;
const utc = () => formatTime(realClock.wallNow());

/**
 * The lines of a forwarded log request: receive time, the client's entry time, the client id and
 * the entry, all on one line. Times are UTC with microseconds.
 * @param {number} received UTC milliseconds
 * @param {string} id
 * @param {unknown} entries
 */
export function forwardedLines(received, id, entries) {
  if (!Array.isArray(entries)) return "";
  const at = formatTime(received);
  const str = (v, max = 64) => (typeof v === "string" ? v.slice(0, max) : undefined);
  let out = "";
  for (const e of entries) {
    if (!e || typeof e !== "object" || !Number.isFinite(e.time)) continue;
    const entry = {
      level: LEVELS.includes(e.level) ? e.level : "info",
      area: str(e.area) ?? "?",
      peer: str(e.peer),
      session: str(e.session),
      msg: String(e.msg ?? "").slice(0, 4096),
      data: e.data,
    };
    out += `${at} ${formatTime(e.time)} ${id} #${Number(e.seq) || 0} ${entryText(entry).replace(/[\r\n]+/g, " ")}\n`;
  }
  return out;
}

/**
 * Creates a signaling server. With `logDir`, POST /log?group=&id= appends forwarded log entries
 * to `<logDir>/<group>.log`, one file per group in arrival order.
 * @param {{port?: number, host?: string, log?: boolean, static?: boolean, logDir?: string}} [options]
 */
export async function createSignalingServer({ port = 0, host = "127.0.0.1", log = false, static: serveStatic = true, logDir = null } = {}) {
  const groups = new Map();
  const controls = { down: false, delayMs: 0, duplicate: false, redeliver: false, transform: null };
  const history = [];
  const say = (...args) => log && console.log(utc(), ...args);

  const deliver = (group, from, to, msg) => {
    if (controls.down) return;
    const out = controls.transform ? controls.transform(from, to, msg) : msg;
    if (!out) return;
    const frame = JSON.stringify({ from, msg: out });
    const send = () => {
      const target = groups.get(group)?.get(to);
      if (target && target.readyState === 1) target.send(frame);
    };
    const times = controls.duplicate ? 2 : 1;
    for (let i = 0; i < times; i++) controls.delayMs ? setTimeout(send, controls.delayMs) : send();
    if (controls.redeliver) {
      history.push({ group, to, frame });
      if (history.length > 200) history.shift();
    }
  };

  const http = createServer(async (req, res) => {
    const url = new URL(req.url, "http://x");
    if (url.pathname.endsWith("/log") && req.method === "POST") {
      const received = realClock.wallNow();
      const group = url.searchParams.get("group") ?? "default";
      const id = url.searchParams.get("id") ?? "";
      if (!logDir) return void res.writeHead(404).end();
      if (!NAME.test(group) || !NAME.test(id)) return void res.writeHead(400).end();
      const chunks = [];
      let size = 0;
      req.on("data", (c) => {
        size += c.length;
        if (size <= MAX_LOG_BODY_BYTES) chunks.push(c);
      });
      req.on("end", () => {
        if (size > MAX_LOG_BODY_BYTES) return void res.writeHead(413).end();
        try {
          const lines = forwardedLines(received, id, JSON.parse(Buffer.concat(chunks).toString("utf8")).entries);
          if (lines) appendFileSync(join(logDir, `${group}.log`), lines);
          res.writeHead(204).end();
        } catch {
          res.writeHead(400).end();
        }
      });
      return;
    }
    if (url.pathname.endsWith("/beacon") && req.method === "POST") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        try {
          const { to, msg } = JSON.parse(body);
          deliver(url.searchParams.get("group") ?? "default", url.searchParams.get("id"), to, msg);
        } catch {
          // ignore malformed beacons
        }
        res.writeHead(204).end();
      });
      return;
    }
    if (!serveStatic) return void res.writeHead(404).end();
    const path = normalize(decodeURIComponent(url.pathname));
    if (path.includes("..")) return void res.writeHead(403).end();
    try {
      const file = path.endsWith("/") ? join(ROOT, path, "index.html") : join(ROOT, path);
      const body = await readFile(file);
      res.writeHead(200, { "content-type": TYPES[extname(file)] ?? "application/octet-stream", "cache-control": "no-store" });
      res.end(body);
    } catch {
      res.writeHead(404).end();
    }
  });

  const wss = new WebSocketServer({ noServer: true });
  http.on("upgrade", (req, socket, head) => {
    if (controls.down) return socket.destroy();
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  });
  wss.on("connection", (ws, req) => {
    const url = new URL(req.url, "http://x");
    const id = url.searchParams.get("id");
    const group = url.searchParams.get("group") ?? "default";
    if (!id) return ws.close();
    if (!groups.has(group)) groups.set(group, new Map());
    const members = groups.get(group);
    members.get(id)?.close();
    for (const [other, sock] of members) {
      if (sock.readyState === 1) sock.send(JSON.stringify({ presence: "join", peer: id }));
      ws.send(JSON.stringify({ presence: "join", peer: other }));
    }
    members.set(id, ws);
    say("join", group, id);
    if (controls.redeliver) for (const h of history) if (h.group === group && h.to === id) ws.send(h.frame);
    ws.on("message", (data) => {
      let frame;
      try {
        frame = JSON.parse(String(data));
      } catch {
        return;
      }
      if (typeof frame.to !== "string" || !frame.msg) return;
      say("msg", group, id, "->", frame.to, frame.msg.t);
      deliver(group, id, frame.to, frame.msg);
    });
    ws.on("close", () => {
      if (members.get(id) !== ws) return;
      members.delete(id);
      say("leave", group, id);
      for (const sock of members.values()) if (sock.readyState === 1) sock.send(JSON.stringify({ presence: "leave", peer: id }));
    });
  });

  if (logDir) mkdirSync(logDir, { recursive: true });
  await new Promise((resolve) => http.listen(port, host, resolve));
  const actualPort = http.address().port;
  return {
    port: actualPort,
    url: `ws://${host === "0.0.0.0" ? "localhost" : host}:${actualPort}/signal`,
    get signaling() {
      return { type: "websocket", url: this.url };
    },
    controls,
    /** Takes the server down (closes all sockets, refuses new ones) or brings it back. */
    setDown(down) {
      controls.down = down;
      if (down) for (const members of groups.values()) for (const ws of members.values()) ws.terminate();
    },
    async close() {
      for (const members of groups.values()) for (const ws of members.values()) ws.terminate();
      wss.close();
      await new Promise((resolve) => http.close(resolve));
    },
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const logAt = args.indexOf("--log-dir");
  const logDir = logAt >= 0 ? args.splice(logAt, 2)[1] : null;
  const port = Number(args[0] ?? 8080);
  const s = await createSignalingServer({ port, host: "0.0.0.0", log: true, logDir });
  if (logDir) console.log(`${utc()} forwarded logs go to ${logDir}/<group>.log`);
  console.log(`${utc()} realtic signaling server on http://localhost:${s.port}/examples/call/?server=node (timestamps are UTC)`);
}
