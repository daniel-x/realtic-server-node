#!/usr/bin/env node
// Signaling server for realtic: WebSocket groups, presence, a sendBeacon endpoint, a log endpoint
// for forwardLog, and the client files (library and demos) as static files. Delivered messages are
// not stored, so nothing is redelivered. Test controls (down, delay, duplicate, redeliver) exist
// for the integration tests of realtic and are set only through the returned object.
//
// Usage: realtic-server-node [port] [--client-dir <dir>] [--log-dir <dir>]
//   then open http://localhost:<port>/examples/call/?server=node
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { appendFileSync, existsSync, mkdirSync, realpathSync } from "node:fs";
import { dirname, extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

const TYPES = { ".js": "text/javascript", ".mjs": "text/javascript", ".html": "text/html", ".css": "text/css", ".json": "application/json", ".svg": "image/svg+xml", ".png": "image/png" };
const NAME = /^[A-Za-z0-9_.-]{1,64}$/;
const LEVELS = ["debug", "info", "warn", "error"];
const MAX_LOG_BODY_BYTES = 262144;

/** UTC microseconds since the epoch, as an integer. */
const nowUs = () => Math.round((performance.timeOrigin + performance.now()) * 1000);

/** Formats integer UTC microseconds as `yyyy-MM-dd_HH-mm-ss.SSSSSS`, like the realtic log. */
export function formatTimeUs(us) {
  const d = new Date(Math.floor(us / 1000));
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return (
    `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}_` +
    `${p(d.getUTCHours())}-${p(d.getUTCMinutes())}-${p(d.getUTCSeconds())}.${p(us % 1000000, 6)}`
  );
}

const utc = () => formatTimeUs(nowUs());

/** One log entry as text, in the format of the realtic log. */
function entryText(e) {
  const ids = [e.peer && `peer=${e.peer}`, e.session && `s=${e.session}`].filter(Boolean).join(" ");
  const data = e.data === undefined ? "" : ` ${JSON.stringify(e.data)}`;
  return `${e.level.toUpperCase().padEnd(5)} [${e.area}]${ids ? " " + ids : ""} ${e.msg}${data}`;
}

/**
 * The directory of the client files: the installed `realtic` package, else a `realtic` checkout
 * next to this server, else null (no static files).
 */
export function defaultClientDir() {
  try {
    return dirname(fileURLToPath(import.meta.resolve("realtic/package.json")));
  } catch {
    const sibling = fileURLToPath(new URL("../realtic/", import.meta.url));
    return existsSync(join(sibling, "src", "index.js")) ? sibling : null;
  }
}

/**
 * The lines of a forwarded log request: receive time, the client's entry time, the client id and
 * the entry, all on one line. Times are UTC with microseconds.
 * @param {number} receivedUs UTC microseconds
 * @param {string} id
 * @param {unknown} entries
 */
export function forwardedLines(receivedUs, id, entries) {
  if (!Array.isArray(entries)) return "";
  const at = formatTimeUs(receivedUs);
  const str = (v, max = 64) => (typeof v === "string" ? v.slice(0, max) : undefined);
  let out = "";
  for (const e of entries) {
    if (!e || typeof e !== "object" || !Number.isSafeInteger(e.timeUs)) continue;
    const entry = {
      level: LEVELS.includes(e.level) ? e.level : "info",
      area: str(e.area) ?? "?",
      peer: str(e.peer),
      session: str(e.session),
      msg: String(e.msg ?? "").slice(0, 4096),
      data: e.data,
    };
    out += `${at} ${formatTimeUs(e.timeUs)} ${id} #${Number(e.seq) || 0} ${entryText(entry).replace(/[\r\n]+/g, " ")}\n`;
  }
  return out;
}

/**
 * Creates a signaling server. GET requests other than the WebSocket upgrade get the files of
 * `clientDir` (default: `defaultClientDir()`, `false` serves none). With `logDir`, POST
 * /log?group=&id= appends forwarded log entries to `<logDir>/<group>.log`, one file per group in
 * arrival order.
 * @param {{port?: number, host?: string, log?: boolean, clientDir?: string | false | null, logDir?: string}} [options]
 */
export async function createSignalingServer({ port = 0, host = "127.0.0.1", log = false, clientDir = defaultClientDir(), logDir = null } = {}) {
  const root = clientDir ? realpathSync(clientDir) : null;
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
      const receivedUs = nowUs();
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
          const lines = forwardedLines(receivedUs, id, JSON.parse(Buffer.concat(chunks).toString("utf8")).entries);
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
    if (!root) return void res.writeHead(404).end();
    const path = normalize(decodeURIComponent(url.pathname));
    if (path.includes("..") || /\/\.|node_modules/.test(path)) return void res.writeHead(403).end();
    try {
      const file = path.endsWith("/") ? join(root, path, "index.html") : join(root, path);
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
    clientDir: root,
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

/** Runs the server from the command line; see the usage at the top of this file. */
async function main(args) {
  const option = (name) => {
    const at = args.indexOf(name);
    return at >= 0 ? args.splice(at, 2)[1] : undefined;
  };
  const logDir = option("--log-dir") ?? null;
  const clientDir = option("--client-dir") ?? process.env.REALTIC_CLIENT_DIR ?? defaultClientDir();
  const port = Number(args[0] ?? 8080);
  const s = await createSignalingServer({ port, host: "0.0.0.0", log: true, clientDir, logDir });
  if (logDir) console.log(`${utc()} forwarded logs go to ${logDir}/<group>.log`);
  if (s.clientDir) console.log(`${utc()} client files from ${s.clientDir}`);
  else console.log(`${utc()} no client files: install realtic or pass --client-dir`);
  console.log(`${utc()} realtic signaling server on http://localhost:${s.port}/examples/call/?server=node (timestamps are UTC)`);
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) await main(process.argv.slice(2));
