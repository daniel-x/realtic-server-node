// Minimal signaling server for realtic: WebSocket rooms, presence, a sendBeacon endpoint, and
// static files for the demos. Delivered messages are not stored, so nothing is redelivered
// (doc §2.5). Test controls (down, delay, duplicate, redeliver) exist for the test harness.
//
// Usage: node server/node/server.js [port]   then open http://localhost:<port>/examples/call/?server=node
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

const ROOT = fileURLToPath(new URL("../../", import.meta.url));
const TYPES = { ".js": "text/javascript", ".html": "text/html", ".css": "text/css", ".json": "application/json" };
const utc = () => new Date().toISOString().replace("T", "_").replace(/:/g, "-").replace("Z", "");

/**
 * Creates a signaling server.
 * @param {{port?: number, host?: string, log?: boolean, static?: boolean}} [options]
 */
export async function createSignalingServer({ port = 0, host = "127.0.0.1", log = false, static: serveStatic = true } = {}) {
  const rooms = new Map();
  const controls = { down: false, delayMs: 0, duplicate: false, redeliver: false, transform: null };
  const history = [];
  const say = (...args) => log && console.log(utc(), ...args);

  const deliver = (room, from, to, msg) => {
    if (controls.down) return;
    const out = controls.transform ? controls.transform(from, to, msg) : msg;
    if (!out) return;
    const frame = JSON.stringify({ from, msg: out });
    const send = () => {
      const target = rooms.get(room)?.get(to);
      if (target && target.readyState === 1) target.send(frame);
    };
    const times = controls.duplicate ? 2 : 1;
    for (let i = 0; i < times; i++) controls.delayMs ? setTimeout(send, controls.delayMs) : send();
    if (controls.redeliver) {
      history.push({ room, to, frame });
      if (history.length > 200) history.shift();
    }
  };

  const http = createServer(async (req, res) => {
    const url = new URL(req.url, "http://x");
    if (url.pathname.endsWith("/beacon") && req.method === "POST") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        try {
          const { to, msg } = JSON.parse(body);
          deliver(url.searchParams.get("room") ?? "default", url.searchParams.get("id"), to, msg);
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
    const room = url.searchParams.get("room") ?? "default";
    if (!id) return ws.close();
    if (!rooms.has(room)) rooms.set(room, new Map());
    const members = rooms.get(room);
    members.get(id)?.close();
    for (const [other, sock] of members) {
      if (sock.readyState === 1) sock.send(JSON.stringify({ presence: "join", peer: id }));
      ws.send(JSON.stringify({ presence: "join", peer: other }));
    }
    members.set(id, ws);
    say("join", room, id);
    if (controls.redeliver) for (const h of history) if (h.room === room && h.to === id) ws.send(h.frame);
    ws.on("message", (data) => {
      let frame;
      try {
        frame = JSON.parse(String(data));
      } catch {
        return;
      }
      if (typeof frame.to !== "string" || !frame.msg) return;
      say("msg", room, id, "->", frame.to, frame.msg.t);
      deliver(room, id, frame.to, frame.msg);
    });
    ws.on("close", () => {
      if (members.get(id) !== ws) return;
      members.delete(id);
      say("leave", room, id);
      for (const sock of members.values()) if (sock.readyState === 1) sock.send(JSON.stringify({ presence: "leave", peer: id }));
    });
  });

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
      if (down) for (const members of rooms.values()) for (const ws of members.values()) ws.terminate();
    },
    async close() {
      for (const members of rooms.values()) for (const ws of members.values()) ws.terminate();
      wss.close();
      await new Promise((resolve) => http.close(resolve));
    },
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const port = Number(process.argv[2] ?? 8080);
  const s = await createSignalingServer({ port, host: "0.0.0.0", log: true });
  console.log(`${utc()} realtic signaling server on http://localhost:${s.port}/examples/call/?server=node (timestamps are UTC)`);
}
