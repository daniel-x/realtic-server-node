// The server on its own: presence, message delivery, client files and forwarded logs.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { get } from "node:http";
import WebSocket from "ws";
import { createSignalingServer, formatTimeUs } from "../server.js";

let dir;
let server;

before(async () => {
  dir = mkdtempSync(join(tmpdir(), "realtic-server-node-"));
  mkdirSync(join(dir, "client", "examples"), { recursive: true });
  writeFileSync(join(dir, "client", "examples", "index.html"), "<p>demo</p>");
  writeFileSync(join(dir, "client", ".secret"), "x");
  server = await createSignalingServer({ clientDir: join(dir, "client"), logDir: join(dir, "logs") });
});

after(async () => {
  await server.close();
  rmSync(dir, { recursive: true, force: true });
});

function join_(id) {
  const ws = new WebSocket(`${server.url}?group=g&id=${id}`);
  const frames = [];
  const waiters = [];
  ws.on("message", (data) => {
    frames.push(JSON.parse(String(data)));
    for (const w of waiters.splice(0)) w();
  });
  const next = async (pred) => {
    for (;;) {
      const hit = frames.find(pred);
      if (hit) return hit;
      await new Promise((resolve) => waiters.push(resolve));
    }
  };
  return new Promise((resolve) => ws.on("open", () => resolve({ ws, next })));
}

test("peers see each other join and exchange messages", async () => {
  const a = await join_("a");
  const b = await join_("b");
  await a.next((f) => f.presence === "join" && f.peer === "b");
  await b.next((f) => f.presence === "join" && f.peer === "a");
  a.ws.send(JSON.stringify({ to: "b", msg: { t: "hello" } }));
  assert.deepEqual(await b.next((f) => f.msg), { from: "a", msg: { t: "hello" } });
  b.ws.close();
  await a.next((f) => f.presence === "leave" && f.peer === "b");
  a.ws.close();
});

test("serves the client files, but no dot files and nothing outside", async () => {
  const base = `http://localhost:${server.port}`;
  const page = await fetch(`${base}/examples/`);
  assert.equal(page.status, 200);
  assert.equal(page.headers.get("content-type"), "text/html");
  assert.equal(await page.text(), "<p>demo</p>");
  assert.equal((await fetch(`${base}/.secret`)).status, 403);
  assert.equal((await fetch(`${base}/missing.js`)).status, 404);
  const raw = await new Promise((resolve) => get({ port: server.port, path: "/../client/examples/" }, resolve));
  assert.notEqual(raw.statusCode, 200);
});

test("appends forwarded log entries to one file per group", async () => {
  const timeUs = Date.UTC(2026, 0, 2, 3, 4, 5) * 1000 + 123456;
  const res = await fetch(`http://localhost:${server.port}/log?group=g&id=a`, {
    method: "POST",
    body: JSON.stringify({ entries: [{ seq: 7, timeUs, level: "warn", area: "direct", peer: "b", msg: "slow" }] }),
  });
  assert.equal(res.status, 204);
  const line = readFileSync(join(dir, "logs", "g.log"), "utf8");
  assert.match(line, /^\d{4}-\d\d-\d\d_\d\d-\d\d-\d\d\.\d{6} 2026-01-02_03-04-05\.123456 a #7 WARN  \[direct\] peer=b slow\n$/);
  assert.equal(formatTimeUs(timeUs), "2026-01-02_03-04-05.123456");
});
