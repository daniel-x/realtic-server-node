# realtic-server-node

WebSocket signaling server for [realtic](https://github.com/daniel-x/realtic): groups, presence, a
`sendBeacon` endpoint, an endpoint for the logs that realtic's `forwardLog` sends (one file per
group), and the realtic client files (library and demos) as static files. The wire format is in
realtic's [docs/protocol.md](https://github.com/daniel-x/realtic/blob/main/docs/protocol.md).

```js
new RalPeerGroup({ signaling: { url: "wss://example.com/signal", group: "demo" } });
```

## Usage

```sh
npm install realtic-server-node realtic
npx realtic-server-node 8080 [--client-dir <dir>] [--log-dir <dir>]
# http://localhost:8080/examples/call/?group=test&server=node   (open in several tabs or devices)
# http://localhost:8080/examples/chat/?group=test&server=node
```

- `--client-dir` (or `REALTIC_CLIENT_DIR`): the client files. By default the installed `realtic`
  package, else a realtic checkout next to this directory. Paths with a dot segment or
  `node_modules` are not served.
- `--log-dir`: accept forwarded logs at `POST /log?group=&id=` and append them to
  `<dir>/<group>.log`. Without it, `/log` answers 404.
- `POST /beacon?group=&id=` with body `{ to, msg }` delivers one message, for
  `navigator.sendBeacon` on `pagehide`.

Delivered messages are not stored, so nothing is redelivered. Put the server behind a TLS reverse
proxy for `wss://`.

As a module: `createSignalingServer({ port, host, clientDir, logDir, log })` returns
`{ port, url, close() }` and the test controls that the realtic integration tests use.

## Development

```sh
npm install
npm test
```

The realtic integration tests run against this server (`npm run test:integration` in realtic,
with this repository next to it and its dependencies installed).

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.

## Authorship

This project, including its purpose and overall architecture, was conceived and designed by its
human authors. The implementation was generated in full with AI assistance (Claude Code), under
the human authors' direction.
