// Copyright 2025 The Oikome Authors
// SPDX-License-Identifier: Apache-2.0
//
// Serve the browser-node test page for the Playwright tests: static files
// only, with the MIME types the browser node needs (ES modules, workers,
// WebAssembly). Each URL prefix maps to one directory of the repository:
//
//   /pkg/           packages/node/pkg (wasm-bindgen output of the build)
//   /node_modules/  node_modules (duckdb-wasm's worker and wasm bundles)
//   /               e2e/harness (index.html and the bundled dist/app.js)

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize, sep } from "node:path";
import { fileURLToPath } from "node:url";

const REPO = fileURLToPath(new URL("..", import.meta.url));
const PORT = Number(process.env.PORT ?? 4173);
const MOUNTS = [
  ["/pkg/", join(REPO, "packages", "node", "pkg")],
  ["/node_modules/", join(REPO, "node_modules")],
  ["/", join(REPO, "e2e", "harness")],
];
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".mjs": "text/javascript",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

createServer(async (req, res) => {
  let path;
  try {
    path = decodeURIComponent(new URL(req.url, "http://localhost").pathname);
  } catch {
    res.writeHead(400).end();
    return;
  }
  const [prefix, root] = MOUNTS.find(([p]) => path.startsWith(p));
  const file = normalize(join(root, path.slice(prefix.length) || "index.html"));
  // Never serve anything outside the mounted directory.
  if (!file.startsWith(root + sep)) {
    res.writeHead(403).end();
    return;
  }
  try {
    const body = await readFile(file);
    res.writeHead(200, { "content-type": TYPES[extname(file)] ?? "application/octet-stream" });
    res.end(body);
  } catch {
    res.writeHead(404).end();
  }
}).listen(PORT, "127.0.0.1", () => console.log(`serving the test page on http://127.0.0.1:${PORT}`));
