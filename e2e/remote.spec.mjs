// Copyright 2025 The Oikome Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The browser node and a Full Node over WebRTC (3.2 exit criteria): the
// test runs `oikome serve --webrtc-listen`, the Full Node grants the
// browser's DID and issues it a UCAN, and the browser joins a duckdb-wasm
// table with the node's table. A tampered record, a wrong fingerprint and
// a revoked token are each refused.
//
// OIKOME_BIN: the native binary (default <target>/debug/oikome, where
// <target> is CARGO_TARGET_DIR or the repository's target/). It loads the
// prebuilt libduckdb cargo downloaded into <target>/duckdb-download.

import { execFileSync, spawn } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";

const REPO = fileURLToPath(new URL("..", import.meta.url));
const TARGET = resolve(REPO, process.env.CARGO_TARGET_DIR ?? "target");
const BIN = process.env.OIKOME_BIN ?? join(TARGET, "debug", "oikome");
/** Directories under target/duckdb-download/<target>/<version> with libduckdb. */
function libDirs() {
  const root = join(TARGET, "duckdb-download");
  const subdirs = (dir) => (existsSync(dir) ? readdirSync(dir).map((d) => join(dir, d)) : []);
  return subdirs(root)
    .flatMap(subdirs)
    .filter((dir) => ["libduckdb.so", "libduckdb.dylib"].some((f) => existsSync(join(dir, f))));
}
const LIB_DIRS = libDirs().join(":");
const ENV = {
  ...process.env,
  LD_LIBRARY_PATH: [LIB_DIRS, process.env.LD_LIBRARY_PATH].filter(Boolean).join(":"),
  DYLD_LIBRARY_PATH: [LIB_DIRS, process.env.DYLD_LIBRARY_PATH].filter(Boolean).join(":"),
};

test.describe.configure({ mode: "serial", timeout: 120_000 });

let root;
let server;

/** Run SQL as the Full Node's owner; rows as arrays of strings. */
function nodeSql(text) {
  const out = execFileSync(BIN, ["--root", root, "sql", "--raw", text], {
    encoding: "utf8",
    env: ENV,
    stdio: ["ignore", "pipe", "pipe"],
  });
  return out
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => line.split("\t"));
}

/** The fields of an access record (`webrtc:<json>.<signature>`). */
function recordFields(record) {
  const json = record.slice("webrtc:".length).split(".")[0];
  return JSON.parse(Buffer.from(json, "base64url").toString("utf8"));
}

test.beforeAll(async () => {
  expect(existsSync(BIN), `${BIN} (cargo build -p oikome-server)`).toBe(true);
  root = mkdtempSync(join(tmpdir(), "oikome-e2e-"));
  nodeSql(
    "CREATE SPACE blog; " +
      "CREATE TABLE blog.posts (id BIGINT NOT NULL, author VARCHAR NOT NULL, title VARCHAR, draft BOOLEAN); " +
      "INSERT INTO blog.posts VALUES (1, 'alice', 'Hello', false), (2, 'bob', 'World', false), " +
      "(3, 'alice', 'Again', false), (4, 'alice', 'Unfinished', true)",
  );
  server = spawn(
    BIN,
    [
      "--root",
      root,
      "serve",
      "--listen",
      "127.0.0.1:0",
      "--p2p-listen",
      "/ip4/127.0.0.1/tcp/0",
      "--no-mdns",
      "--webrtc-listen",
      "127.0.0.1:0",
    ],
    { stdio: ["ignore", "inherit", "inherit"], env: ENV },
  );
  // The record is written once the UDP socket is bound.
  for (let i = 0; ; i++) {
    try {
      nodeSql("SHOW WEBRTC");
      break;
    } catch (e) {
      if (i === 100 || server.exitCode !== null) throw e;
      await new Promise((r) => setTimeout(r, 100));
    }
  }
});

test.afterAll(() => {
  server?.kill();
  if (root) rmSync(root, { recursive: true, force: true });
});

async function ready(page) {
  // The page's errors (a Rust panic among them) in the test output.
  page.on("console", (m) => m.type() === "error" && console.error(`[page] ${m.text()}`));
  page.on("pageerror", (e) => console.error(`[page] ${e.message}`));
  await page.waitForFunction(() => document.body.dataset.ready || document.body.dataset.error);
  const error = await page.evaluate(() => document.body.dataset.error);
  expect(error, "boot error").toBeUndefined();
}

const sql = (page, text) => page.evaluate((t) => window.oikome.sql(t), text);

const JOIN =
  "SELECT o.id, o.customer, p.title FROM orders.orders o " +
  "JOIN pub.posts p ON p.author = o.customer ORDER BY o.id, p.title";

test("a duckdb-wasm table joins a Full Node's table over WebRTC until the token is revoked", async ({
  page,
}) => {
  await page.goto("/");
  await ready(page);
  const did = await page.evaluate(() => window.oikome.did());

  nodeSql(`GRANT SELECT (id, author, title) ON blog.posts TO "${did}"`);
  nodeSql(`CREATE POLICY published ON blog.posts TO "${did}" USING (NOT draft)`);
  const [[id, , , token]] = nodeSql(`ISSUE UCAN ON SPACE blog TO '${did}'`);
  const [[record]] = nodeSql("SHOW WEBRTC");

  await sql(page, "CREATE SPACE orders");
  await sql(page, "CREATE TABLE orders.orders (id BIGINT, customer VARCHAR)");
  await sql(page, "INSERT INTO orders.orders VALUES (10, 'alice'), (11, 'carol'), (12, 'bob')");
  await sql(
    page,
    `CREATE SPACE pub WITH PEER = '${record}', REMOTE = 'blog', TOKEN = '${token}'`,
  );

  const expected = [
    { id: 10, customer: "alice", title: "Again" },
    { id: 10, customer: "alice", title: "Hello" },
    { id: 12, customer: "bob", title: "World" },
  ];
  // The draft post is filtered on the Full Node by its row policy.
  expect(await sql(page, JOIN)).toEqual(expected);
  expect(
    await sql(page, "SELECT author, count(*) AS n FROM pub.posts GROUP BY author ORDER BY author"),
  ).toEqual([
    { author: "alice", n: 2 },
    { author: "bob", n: 1 },
  ]);
  expect(
    await sql(
      page,
      "SELECT customer AS name FROM orders.orders UNION SELECT author FROM pub.posts ORDER BY name",
    ),
  ).toEqual([{ name: "alice" }, { name: "bob" }, { name: "carol" }]);
  // Only the granted columns.
  expect(Object.keys((await sql(page, "SELECT * FROM pub.posts ORDER BY id"))[0])).toEqual([
    "id",
    "author",
    "title",
  ]);

  // The mount survives a reload (a new channel, the same key and token).
  await page.reload();
  await ready(page);
  expect(await sql(page, JOIN)).toEqual(expected);

  // Revocation on the Full Node takes effect on the next query.
  nodeSql(`REVOKE UCAN '${id}'`);
  await expect(sql(page, JOIN)).rejects.toThrow(/permission denied/);
});

test("tampered records and wrong fingerprints are refused", async ({ page }) => {
  await page.goto("/");
  await ready(page);
  const did = await page.evaluate(() => window.oikome.did());
  nodeSql(`GRANT SELECT ON blog.posts TO "${did}"`);
  // Once a table has a row policy, a principal no policy covers sees no rows.
  nodeSql(`CREATE POLICY everything ON blog.posts TO "${did}" USING (true)`);
  const [[, , , token]] = nodeSql(`ISSUE UCAN ON SPACE blog TO '${did}'`);
  const [[record]] = nodeSql("SHOW WEBRTC");

  // A record pointed at another endpoint no longer matches the node's
  // signature.
  const signature = record.split(".")[1];
  const redirected = { ...recordFields(record), addr: "127.0.0.1:9" };
  const body = Buffer.from(JSON.stringify(redirected)).toString("base64url");
  const tampered = `webrtc:${body}.${signature}`;
  await expect(
    sql(page, `CREATE SPACE pub WITH PEER = '${tampered}', REMOTE = 'blog', TOKEN = '${token}'`),
  ).rejects.toThrow(/PEER: access record signature does not match its DID/);

  // The browser accepts only the certificate the record pins: dialing the
  // node's address with another fingerprint fails the DTLS handshake.
  const { addr, fingerprint } = recordFields(record);
  const last = fingerprint.at(-1) === "0" ? "1" : "0";
  const wrong = `${fingerprint.slice(0, -1)}${last}`;
  await expect(
    page.evaluate(
      ([a, f]) => window.oikome.transport.call(a, f, new Uint8Array([1])),
      [addr, wrong],
    ),
  ).rejects.toThrow(/could not open a WebRTC channel/);

  // The right fingerprint still works.
  await sql(page, `CREATE SPACE pub WITH PEER = '${record}', REMOTE = 'blog', TOKEN = '${token}'`);
  expect(await sql(page, "SELECT count(*) AS n FROM pub.posts")).toEqual([{ n: 4 }]);
});
