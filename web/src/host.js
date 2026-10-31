// Copyright 2025 The AgoraDB Authors
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

// The browser node's engine: duckdb-wasm in a worker, its database in OPFS.
// This is the `HostEngineAdapter` crates/agoradb-wasm/src/host.rs calls;
// every result crosses to Rust as an Arrow IPC stream.

import * as duckdb from "@duckdb/duckdb-wasm";
import { tableToIPC } from "apache-arrow";

const DIST = new URL("/node_modules/@duckdb/duckdb-wasm/dist/", location.origin);

const BUNDLES = {
  mvp: {
    mainModule: new URL("duckdb-mvp.wasm", DIST).href,
    mainWorker: new URL("duckdb-browser-mvp.worker.js", DIST).href,
  },
  eh: {
    mainModule: new URL("duckdb-eh.wasm", DIST).href,
    mainWorker: new URL("duckdb-browser-eh.worker.js", DIST).href,
  },
};

function quoteIdent(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

/** Open duckdb-wasm on `opfs://<file>` and return the engine adapter. */
export async function createEngine(file = "agora.duckdb") {
  const bundle = await duckdb.selectBundle(BUNDLES);
  const worker = new Worker(bundle.mainWorker);
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  await db.open({ path: `opfs://${file}`, accessMode: duckdb.DuckDBAccessMode.READ_WRITE });
  const conn = await db.connect();

  return {
    async query(sql) {
      return tableToIPC(await conn.query(sql), "stream");
    },

    async execute(sql) {
      const result = await conn.query(sql);
      // Data reaches OPFS at a checkpoint; take one so a reload sees it.
      await conn.query("CHECKPOINT");
      const count = result.numCols === 1 ? result.getChildAt(0)?.get(0) : undefined;
      return typeof count === "bigint" || typeof count === "number" ? Number(count) : 0;
    },

    async tableNames(space) {
      const stmt = await conn.prepare(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY table_name",
      );
      try {
        const table = await stmt.query(space);
        return table.getChild("table_name").toArray().map(String);
      } finally {
        await stmt.close();
      }
    },

    async tableSchema(space, table) {
      const empty = await conn.query(`SELECT * FROM ${quoteIdent(space)}.${quoteIdent(table)} LIMIT 0`);
      return tableToIPC(empty, "stream");
    },
  };
}
