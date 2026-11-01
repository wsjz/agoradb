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

// Boot the browser node and expose it as `window.oikome` (used by the page
// and the Playwright tests).

import { tableFromIPC } from "apache-arrow";
import init, { OikomeNode } from "../pkg/oikome_wasm.js";
import { createEngine } from "./host.js";
import { openStore } from "./store.js";
import { createTransport } from "./transport.js";

/** Rows of an Arrow IPC stream as plain objects (BigInt as Number). */
function rows(ipc) {
  return tableFromIPC(ipc)
    .toArray()
    .map((row) =>
      Object.fromEntries(
        Object.entries(row.toJSON()).map(([k, v]) => [k, typeof v === "bigint" ? Number(v) : v]),
      ),
    );
}

async function boot() {
  await init({ module_or_path: new URL("../pkg/oikome_wasm_bg.wasm", import.meta.url) });
  const transport = createTransport();
  const node = await OikomeNode.open(await createEngine(), await openStore(), transport);
  window.oikome = {
    did: () => node.did(),
    transport,
    async sql(text) {
      const result = await node.sql(text);
      return result.ipc ? rows(result.ipc) : { rowsAffected: result.rowsAffected ?? null };
    },
  };
  document.querySelector("#did").textContent = node.did();
  document.body.dataset.ready = "true";
}

boot().catch((e) => {
  document.body.dataset.error = String(e?.message ?? e);
  console.error(e);
});
