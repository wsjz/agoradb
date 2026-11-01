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

// Byte storage for the browser node (its seed and registry), in IndexedDB.
// This is the `HostStore` crates/oikome-wasm/src/bindings.rs calls.

const STORE = "kv";

function request(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

/** Open (or create) the IndexedDB database `name`. */
export async function openStore(name = "oikome") {
  const open = indexedDB.open(name, 1);
  open.onupgradeneeded = () => open.result.createObjectStore(STORE);
  const db = await request(open);

  function run(mode, operation) {
    const tx = db.transaction(STORE, mode);
    const result = request(operation(tx.objectStore(STORE)));
    const done = new Promise((resolve, reject) => {
      tx.oncomplete = resolve;
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
    return Promise.all([result, done]).then(([value]) => value);
  }

  return {
    get: (key) => run("readonly", (s) => s.get(key)),
    put: (key, bytes) => run("readwrite", (s) => s.put(bytes, key)).then(() => undefined),
  };
}
