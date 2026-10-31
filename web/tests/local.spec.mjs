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

// 3.2a: the browser node runs local queries on duckdb-wasm; data (OPFS),
// identity and registry (IndexedDB) survive a reload.

import { expect, test } from "@playwright/test";

async function ready(page) {
  await page.waitForFunction(() => document.body.dataset.ready || document.body.dataset.error);
  const error = await page.evaluate(() => document.body.dataset.error);
  expect(error, "boot error").toBeUndefined();
}

const sql = (page, text) => page.evaluate((t) => window.agora.sql(t), text);

test("local queries on duckdb-wasm survive a reload", async ({ page }) => {
  await page.goto("/");
  await ready(page);
  const did = await page.evaluate(() => window.agora.did());
  expect(did).toMatch(/^did:key:z6Mk/);

  await sql(page, "CREATE SPACE notes");
  await sql(page, "CREATE TABLE notes.items (id BIGINT NOT NULL, title VARCHAR)");
  expect(await sql(page, "INSERT INTO notes.items VALUES (1, 'a'), (2, 'b'), (3, 'c')")).toEqual({
    rowsAffected: 3,
  });
  expect(await sql(page, "SELECT id, title FROM notes.items WHERE id > 1 ORDER BY id")).toEqual([
    { id: 2, title: "b" },
    { id: 3, title: "c" },
  ]);
  await sql(page, "SET SPACE notes");
  expect(await sql(page, "SELECT count(*) AS n FROM items")).toEqual([{ n: 3 }]);

  await page.reload();
  await ready(page);
  // Same key, same registry (default Space included), same data.
  expect(await page.evaluate(() => window.agora.did())).toBe(did);
  expect(await sql(page, "SELECT count(*) AS n FROM items")).toEqual([{ n: 3 }]);
});

test("errors from the Rust core reach JavaScript", async ({ page }) => {
  await page.goto("/");
  await ready(page);
  await expect(sql(page, "SELECT * FROM nowhere.t")).rejects.toThrow(/Space not found: nowhere/);
  await expect(sql(page, "CREATE SPACE shop WITH KIND = 'transactional'")).rejects.toThrow(
    /not available in the browser node/,
  );
  await sql(page, "CREATE SPACE notes");
  await expect(sql(page, "SELECT * FROM notes.missing")).rejects.toThrow(/missing/);
});
