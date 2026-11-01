// Copyright 2025 The Oikome Authors
// SPDX-License-Identifier: Apache-2.0

import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  timeout: 60_000,
  use: { baseURL: "http://127.0.0.1:4173", ...devices["Desktop Chrome"] },
  webServer: {
    command: "node serve.mjs",
    url: "http://127.0.0.1:4173",
    reuseExistingServer: !process.env.CI,
  },
});
