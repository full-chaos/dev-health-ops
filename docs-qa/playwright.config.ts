import { defineConfig, devices } from "@playwright/test";

import { previewHeaders } from "./tests/support/accessHeaders";

const docsBaseUrl = process.env["DOCS_BASE_URL"] ?? "http://127.0.0.1:8008";
const usesProtectedPreview = process.env["DOCS_BASE_URL"] !== undefined;

export default defineConfig({
  testDir: "./tests",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  outputDir: "./test-results",
  use: {
    baseURL: docsBaseUrl,
    extraHTTPHeaders: previewHeaders(),
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chrome-visual",
      use: { ...devices["Desktop Chrome"], channel: "chrome" },
      testMatch: "**/*.visual.spec.ts",
    },
    {
      name: "chrome-a11y",
      use: { ...devices["Desktop Chrome"], channel: "chrome" },
      testMatch: "**/*.a11y.spec.ts",
    },
  ],
  ...(usesProtectedPreview
    ? {}
    : {
        webServer: {
        command: "python3 -m http.server 8008 --directory ../.build/site",
        url: "http://127.0.0.1:8008/",
        reuseExistingServer: !process.env["CI"],
        timeout: 30_000,
        },
      }),
});
