import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "@playwright/test";

type SearchQuery = Readonly<{
  canonical_url: string;
  query: string;
  synonyms: readonly string[];
}>;

type SearchAcceptanceSet = Readonly<{ queries: readonly SearchQuery[] }>;

const acceptancePath = process.env["DOCS_SEARCH_ACCEPTANCE_PATH"] ?? resolve(__dirname, "../../docs/search-acceptance.json");
const acceptance: SearchAcceptanceSet = JSON.parse(readFileSync(acceptancePath, "utf8"));

test.describe("audience-first documentation search", () => {
  for (const entry of acceptance.queries) {
    test(`ranks ${entry.query} in the top three`, async ({ page }) => {
      await page.goto("/");
      await page.getByRole("textbox", { name: "Search" }).fill(entry.query);

      const resultLinks = page.locator(".md-search-result__link");
      await expect(resultLinks.first()).toBeVisible();
      const resultUrls = await resultLinks.evaluateAll((links) =>
        links.map((link) => link.getAttribute("href") ?? ""),
      );
      const rank = resultUrls.findIndex(
        (href) => new URL(href, page.url()).pathname === entry.canonical_url,
      ) + 1;
      expect(rank, `expected ${entry.canonical_url}; observed rank ${rank}`).toBeGreaterThan(0);
      expect(rank, `expected ${entry.canonical_url}; observed rank ${rank}`).toBeLessThanOrEqual(3);

      await resultLinks.nth(rank - 1).click();
      await expect(page).toHaveURL((url) => new URL(url).pathname === entry.canonical_url);
    });
  }
});
