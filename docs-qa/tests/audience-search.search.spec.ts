import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

type SearchQuery = Readonly<{
  canonical_url: string;
  query: string;
  synonyms: readonly string[];
}>;

type SearchAcceptanceSet = Readonly<{ queries: readonly SearchQuery[] }>;

type SearchOutcome =
  | Readonly<{ kind: "results"; urls: readonly string[] }>
  | Readonly<{ kind: "no-results"; status: string }>;

const acceptancePath =
  process.env["DOCS_SEARCH_ACCEPTANCE_PATH"] ??
  resolve(__dirname, "../../docs/search-acceptance.json");
const acceptance: SearchAcceptanceSet = JSON.parse(readFileSync(acceptancePath, "utf8"));
const SEARCH_INDEX_PATH = "/search/search_index.json";
const SEARCH_READY_QUERY = "Inspectability";
const SEARCH_READY_PATH = "/product/concepts/";

const searchInput = (page: Page) => page.getByRole("textbox", { name: "Search" });
const searchResultLinks = (page: Page) => page.locator(".md-search-result__link");
const searchStatus = (page: Page) => page.locator(".md-search-result__meta");

const pathname = (url: string, page: Page): string => new URL(url, page.url()).pathname;

const assertNever = (value: never): never => {
  throw new Error(`Unhandled search outcome: ${JSON.stringify(value)}`);
};

const resultUrlsFor = (outcome: SearchOutcome): readonly string[] => {
  switch (outcome.kind) {
    case "results":
      return outcome.urls;
    case "no-results":
      throw new Error(`search completed without results: ${outcome.status}`);
    default:
      return assertNever(outcome);
  }
};

const assertNoResults = (outcome: SearchOutcome): void => {
  switch (outcome.kind) {
    case "no-results":
      expect(outcome.status).toContain("No matching documents");
      return;
    case "results":
      expect(outcome.urls, "stale result was rendered for a no-match query").toEqual([]);
      return;
    default:
      return assertNever(outcome);
  }
};

const submitSearchQuery = async (page: Page, query: string): Promise<void> => {
  const input = searchInput(page);

  await input.fill(query);
  await input.press("End");
  await expect(input).toHaveValue(query);
};

const waitForSearchOutcome = async (page: Page): Promise<SearchOutcome> => {
  const resultLinks = searchResultLinks(page);
  const status = searchStatus(page);

  await expect
    .poll(async () => {
      const [resultCount, statusText] = await Promise.all([
        resultLinks.count(),
        status.textContent(),
      ]);
      if (resultCount > 0) {
        return "results";
      }
      return /no matching documents/i.test(statusText ?? "") ? "no-results" : "pending";
    })
    .not.toBe("pending");

  const urls = await resultLinks.evaluateAll((links) =>
    links.map((link) => link.getAttribute("href") ?? ""),
  );
  if (urls.length > 0) {
    return { kind: "results", urls };
  }
  return { kind: "no-results", status: (await status.textContent()) ?? "" };
};

const searchFor = async (page: Page, query: string): Promise<SearchOutcome> => {
  await submitSearchQuery(page, query);
  return waitForSearchOutcome(page);
};

const resetSearch = async (page: Page): Promise<void> => {
  await searchInput(page).fill("");
  await searchInput(page).press("End");
  await expect(searchInput(page)).toHaveValue("");
  await expect(searchStatus(page)).toHaveText("Type to start searching");
  await expect(searchResultLinks(page)).toHaveCount(0);
};

const waitForSearchApplication = async (page: Page): Promise<void> => {
  const indexResponse = page.waitForResponse(
    (response) => pathname(response.url(), page) === SEARCH_INDEX_PATH && response.ok(),
  );
  const searchWorker = page.waitForEvent(
    "worker",
    (worker) => pathname(worker.url(), page).includes("/assets/javascripts/workers/search."),
  );
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await Promise.all([indexResponse, searchWorker]);

  const readyUrls = resultUrlsFor(await searchFor(page, SEARCH_READY_QUERY));
  expect(readyUrls.some((url) => pathname(url, page) === SEARCH_READY_PATH)).toBeTruthy();
  await resetSearch(page);
};

const assertTopThreeUrls = async (
  page: Page,
  entry: SearchQuery,
  resultUrls: readonly string[],
): Promise<void> => {
  const rank = resultUrls.findIndex((url) => pathname(url, page) === entry.canonical_url) + 1;
  expect(rank, `expected ${entry.canonical_url}; observed rank ${rank}`).toBeGreaterThan(0);
  expect(rank, `expected ${entry.canonical_url}; observed rank ${rank}`).toBeLessThanOrEqual(3);

  await searchResultLinks(page).nth(rank - 1).click();
  await expect(page).toHaveURL((url) => pathname(url.href, page) === entry.canonical_url);
};

const assertTopThreeResult = async (page: Page, entry: SearchQuery): Promise<void> => {
  await assertTopThreeUrls(page, entry, resultUrlsFor(await searchFor(page, entry.query)));
};

const assertTopThree = async (page: Page, entry: SearchQuery): Promise<void> => {
  await waitForSearchApplication(page);
  await assertTopThreeResult(page, entry);
};

test.describe("audience-first documentation search", () => {
  test.describe.configure({ mode: "parallel" });

  test("does not drop a query while the search index is delayed", async ({ page }) => {
    const indexRequested = Promise.withResolvers<void>();
    const releaseIndex = Promise.withResolvers<void>();
    await page.route("**/search/search_index.json", async (route) => {
      indexRequested.resolve();
      await releaseIndex.promise;
      await route.continue();
    });
    const searchWorker = page.waitForEvent(
      "worker",
      (worker) => pathname(worker.url(), page).includes("/assets/javascripts/workers/search."),
    );
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await Promise.all([indexRequested.promise, searchWorker]);

    const target = {
      query: "Capacity Planning View",
      synonyms: [],
      canonical_url: "/user-guide/views/capacity-planning/",
    } as const;
    await submitSearchQuery(page, target.query);
    const targetOutcome = waitForSearchOutcome(page);
    releaseIndex.resolve();
    await assertTopThreeUrls(page, target, resultUrlsFor(await targetOutcome));
  });

  test("synchronizes an explicit no-match state before another query", async ({ page }) => {
    await waitForSearchApplication(page);
    resultUrlsFor(await searchFor(page, "Capacity Planning View"));
    await resetSearch(page);
    assertNoResults(await searchFor(page, "qzxwvut987654321"));
    await resetSearch(page);
  });

  for (const entry of acceptance.queries) {
    test(`ranks ${entry.query} in the top three`, async ({ page }) => {
      await assertTopThree(page, entry);
    });
  }
});
