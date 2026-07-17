import { expect, test } from "@playwright/test";

const taskPaths = [
  "/product/concepts/",
  "/user-guide/views-index/",
  "/self-hosted-quickstart/",
  "/customer-push-ingestion/overview/",
  "/api/graphql-overview/",
  "/contributing/platform-contract/",
] as const;

test.describe("audience-first documentation navigation", () => {
  test("offers six task paths in the first desktop viewport", async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 900 });
    await page.goto("/");

    const paths = page.locator(".fc-task-ctas a");
    await expect(page.locator('link[rel="canonical"]')).toHaveAttribute(
      "href",
      "https://docs.fullchaos.dev/",
    );
    await expect(page.getByLabel("Page information")).toContainText("For Start here");
    await expect(page.getByRole("navigation", { name: "Continue this documentation path" })).toContainText(
      "Next step: Understand the product model",
    );
    await expect(paths).toHaveCount(taskPaths.length);
    await expect(paths.first()).toBeVisible();

    const bounds = await paths.evaluateAll((links) =>
      links.map((link) => link.getBoundingClientRect().bottom),
    );
    expect(Math.max(...bounds)).toBeLessThanOrEqual(900);

    const hrefs = await paths.evaluateAll((links) => links.map((link) => link.getAttribute("href") ?? ""));
    for (const path of taskPaths) {
      expect(hrefs.some((href) => new URL(href).pathname === path)).toBeTruthy();
    }
  });

  test("keeps the landing task paths keyboard-reachable", async ({ page }) => {
    await page.goto("/");

    const firstTask = page.getByRole("link", { name: "Understand the product", exact: true });
    await firstTask.focus();
    await expect(firstTask).toHaveCSS("box-shadow", /rgb/);
    await page.keyboard.press("Enter");
    await expect(page).toHaveURL((url) => new URL(url).pathname === taskPaths[0]);
  });

  for (const viewport of [
    { name: "mobile", width: 375, height: 900 },
    { name: "tablet", width: 768, height: 900 },
    { name: "desktop", width: 1280, height: 900 },
  ] as const) {
    test(`keeps task navigation readable at ${viewport.name}`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto("/");

      const dimensions = await page.evaluate(() => ({
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: document.documentElement.scrollWidth,
      }));
      expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
      await page.screenshot({ path: testInfo.outputPath(`audience-navigation-${viewport.name}.png`), fullPage: true });
    });
  }
});
