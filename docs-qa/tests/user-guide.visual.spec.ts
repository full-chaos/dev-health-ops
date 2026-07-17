import { expect, test } from "@playwright/test";

import { userGuideCoverage, userGuideViewports } from "./support/userGuideCoverage";

test.describe("user-guide coverage matrix", () => {
  for (const sample of userGuideCoverage) {
    for (const viewport of userGuideViewports) {
      test(`${sample.id} is evidence-complete at ${viewport.name}`, async ({ page }) => {
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        const response = await page.goto(sample.path);

        if (response === null) {
          throw new Error(`No document response for ${sample.path}`);
        }
        expect(response.status()).toBe(200);

        const article = page.locator(".md-content");
        await expect(article).toBeVisible();
        await expect(article.getByRole("heading", { name: sample.title })).toBeVisible();

        const dimensions = await page.evaluate(() => ({
          clientWidth: document.documentElement.clientWidth,
          scrollWidth: document.documentElement.scrollWidth,
        }));
        expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);

        const evidenceTrail = page.getByRole("complementary", { name: "Evidence trail" });
        await expect(evidenceTrail).toHaveCount(1);
        await expect(evidenceTrail).toBeVisible();
        await expect(evidenceTrail).toHaveClass(
          new RegExp(`fc-evidence-rail--${viewport.variant}`),
        );

        const actionExpectation =
          viewport.variant === "rail" && "desktopAction" in sample
            ? sample.desktopAction
            : sample.action;
        const action = evidenceTrail.getByRole("link", { name: actionExpectation.label });
        await expect(action).toBeVisible();
        await action.focus();
        await expect(action).toBeFocused();
        const hasVisibleFocus = await action.evaluate((element) => {
          const styles = getComputedStyle(element);
          return styles.boxShadow !== "none" || styles.outlineStyle !== "none";
        });
        expect(hasVisibleFocus).toBeTruthy();

        await action.press("Enter");
        await expect(page).toHaveURL(
          (url) => new URL(url).pathname === actionExpectation.target,
        );
      });
    }
  }

  test("keeps a supplied CJK label and long source key within the narrow glossary viewport", async ({ page }) => {
    // Given a narrow reader viewport.
    await page.setViewportSize({ width: 375, height: 900 });

    // When the glossary renders source-label guidance.
    await page.goto("/user-guide/glossary/");

    // Then the multilingual label and unbroken source key remain visible without widening the page.
    await expect(page.getByText("진행 중인 검토", { exact: true })).toBeVisible();
    await expect(
      page.getByText("sourceEventRef_2026Q3EngineeringEnablementMigration", { exact: true }),
    ).toBeVisible();
    const dimensions = await page.evaluate(() => ({
      clientWidth: document.documentElement.clientWidth,
      scrollWidth: document.documentElement.scrollWidth,
    }));
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
  });
});
