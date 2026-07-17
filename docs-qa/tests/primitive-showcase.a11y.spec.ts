import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

test.describe("primitive showcase accessibility", () => {
  test("has no serious or critical axe violations", async ({ page }) => {
    await page.goto("/reference/primitive-showcase/");

    const results = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
      .analyze();
    const blocking = results.violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );

    expect(blocking).toEqual([]);
  });
});
