import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

test.describe("user-guide onboarding accessibility", () => {
    test("has no serious or critical violations on the first 10 minutes guide", async ({ page }) => {
        await page.goto("/user-guide/first-10-minutes/");

        const results = await new AxeBuilder({ page })
            .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
            .analyze();
        const blocking = results.violations.filter(
            (violation) => violation.impact === "serious" || violation.impact === "critical",
        );

        expect(blocking).toEqual([]);
    });
});

test.describe("diagnostic guides accessibility", () => {
    test("has no serious or critical violations on Quadrants", async ({ page }) => {
        await page.goto("/user-guide/views/quadrants/");

        const results = await new AxeBuilder({ page })
            .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
            .analyze();
        const blocking = results.violations.filter(
            (violation) => violation.impact === "serious" || violation.impact === "critical",
        );

        expect(blocking).toEqual([]);
    });
});
