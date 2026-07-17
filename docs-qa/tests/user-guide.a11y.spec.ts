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

test.describe("flow and planning guides accessibility", () => {
    for (const guide of [
        { path: "/user-guide/views/pr-flow/", title: "PR Flow" },
        { path: "/user-guide/views/capacity-planning/", title: "Capacity Planning View" },
        { path: "/user-guide/views/work-graph/", title: "Work Graph: follow relationships" },
    ] as const) {
        test(`has no serious or critical violations on ${guide.title}`, async ({ page }) => {
            await page.goto(guide.path);

            const results = await new AxeBuilder({ page })
                .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
                .analyze();
            const blocking = results.violations.filter(
                (violation) => violation.impact === "serious" || violation.impact === "critical",
            );

            expect(blocking).toEqual([]);
        });
    }
});
