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

test.describe("AI view guides accessibility", () => {
    for (const guide of [
        { path: "/user-guide/views/ai-impact/", title: "AI Impact" },
        { path: "/user-guide/views/ai-review-load/", title: "AI Review Load" },
        { path: "/user-guide/views/ai-risk/", title: "AI Risk" },
        { path: "/user-guide/views/ai-attribution/", title: "AI Attribution" },
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

    test("keeps the in-flow Review Load evidence trail available to assistive technology", async ({ page }) => {
        await page.setViewportSize({ width: 375, height: 900 });
        await page.goto("/user-guide/views/ai-review-load/");

        await expect(
            page
                .getByRole("complementary", { name: "Evidence trail" })
                .getByRole("link", { name: "Open the evidence model" }),
        ).toBeVisible();
        const results = await new AxeBuilder({ page })
            .withTags(["wcag2a", "wcag2aa", "wcag21aa", "wcag22aa"])
            .analyze();
        const blocking = results.violations.filter(
            (violation) => violation.impact === "serious" || violation.impact === "critical",
        );

        expect(blocking).toEqual([]);
    });

    for (const viewport of [
        { name: "mobile", width: 375, height: 900, variant: "in-flow" },
        { name: "tablet", width: 768, height: 900, variant: "in-flow" },
        { name: "desktop", width: 1280, height: 900, variant: "rail" },
    ] as const) {
        test(`exposes one named Evidence Trail ${viewport.variant} landmark at ${viewport.name}`, async ({ page }) => {
            await page.setViewportSize({ width: viewport.width, height: viewport.height });
            await page.goto("/user-guide/views/ai-impact/");

            const evidenceTrail = page.getByRole("complementary", {
                name: "Evidence trail",
            });
            await expect(evidenceTrail).toHaveCount(1);
            await expect(evidenceTrail).toHaveClass(
                new RegExp(`fc-evidence-rail--${viewport.variant}`),
            );
        });
    }
});
