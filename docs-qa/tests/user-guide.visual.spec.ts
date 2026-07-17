import { expect, test } from "@playwright/test";

const onboardingPath = "/user-guide/first-10-minutes/";
const flowGuides = [
    { path: "/user-guide/views/pr-flow/", continuation: "Next step: Plan capacity" },
    { path: "/user-guide/views/capacity-planning/", continuation: "Next step: Follow work relationships" },
    { path: "/user-guide/views/work-graph/", continuation: "Next step: Return to views and charts" },
] as const;
const aiGuides = [
    { path: "/user-guide/views/ai-impact/", continuation: "Next step: Compare AI review load" },
    { path: "/user-guide/views/ai-review-load/", continuation: "Next step: Review AI risk" },
    { path: "/user-guide/views/ai-risk/", continuation: "Next step: Inspect AI attribution" },
    { path: "/user-guide/views/ai-attribution/", continuation: "Next step: Return to views and charts" },
] as const;
const reportsAndMetricsGuides = [
    { path: "/user-guide/reports/", continuation: "Next step: Interpret shared metrics" },
    { path: "/user-guide/metrics-interpretation/", continuation: "Next step: Review reports" },
] as const;
const responsiveEvidenceGuides = [
    onboardingPath,
    "/user-guide/views/quadrants/",
    "/user-guide/views/flame-diagrams/",
    "/user-guide/views/code-hotspots/",
    "/user-guide/views/pr-flow/",
    "/user-guide/views/capacity-planning/",
    "/user-guide/views/work-graph/",
    "/user-guide/views/ai-impact/",
    "/user-guide/views/ai-review-load/",
    "/user-guide/views/ai-risk/",
    "/user-guide/views/ai-attribution/",
    "/user-guide/reports/",
    "/user-guide/metrics-interpretation/",
] as const;

test.describe("user-guide onboarding", () => {
    for (const viewport of [
        { name: "mobile", width: 375, height: 900 },
        { name: "tablet", width: 768, height: 900 },
        { name: "desktop", width: 1280, height: 900 },
    ] as const) {
        test(`keeps the first 10 minutes guide readable at ${viewport.name}`, async ({ page }) => {
            await page.setViewportSize({ width: viewport.width, height: viewport.height });
            await page.goto(onboardingPath);

            await expect(page.getByRole("heading", { name: "Your first 10 minutes" })).toBeVisible();
            const dimensions = await page.evaluate(() => ({
                clientWidth: document.documentElement.clientWidth,
                scrollWidth: document.documentElement.scrollWidth,
            }));
            expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
            await expect(
                page
                    .getByRole("navigation", { name: "Continue this documentation path" })
                    .getByRole("link", { name: "Next step: Learn how to read a signal" }),
            ).toHaveCSS("color", "rgb(255, 250, 242)");
        });
    }
});

test.describe("diagnostic guides", () => {
    for (const viewport of [
        { name: "mobile", width: 375, height: 900 },
        { name: "tablet", width: 768, height: 900 },
        { name: "desktop", width: 1280, height: 900 },
    ] as const) {
        test(`render without horizontal overflow at ${viewport.name}`, async ({ page }) => {
            await page.setViewportSize({ width: viewport.width, height: viewport.height });
            await page.goto("/user-guide/views/quadrants/");

            const dimensions = await page.evaluate(() => ({
                clientWidth: document.documentElement.clientWidth,
                scrollWidth: document.documentElement.scrollWidth,
            }));
            expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
            await expect(
                page
                    .getByRole("navigation", { name: "Continue this documentation path" })
                    .getByRole("link", { name: "Next step: Diagnose a single item" }),
            ).toHaveCSS("color", "rgb(255, 250, 242)");
        });
    }
});

test.describe("flow and planning guides", () => {
    for (const viewport of [
        { name: "mobile", width: 375, height: 900 },
        { name: "tablet", width: 768, height: 900 },
        { name: "desktop", width: 1280, height: 900 },
    ] as const) {
        for (const guide of flowGuides) {
            test(`renders ${guide.path} without horizontal overflow at ${viewport.name}`, async ({ page }) => {
                await page.setViewportSize({ width: viewport.width, height: viewport.height });
                await page.goto(guide.path);

                const dimensions = await page.evaluate(() => ({
                    clientWidth: document.documentElement.clientWidth,
                    scrollWidth: document.documentElement.scrollWidth,
                }));
                expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
                await expect(
                    page
                        .getByRole("navigation", { name: "Continue this documentation path" })
                        .getByRole("link", { name: guide.continuation }),
                ).toHaveCSS("color", "rgb(255, 250, 242)");

                const evidenceTrail = page.getByRole("complementary", {
                    name: "Evidence trail",
                });
                await expect(evidenceTrail).toBeVisible();
                await evidenceTrail
                    .locator(".fc-evidence-rail__step > span:last-child")
                    .first()
                    .evaluate(
                        (element) =>
                            (element.textContent =
                                "긴문장검증과AIVIEW_EVIDENCE_TRAIL_UNBROKEN_TOKEN_1234567890"),
                    );
                const wrappedDimensions = await page.evaluate(() => ({
                    clientWidth: document.documentElement.clientWidth,
                    scrollWidth: document.documentElement.scrollWidth,
                }));
                expect(wrappedDimensions.scrollWidth).toBeLessThanOrEqual(
                    wrappedDimensions.clientWidth,
                );
            });
        }
    }
});

test.describe("AI view guides", () => {
    for (const viewport of [
        { name: "mobile", width: 375, height: 900 },
        { name: "tablet", width: 768, height: 900 },
        { name: "desktop", width: 1280, height: 900 },
    ] as const) {
        for (const guide of aiGuides) {
            test(`renders ${guide.path} without horizontal overflow at ${viewport.name}`, async ({ page }) => {
                await page.setViewportSize({ width: viewport.width, height: viewport.height });
                await page.goto(guide.path);

                const dimensions = await page.evaluate(() => ({
                    clientWidth: document.documentElement.clientWidth,
                    scrollWidth: document.documentElement.scrollWidth,
                }));
                expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
                await expect(
                    page
                        .getByRole("navigation", { name: "Continue this documentation path" })
                        .getByRole("link", { name: guide.continuation }),
                ).toHaveCSS("color", "rgb(255, 250, 242)");

                const evidenceTrail = page.getByRole("complementary", {
                    name: "Evidence trail",
                });
                await expect(evidenceTrail).toHaveCount(1);
                await evidenceTrail
                    .locator(".fc-evidence-rail__step > span:last-child")
                    .first()
                    .evaluate(
                        (element) =>
                            (element.textContent =
                                "긴문장검증과AIVIEW_EVIDENCE_TRAIL_UNBROKEN_TOKEN_1234567890"),
                    );
                const wrappedDimensions = await page.evaluate(() => ({
                    clientWidth: document.documentElement.clientWidth,
                    scrollWidth: document.documentElement.scrollWidth,
                }));
                expect(wrappedDimensions.scrollWidth).toBeLessThanOrEqual(
                    wrappedDimensions.clientWidth,
                );
            });
        }
    }
});

test.describe("reports and metrics guides", () => {
    for (const viewport of [
        { name: "mobile", width: 375, height: 900 },
        { name: "tablet", width: 768, height: 900 },
        { name: "desktop", width: 1280, height: 900 },
    ] as const) {
        for (const guide of reportsAndMetricsGuides) {
            test(`renders ${guide.path} without horizontal overflow at ${viewport.name}`, async ({ page }) => {
                await page.setViewportSize({ width: viewport.width, height: viewport.height });
                await page.goto(guide.path);

                const dimensions = await page.evaluate(() => ({
                    clientWidth: document.documentElement.clientWidth,
                    scrollWidth: document.documentElement.scrollWidth,
                }));
                expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
                await expect(
                    page
                        .getByRole("navigation", { name: "Continue this documentation path" })
                        .getByRole("link", { name: guide.continuation }),
                ).toHaveCSS("color", "rgb(255, 250, 242)");
            });
        }
    }

    test("hides collapsed duplicate navigation routes on desktop", async ({ page }) => {
        await page.setViewportSize({ width: 1280, height: 900 });
        await page.goto("/user-guide/reports/");

        const collapsedRoute = page
            .locator('.md-sidebar--primary .md-nav[aria-expanded="false"]')
            .getByRole("link", { name: "Find the right view" })
            .first();
        await expect(collapsedRoute).toBeHidden();
    });
});

test.describe("responsive Evidence Trail contract", () => {
    for (const viewport of [
        { name: "mobile", width: 375, height: 900, variant: "in-flow" },
        { name: "tablet", width: 768, height: 900, variant: "in-flow" },
        { name: "desktop", width: 1280, height: 900, variant: "rail" },
    ] as const) {
        for (const path of responsiveEvidenceGuides) {
            test(`renders one visible ${viewport.variant} Evidence Trail for ${path} at ${viewport.name}`, async ({ page }) => {
                await page.setViewportSize({ width: viewport.width, height: viewport.height });
                await page.goto(path);

                const evidenceTrail = page.getByRole("complementary", {
                    name: "Evidence trail",
                });
                await expect(evidenceTrail).toHaveCount(1);
                await expect(evidenceTrail).toHaveClass(
                    new RegExp(`fc-evidence-rail--${viewport.variant}`),
                );
                const dimensions = await page.evaluate(() => ({
                    clientWidth: document.documentElement.clientWidth,
                    scrollWidth: document.documentElement.scrollWidth,
                }));
                expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
            });
        }
    }
});
