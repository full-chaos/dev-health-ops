import { expect, test } from "@playwright/test";

const onboardingPath = "/user-guide/first-10-minutes/";
const flowGuides = [
    { path: "/user-guide/views/pr-flow/", continuation: "Next step: Plan capacity" },
    { path: "/user-guide/views/capacity-planning/", continuation: "Next step: Follow work relationships" },
    { path: "/user-guide/views/work-graph/", continuation: "Next step: Return to views and charts" },
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
            });
        }
    }
});
