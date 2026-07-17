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

    test("scrolls the narrow common-measures table with scoped keyboard controls", async ({ page }) => {
        // Given the common-measures table overflows on a narrow reader viewport.
        await page.setViewportSize({ width: 375, height: 900 });

        // When the guide is rendered with instant navigation enabled.
        await page.goto("/user-guide/how-to-read-dev-health/");

        // Then keyboard users can focus and scroll the table's horizontal scroll region.
        const table = page.locator(".md-typeset__table");
        await expect(table).toHaveAttribute("tabindex", "0");
        await table.focus();
        await expect(table).toBeFocused();

        const initialScrollLeft = await table.evaluate((element) => element.scrollLeft);
        await page.keyboard.press("ArrowRight");
        const afterArrowRight = await table.evaluate((element) => element.scrollLeft);
        expect(afterArrowRight).toBeGreaterThan(initialScrollLeft);

        const maximumScrollLeft = await table.evaluate(
            (element) => element.scrollWidth - element.clientWidth,
        );
        await page.keyboard.press("End");
        const afterEnd = await table.evaluate((element) => element.scrollLeft);
        expect(afterEnd).toBeCloseTo(maximumScrollLeft, 1);

        await page.keyboard.press("Home");
        await expect
            .poll(() => table.evaluate((element) => element.scrollLeft))
            .toBeCloseTo(0, 1);

        await table.evaluate((element) => {
            const recordPageDown: EventListener = (event) => {
                if (event instanceof KeyboardEvent && event.key === "PageDown") {
                    element.setAttribute("data-page-down-default-prevented", String(event.defaultPrevented));
                    element.removeEventListener("keydown", recordPageDown);
                }
            };
            element.addEventListener("keydown", recordPageDown);
        });
        await page.keyboard.press("PageDown");
        await expect(table).toHaveAttribute("data-page-down-default-prevented", "false");
    });
});

test.describe("shared guide navigation accessibility", () => {
    test("shows skip and continuation links above surrounding surfaces when focused", async ({ page }) => {
        await page.goto("/user-guide/views/pr-flow/");

        await page.keyboard.press("Tab");
        const skipLink = page.getByRole("link", { name: "Skip to content" });
        await expect(skipLink).toBeFocused();
        const skipLinkIsOnTop = await skipLink.evaluate((element) => {
            const bounds = element.getBoundingClientRect();
            const topElement = document.elementFromPoint(
                bounds.left + bounds.width / 2,
                bounds.top + bounds.height / 2,
            );
            return element.contains(topElement);
        });
        expect(skipLinkIsOnTop).toBe(true);

        const continuationLink = page
            .getByRole("navigation", { name: "Continue this documentation path" })
            .getByRole("link", { name: "Next step: Plan capacity" });
        await continuationLink.focus();
        await expect(continuationLink).toBeFocused();
        const focusRing = await continuationLink.evaluate(
            (element) => getComputedStyle(element).boxShadow,
        );
        expect(focusRing).not.toBe("none");
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

});

test.describe("reports and metrics guides accessibility", () => {
    for (const guide of [
        { path: "/user-guide/reports/", title: "Report Center" },
        { path: "/user-guide/metrics-interpretation/", title: "Interpret shared metrics" },
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
