import { expect, test } from "@playwright/test";

test.describe("first 10 minutes", () => {
    test("follows the onboarding path from Cockpit context to the Investment journey", async ({ page }) => {
        await page.goto("/user-guide/first-10-minutes/");

        const article = page.locator(".md-content");
        await expect(article.getByRole("heading", { name: "Your first 10 minutes" })).toBeVisible();
        await expect(
            article.getByRole("img", { name: /Sanitized fixture-backed Cockpit capture/i }),
        ).toBeVisible();
        await expect(article).toContainText(
            "populated Cockpit signals alongside a source-connection prompt",
        );
        await expect(article.getByRole("link", { name: "How to read Dev Health" })).toBeVisible();

        await article.getByRole("link", { name: "How to read Dev Health" }).click();
        await expect(page).toHaveURL(/\/user-guide\/how-to-read-dev-health\/$/);
        await expect(article.getByRole("link", { name: "Glossary" })).toBeVisible();

        await article.getByRole("link", { name: "Investment: follow the evidence" }).click();
        await expect(page).toHaveURL(/\/user-guide\/journeys\/investment-view\/$/);
        await expect(
            article.getByRole("img", { name: /Sanitized fixture-backed Investment availability capture/i }),
        ).toBeVisible();
        await expect(article).toContainText("Team-plan availability gate");
    });

    test("exposes the fixture source metadata without putting a raw screenshot claim in the copy", async ({ page }) => {
        await page.goto("/user-guide/journeys/investment-view/");

        const article = page.locator(".md-content");
        const metadata = article.getByRole("link", { name: "capture metadata" });
        await expect(metadata).toHaveAttribute("href", /fixture-capture-metadata\.json$/);
        await expect(article).toContainText("sanitized fixture capture");
    });
});

test.describe("diagnostic guides", () => {
    const diagnosticGuides = [
        { path: "/user-guide/views/quadrants/", title: "Quadrants" },
        { path: "/user-guide/views/flame-diagrams/", title: "Flame diagrams" },
        { path: "/user-guide/views/code-hotspots/", title: "Code Hotspots" },
    ] as const;

    test("opens the plain-language guides with evidence and glossary paths", async ({ page }) => {
        for (const guide of diagnosticGuides) {
            await page.goto(guide.path);

            const article = page.locator(".md-content");
            await expect(article.getByRole("heading", { name: guide.title })).toBeVisible();
            await expect(article).toContainText("Evidence path");
            await expect(article.getByRole("link", { name: /glossary/i })).toBeVisible();
            await expect(article.getByRole("link", { name: /evidence model/i })).toBeVisible();
        }
    });

});

test.describe("flow and planning guides", () => {
    test("follows PR Flow through capacity planning to work relationships", async ({ page }) => {
        await page.goto("/user-guide/views/pr-flow/");

        const article = page.locator(".md-content");
        await expect(article.getByRole("heading", { name: "PR Flow" })).toBeVisible();
        await expect(article).toContainText("work-item state-transition Sankey");
        await expect(article).not.toContainText("review latency");
        await expect(article).toContainText("Current behavior");
        await expect(article).toContainText("Planned behavior");

        await article.getByRole("link", { name: "Next step: Plan capacity", exact: true }).click();
        await expect(page).toHaveURL(/\/user-guide\/views\/capacity-planning\/$/);
        await expect(article.getByRole("heading", { name: "Capacity Planning View" })).toBeVisible();
        await expect(article).toContainText("backlog");
        await expect(article).toContainText("historical throughput");

        await article
            .getByRole("link", { name: "Next step: Follow work relationships", exact: true })
            .click();
        await expect(page).toHaveURL(/\/user-guide\/views\/work-graph\/$/);
        await expect(article.getByRole("heading", { name: "Work Graph: follow relationships" })).toBeVisible();
        await expect(article).toContainText("Theme → Subcategory → Evidence");
    });

    test("keeps evidence and glossary routes available for each guide", async ({ page }) => {
        for (const guide of [
            "/user-guide/views/pr-flow/",
            "/user-guide/views/capacity-planning/",
            "/user-guide/views/work-graph/",
        ] as const) {
            await page.goto(guide);

            const article = page.locator(".md-content");
            await expect(article.getByRole("link", { name: /evidence model/i })).toBeVisible();
            await expect(article.getByRole("link", { name: /glossary/i })).toBeVisible();
        }
    });
});

test.describe("AI view guides", () => {
    test("opens every guide with calibrated language and interpretation routes", async ({ page }) => {
        for (const guide of [
            { path: "/user-guide/views/ai-impact/", title: "AI Impact", field: "AI-assisted work share" },
            { path: "/user-guide/views/ai-review-load/", title: "AI Review Load", field: "Pickup latency" },
            { path: "/user-guide/views/ai-risk/", title: "AI Risk", field: "Rework rate" },
            { path: "/user-guide/views/ai-attribution/", title: "AI Attribution", field: "Attribution mix" },
        ] as const) {
            await page.goto(guide.path);

            const article = page.locator(".md-content");
            await expect(article.getByRole("heading", { name: guide.title })).toBeVisible();
            await expect(article).toContainText(guide.field);
            await expect(article).toContainText("appears");
            await expect(article.getByRole("link", { name: "How to read Dev Health" })).toBeVisible();
            await expect(article.getByRole("link", { name: "Glossary" })).toBeVisible();
        }
    });

    test("keeps the in-flow evidence trail keyboard reachable", async ({ page }) => {
        for (const guide of [
            "/user-guide/views/ai-impact/",
            "/user-guide/views/ai-review-load/",
            "/user-guide/views/ai-risk/",
            "/user-guide/views/ai-attribution/",
        ] as const) {
            await page.setViewportSize({ width: 375, height: 900 });
            await page.goto(guide);

            const evidenceLink = page
                .getByRole("complementary", { name: "Evidence trail" })
                .getByRole("link", { name: "Open the evidence model" });
            await evidenceLink.focus();
            await expect(evidenceLink).toBeFocused();
            await page.keyboard.press("Enter");
            await expect(page).toHaveURL(/\/user-guide\/how-to-read-dev-health\/$/);
        }
    });
});

test.describe("Report Center guide", () => {
    test("follows report creation, review, and metric interpretation in a real browser", async ({ page }) => {
        await page.goto("/user-guide/reports/");

        const article = page.locator(".md-content");
        await expect(article.getByRole("heading", { name: "Report Center" })).toBeVisible();
        await expect(article).toContainText("New report");
        await expect(article).toContainText("Clone");
        await expect(article).toContainText("None");
        await expect(article).toContainText("Weekly");
        await expect(article).toContainText("Monthly");
        await expect(article).toContainText("Run Now");
        await expect(article).toContainText("Rendered Markdown");
        await expect(article).toContainText("does not show a separate provenance panel");
        await expect(article).not.toContainText("cron");
        await expect(article).not.toContainText("timezone");
        await expect(article).toContainText("AI-generated");

        await article.getByRole("link", { name: "Interpret shared metrics" }).first().click();
        await expect(page).toHaveURL(/\/user-guide\/metrics-interpretation\/$/);
        await expect(article.getByRole("heading", { name: "Interpret shared metrics" })).toBeVisible();
        await expect(article).toContainText("Cycle time");
        await expect(article).toContainText("at least 80%");
        await expect(article).toContainText("does not mean zero");
    });
});
