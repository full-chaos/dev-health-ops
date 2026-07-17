import { expect, test } from "@playwright/test";

test.describe("first 10 minutes", () => {
    test("follows the onboarding path from Cockpit context to the Investment journey", async ({ page }) => {
        await page.goto("/user-guide/first-10-minutes/");

        const article = page.locator(".md-content");
        await expect(article.getByRole("heading", { name: "Your first 10 minutes" })).toBeVisible();
        await expect(
            article.getByRole("img", { name: /Sanitized fixture-backed Cockpit capture/i }),
        ).toBeVisible();
        await expect(article.getByRole("link", { name: "How to read Dev Health" })).toBeVisible();

        await article.getByRole("link", { name: "How to read Dev Health" }).click();
        await expect(page).toHaveURL(/\/user-guide\/how-to-read-dev-health\/$/);
        await expect(article.getByRole("link", { name: "Glossary" })).toBeVisible();

        await article.getByRole("link", { name: "Investment: follow the evidence" }).click();
        await expect(page).toHaveURL(/\/user-guide\/journeys\/investment-view\/$/);
        await expect(
            article.getByRole("img", { name: /Sanitized fixture-backed Investment availability capture/i }),
        ).toBeVisible();
        await expect(article).toContainText("availability gate");
    });

    test("exposes the fixture source metadata without putting a raw screenshot claim in the copy", async ({ page }) => {
        await page.goto("/user-guide/journeys/investment-view/");

        const article = page.locator(".md-content");
        const metadata = article.getByRole("link", { name: "capture metadata" });
        await expect(metadata).toHaveAttribute("href", /fixture-capture-metadata\.json$/);
        await expect(article).toContainText("sanitized fixture capture");
    });
});
