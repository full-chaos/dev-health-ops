import { expect, test } from "@playwright/test";

const showcasePath = "/reference/primitive-showcase/";

test.describe("primitive showcase", () => {
  test("keeps a sequential heading outline and visible keyboard focus", async ({ page }) => {
    await page.goto(showcasePath);

    const headingLevels = await page.locator("h1, h2, h3, h4, h5, h6").evaluateAll((headings) =>
      headings.map((heading) => Number(heading.tagName.slice(1))),
    );

    expect(headingLevels[0]).toBe(1);
    expect(
      headingLevels.every((level, index) => {
        const previousLevel = index === 0 ? undefined : headingLevels[index - 1];
        return previousLevel === undefined || level - previousLevel <= 1;
      }),
    ).toBeTruthy();

    await page.keyboard.press("Tab");
    await expect(page.getByRole("link", { name: "Skip to content" })).toHaveCSS("opacity", "1");
    const focus = await page.evaluate(() => {
      const activeElement = document.activeElement;
      if (!(activeElement instanceof HTMLElement)) {
        return { hasFocus: false, visibleFocus: false };
      }

      const styles = getComputedStyle(activeElement);
      return {
        hasFocus: true,
        visibleFocus:
          styles.outlineStyle !== "none" || styles.boxShadow !== "none",
      };
    });

    expect(focus).toEqual({ hasFocus: true, visibleFocus: true });
  });

  test("gives action CTAs animated motion and an explicit keyboard focus ring", async ({ page }) => {
    await page.goto(showcasePath);

    const action = page.locator(".fc-action").first();
    await expect(action).toBeVisible();
    await expect(action).toHaveCSS("transition-property", /transform/);

    const restTransform = await action.evaluate((element) => getComputedStyle(element).transform);
    await action.hover();
    await expect(action).not.toHaveCSS("transform", restTransform);

    await action.focus();
    await expect(action).toHaveCSS("box-shadow", /rgb/);

    await page.emulateMedia({ reducedMotion: "reduce" });
    await expect(action).toHaveCSS("transition-duration", "0s");
  });

  for (const viewport of [
    { name: "mobile", width: 375, height: 900 },
    { name: "tablet", width: 768, height: 900 },
    { name: "desktop", width: 1280, height: 900 },
  ] as const) {
    test(`renders without horizontal overflow at ${viewport.name}`, async ({ page }, testInfo) => {
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto(showcasePath);

      const dimensions = await page.evaluate(() => ({
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: document.documentElement.scrollWidth,
      }));

      expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
      await page.screenshot({ path: testInfo.outputPath(`primitive-showcase-${viewport.name}.png`), fullPage: true });
    });
  }
});
