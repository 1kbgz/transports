import { test, expect } from "@playwright/test";

const wheel = process.env.TRANSPORTS_PYODIDE_WHEEL;

test("Pyodide wheel runs the transports round trip in a browser", async ({
  page,
}) => {
  test.skip(!wheel, "set TRANSPORTS_PYODIDE_WHEEL to test the browser wheel");
  test.setTimeout(180_000);

  const url = new URL("/examples/pyodide.html", "http://127.0.0.1:3000");
  url.searchParams.set("wheel", wheel);
  await page.goto(url.toString());

  await expect(page.locator("html")).toHaveAttribute("data-ready", "true", {
    timeout: 150_000,
  });
  await expect(page.locator("#name")).toHaveText("workbench");
  await expect(page.locator("#enabled")).toHaveText("no");

  await page.locator("#server").click();
  await expect(page.locator("#enabled")).toHaveText("yes");

  await page.locator("#client").click();
  await expect(page.locator("#name")).toHaveText("browser");
});
