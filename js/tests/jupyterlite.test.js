import fs from "fs";
import { test, expect } from "@playwright/test";

const built = fs.existsSync("../dist/lite/repl/index.html");
// the repo version (bumpversion keeps package.json in sync): asserting it proves the lite site
// serves the freshly built wheel, not a stale one — without hardcoding a version that rots on bump
const { version } = JSON.parse(fs.readFileSync("./package.json", "utf8"));

test("JupyterLite pyodide kernel installs the wheel and hosts a session", async ({
  page,
}) => {
  test.skip(!built, "run `make jupyterlite` first to build dist/lite");
  test.setTimeout(300_000);

  const url = new URL("/dist/lite/repl/index.html", "http://127.0.0.1:3000");
  url.searchParams.set("kernel", "python");
  url.searchParams.append("code", "%pip install -q transports anywidget");
  url.searchParams.append(
    "code",
    [
      "import transports",
      "from pydantic import BaseModel",
      "class Device(BaseModel):",
      "    name: str = 'lamp'",
      "session = transports.Session()",
      "mid = session.host(Device())",
      "server = transports.Server(session)",
      // the widget factory reads its frontend module out of the installed wheel — this is the call
      // that fails when the wheel ships without transports/extension
      "w = transports.widget(server)",
      "assert 'msg:custom' in w._esm",
      "print('lite-ok', transports.__version__, session.snapshot(mid)['rev'], 'widget-ok')",
    ].join("\n"),
  );
  await page.goto(url.toString());
  await expect(page.getByText(`lite-ok ${version} 0 widget-ok`)).toBeVisible({
    timeout: 240_000,
  });
});

test("JupyterLite pip works without WebAssembly JSPI", async ({ page }) => {
  // browsers without JS Promise Integration (older Safari/Firefox/Chrome) must still be able to
  // %pip install — delete the JSPI globals before pyodide loads so its feature detection sees none,
  // guarding against a kernel/pyodide upgrade that makes pip require stack switching
  test.skip(!built, "run `make jupyterlite` first to build dist/lite");
  test.setTimeout(300_000);
  await page.addInitScript(() => {
    delete WebAssembly.Suspending;
    delete WebAssembly.promising;
  });

  const url = new URL("/dist/lite/repl/index.html", "http://127.0.0.1:3000");
  url.searchParams.set("kernel", "python");
  url.searchParams.append("code", "%pip install -q transports");
  url.searchParams.append(
    "code",
    "import transports\nprint('nojspi-ok', 'v=' + transports.__version__)",
  );
  await page.goto(url.toString());
  await expect(page.getByText(`nojspi-ok v=${version}`)).toBeVisible({
    timeout: 240_000,
  });
});
