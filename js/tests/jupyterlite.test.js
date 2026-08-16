import fs from "fs";
import { test, expect } from "@playwright/test";

const built = fs.existsSync("../dist/lite/repl/index.html");

test("JupyterLite pyodide kernel installs the wheel and hosts a session", async ({
  page,
}) => {
  test.skip(!built, "run `make jupyterlite` first to build dist/lite");
  test.setTimeout(300_000);

  const url = new URL("/dist/lite/repl/index.html", "http://127.0.0.1:3000");
  url.searchParams.set("kernel", "python");
  url.searchParams.append("code", "%pip install -q transports");
  url.searchParams.append(
    "code",
    [
      "import transports",
      "from pydantic import BaseModel",
      "class Device(BaseModel):",
      "    name: str = 'lamp'",
      "session = transports.Session()",
      "mid = session.host(Device())",
      "session.value(mid)",
      "print('lite-ok', transports.__version__, session.snapshot(mid)['rev'])",
    ].join("\n"),
  );
  await page.goto(url.toString());
  await expect(page.getByText(/lite-ok 0\.6\.0 0/)).toBeVisible({
    timeout: 240_000,
  });
});
