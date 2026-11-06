import { test, expect } from "@playwright/test";
import { spawn } from "node:child_process";
import { once } from "node:events";

for (const codec of ["json", "msgpack", "cbor"]) {
  test(`Python and browser exchange binary data and ownership over ${codec}`, async ({
    page,
  }) => {
    const child = spawn(
      process.env.PYTHON || "python",
      ["-m", "transports.tests.binary_server"],
      { cwd: ".." },
    );
    let output = "",
      errors = "";
    child.stdout.on("data", (chunk) => (output += chunk));
    child.stderr.on("data", (chunk) => (errors += chunk));
    try {
      await expect
        .poll(() => {
          if (child.exitCode !== null) throw new Error(errors);
          return output.trim();
        })
        .toMatch(/^http:/);
      const origin = output.trim();
      await expect
        .poll(async () => {
          try {
            return (await fetch(`${origin}/state`)).status;
          } catch {
            return 0;
          }
        })
        .toBe(200);
      const { id } = await (await fetch(`${origin}/state`)).json();
      await page.goto("/examples/index.html");
      await page.evaluate(
        async ({ origin, codec, id }) => {
          const api = await import("/js/dist/esm/index.js");
          await api.wasm.default({
            module_or_path: "/js/dist/pkg/transports_bg.wasm",
          });
          const client = new api.Client(codec, { updateInterval: 0.01 });
          window.api = api;
          window.client = client;
          window.commands = [];
          client.onCommand((command) => {
            window.commands.push(api.fromValue(command.value));
            client.proposeOps(id, [
              { Set: { path: [{ Key: command.field }], value: command.value } },
            ]);
            client.ackCommand(command);
          });
          window.socket = client.connect(
            `${origin.replace("http:", "ws:")}/ws`,
          );
        },
        { origin, codec, id },
      );
      await expect
        .poll(() => page.evaluate(() => window.commands))
        .toEqual([true, false, true]);
      await expect
        .poll(
          async () => (await (await fetch(`${origin}/state`)).json()).playing,
        )
        .toBe(true);
      const initial = await page.evaluate((id) => {
        const data = window.api.fromValue(window.client.value(id)).data;
        return {
          size: data.length,
          first: data[0],
          last: data[255],
          owned: window.client.owns(id, "playing"),
        };
      }, id);
      expect(initial).toEqual({
        size: 32768,
        first: 0,
        last: 255,
        owned: true,
      });
      await page.evaluate(
        (id) =>
          window.client.proposeOps(id, [
            {
              Set: {
                path: [{ Key: "data" }],
                value: window.api.toValue(new Uint8Array(65536).fill(7)),
              },
            },
          ]),
        id,
      );
      await expect
        .poll(async () => (await (await fetch(`${origin}/state`)).json()).size)
        .toBe(65536);
      await page.evaluate(
        ({ id, origin, codec }) => {
          window.second = new window.api.Client(codec);
          window.secondSocket = window.second.connect(
            `${origin.replace("http:", "ws:")}/ws`,
          );
        },
        { id, origin, codec },
      );
      await expect
        .poll(() =>
          page.evaluate((id) => window.second.value(id) !== undefined, id),
        )
        .toBe(true);
      expect(
        await page.evaluate((id) => window.second.owns(id, "playing"), id),
      ).toBe(false);
      await page.evaluate(() => window.socket.close());
      await expect
        .poll(() =>
          page.evaluate((id) => window.second.owns(id, "playing"), id),
        )
        .toBe(true);
      await page.evaluate(() => window.secondSocket.close());
    } finally {
      if (child.exitCode === null) {
        const stopped = once(child, "exit");
        child.kill();
        await stopped;
      }
    }
  });
}
