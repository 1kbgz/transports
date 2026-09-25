import { expect, test } from "@playwright/test";

test("Client exchanges frames over a browser RTCDataChannel", async ({
  page,
}) => {
  await page.goto("/");
  const result = await page.evaluate(async () => {
    const transports = await import("/js/dist/esm/index.js");
    await transports.wasm.default("/js/dist/pkg/transports_bg.wasm");

    const gather = async (connection) => {
      if (connection.iceGatheringState === "complete") return;
      await new Promise((resolve) => {
        connection.addEventListener("icegatheringstatechange", () => {
          if (connection.iceGatheringState === "complete") resolve();
        });
      });
    };
    const opened = (channel) =>
      channel.readyState === "open"
        ? Promise.resolve()
        : new Promise((resolve) =>
            channel.addEventListener("open", resolve, { once: true }),
          );

    const left = new RTCPeerConnection();
    const right = new RTCPeerConnection();
    try {
      const remoteChannel = new Promise((resolve) =>
        right.addEventListener(
          "datachannel",
          (event) => resolve(event.channel),
          {
            once: true,
          },
        ),
      );
      const local = left.createDataChannel("transports");
      const client = new transports.Client();
      client.connectDataChannel(local);

      await left.setLocalDescription(await left.createOffer());
      await gather(left);
      await right.setRemoteDescription(left.localDescription);
      await right.setLocalDescription(await right.createAnswer());
      await gather(right);
      await left.setRemoteDescription(right.localDescription);

      const remote = await remoteChannel;
      await Promise.all([opened(local), opened(remote)]);
      remote.send(
        JSON.stringify({
          t: "snapshot",
          id: 32,
          type: "Counter",
          rev: 0,
          value: transports.toValue({ tick: 2 }),
        }),
      );
      while (client.value(32) === undefined)
        await new Promise((resolve) => setTimeout(resolve, 0));

      const received = new Promise((resolve) =>
        remote.addEventListener("message", (event) => resolve(event.data), {
          once: true,
        }),
      );
      const sent = client.send("outbound");
      const frame = await received;
      local.close();
      while (client.connected)
        await new Promise((resolve) => setTimeout(resolve, 0));

      return {
        connected: sent,
        value: transports.fromValue(client.value(32)),
        frame,
        disconnected: !client.connected,
      };
    } finally {
      left.close();
      right.close();
    }
  });

  expect(result).toEqual({
    connected: true,
    value: { tick: 2 },
    frame: "outbound",
    disconnected: true,
  });
});
