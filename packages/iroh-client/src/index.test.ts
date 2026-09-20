import { describe, expect, it } from "vitest";

import { Channel } from "./index";

describe("PeerSession", () => {
    it("sends directly to the target peer and filters events to that peer", async () => {
        const sent: { target: string; payload: unknown }[] = [];
        const stream = new ReadableStream({
            start(controller) {
                controller.enqueue({
                    type: "messageReceived",
                    from: "peer-1",
                    text: "hello",
                    sentTimestamp: 1,
                    scope: { kind: "direct" },
                });
                controller.enqueue({
                    type: "messageReceived",
                    from: "peer-2",
                    text: "ignore me",
                    sentTimestamp: 2,
                    scope: { kind: "direct" },
                });
                controller.close();
            },
        });

        const channel = new Channel({
            id: () => "channel-1",
            neighbors: () => ["peer-1", "peer-2"],
            ticket: () => "ticket",
            broadcast: async () => undefined,
            broadcastNeighbor: async () => undefined,
            broadcastNeighbors: async () => undefined,
            send: async (target: string, payload: unknown) => {
                sent.push({ target, payload });
            },
            events: () => stream,
            close: async () => undefined,
        } as never);

        const session = channel.peerSession("peer-1");

        await session.send("hello");

        const events: unknown[] = [];
        for await (const event of session.events()) {
            events.push(event);
        }

        expect(sent).toEqual([{ target: "peer-1", payload: "hello" }]);
        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({
            type: "messageReceived",
            from: "peer-1",
            text: "hello",
        });
    });
});
