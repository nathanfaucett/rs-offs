import {
    start,
    Channel as WasmChannel,
    IrohClient as WasmIrohClient,
} from "@aicacia/iroh-client-wasm";

export { start };

export type ClientOptions = {
    relayUrl?: string;
    secretKey?: string;
};

export type TicketOptions = {
    includeSelf?: boolean;
    includeBootstrap?: boolean;
    includeNeighbors?: boolean;
};

export type MessagePayload = string | Uint8Array;

export type PeerSessionOptions = {
    peerId: string;
    target?: string;
};

export type ChannelEvent =
    | { type: "joined"; neighbors: string[] }
    | {
          type: "messageReceived";
          from: string;
          text?: string;
          binary?: number[];
          sentTimestamp: number;
          scope: unknown;
      }
    | { type: "neighborUp"; endpointId: string }
    | { type: "neighborDown"; endpointId: string }
    | { type: "lagged" }
    | { type: "closed"; error?: string | null };

export type PeerSessionEvent =
    | { type: "connected"; peerId: string }
    | {
          type: "messageReceived";
          from: string;
          text?: string;
          binary?: number[];
          sentTimestamp: number;
          scope: unknown;
      }
    | { type: "closed"; error?: string | null };

function toAsyncIterable<T>(stream: ReadableStream<T>): AsyncIterable<T> {
    return {
        async *[Symbol.asyncIterator]() {
            const reader = stream.getReader();
            try {
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) {
                        return;
                    }
                    yield value;
                }
            } finally {
                await reader.cancel().catch(() => undefined);
                reader.releaseLock();
            }
        },
    };
}

export class PeerSession {
    constructor(
        private readonly channel: Channel,
        public readonly peerId: string,
        private readonly options: PeerSessionOptions = { peerId },
    ) {}

    async send(payload: MessagePayload): Promise<void> {
        await this.channel.send(this.peerId, payload);
    }

    async close(): Promise<void> {
        await this.channel.close();
    }

    events(): AsyncIterable<PeerSessionEvent> {
        const peerId = this.peerId;
        const channel = this.channel;
        return {
            async *[Symbol.asyncIterator]() {
                for await (const event of channel.events()) {
                    if (event.type !== "messageReceived") {
                        continue;
                    }
                    if (event.from !== peerId) {
                        continue;
                    }
                    yield {
                        type: "messageReceived",
                        from: event.from,
                        text: event.text,
                        binary: event.binary,
                        sentTimestamp: event.sentTimestamp,
                        scope: event.scope,
                    };
                }
            },
        } as AsyncIterable<PeerSessionEvent>;
    }
}

export class Channel {
    constructor(private readonly inner: WasmChannel) {}

    id(): string {
        return this.inner.id();
    }

    neighbors(): string[] {
        return this.inner.neighbors();
    }

    ticket(options: TicketOptions = {}): string {
        return this.inner.ticket(options as never);
    }

    peerSession(
        peerId: string,
        options: Partial<PeerSessionOptions> = {},
    ): PeerSession {
        if (!peerId || !peerId.trim()) {
            throw new Error("peerSession requires a peer id");
        }
        const normalizedPeerId = peerId.trim();
        return new PeerSession(this, normalizedPeerId, {
            peerId: normalizedPeerId,
            ...options,
        });
    }

    async broadcast(payload: MessagePayload): Promise<void> {
        await this.inner.broadcast(payload as never);
    }

    async broadcastNeighbor(payload: MessagePayload): Promise<void> {
        await this.inner.broadcastNeighbor(payload as never);
    }

    async broadcastNeighbors(payload: MessagePayload): Promise<void> {
        await this.inner.broadcastNeighbors(payload as never);
    }

    async send(target: string, payload: MessagePayload): Promise<void> {
        if (!target || !target.trim()) {
            throw new Error("send requires a peer id or ticket");
        }
        await this.inner.send(target, payload as never);
    }

    events(): AsyncIterable<ChannelEvent> {
        return toAsyncIterable(
            this.inner.events() as ReadableStream<ChannelEvent>,
        );
    }

    async close(): Promise<void> {
        await this.inner.close();
    }
}

export class IrohClient {
    constructor(private readonly inner: WasmIrohClient) {}

    static async create(options: ClientOptions = {}): Promise<IrohClient> {
        const inner = await WasmIrohClient.create(options as never);
        return new IrohClient(inner);
    }

    endpointId(): string {
        return this.inner.endpointId();
    }

    async createChannel(): Promise<Channel> {
        return new Channel(await this.inner.createChannel());
    }

    async joinChannel(ticket: string): Promise<Channel> {
        return new Channel(await this.inner.joinChannel(ticket));
    }

    async connectPeer(
        peerId: string,
        options: Partial<PeerSessionOptions> = {},
    ): Promise<PeerSession> {
        if (!peerId || !peerId.trim()) {
            throw new Error("connectPeer requires a peer id");
        }
        return this.createChannel().then((channel) =>
            channel.peerSession(peerId.trim(), options),
        );
    }

    async shutdown(): Promise<void> {
        await this.inner.shutdown();
    }
}

export { WasmChannel };
