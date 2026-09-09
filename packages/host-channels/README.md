# @gino/host-channels

Distribution and routing over `@gino/host-core`: get a task's output out to an
external system, and route replies from that system back into a task.

## When you need it

When something outside the host needs to talk to a task — a chat integration, a
ticket system, a bot. If you are consuming inside your own process, subscribe to
`host.events` and read `host.transcript` instead; that is the primary model and
needs nothing from this package. Channels are a separate install on purpose.

Egress tails the transcript by a per-channel cursor and delivers; ingress maps an
external conversation to a task, continuing it or opening a new one. Channels
never write the transcript.

## Entry points

One entry, `@gino/host-channels`. Register a channel, then bind conversations to
tasks as they arrive:

```ts
import { registerChannel, routeInbound } from "@gino/host-channels";

const channel = registerChannel(host.storage, { name: "support", type: "slack", ownerId: "alice" });

const routed = routeInbound(host.storage, host.sessions, channel, {
  conversationRef: "C123/thread-9",
  text: "the deploy failed again",
}, host.events);
// routed.created tells you whether this opened a new task
```

**Pull delivery** — poll, then confirm receipt by echoing the cursor back. The
read never advances the stored cursor, so an unconfirmed batch is redelivered:

```ts
import { ackCursor, pollChannel } from "@gino/host-channels";

const deliveries = pollChannel(host.storage, channel, { limit: 100 });
for (const d of deliveries) {
  await send(d.messages);
  ackCursor(host.storage, channel.id, d.taskId, d.cursor);
}
```

**Push delivery** — register a webhook and run the pump, which subscribes to
`transcript.message` and retries with a bounded attempt count:

```ts
import { addWebhook, startWebhookPump } from "@gino/host-channels";

addWebhook(host.storage, channel.id, { url: "https://example.test/hook", secret: "…" });
const pump = startWebhookPump({ storage: host.storage, events: host.events, logger: host.logger });
host.registerService(pump); // stopped by host.stop()
```

Webhook URLs are SSRF-guarded at registration and re-checked against the
resolved address at delivery.

## What it does not do

- **It never writes the transcript.** Storage and distribution stay separate: it
  reads messages by cursor and writes only its own channel tables.
- **No delivery guarantee beyond at-least-once.** Deduplicate downstream.
- **No transport clients.** There is no Slack or Discord SDK here — you receive
  the rows and send them yourself, or you receive a webhook.
- **No storage of its own.** It needs a `Storage` whose optional `channels`
  repository is present.
