# RabbitMQ Architecture & Usage Reference

> **Scope:** Production-oriented RabbitMQ architecture and best-practices reference implementation.
> Some production concerns — cluster configuration, observability pipelines, persistence strategy,
> publisher confirms, and operational monitoring — depend on your specific deployment environment
> and are not fully covered here.

## Table of Contents
1. [Connection & Channel Model](#1-connection--channel-model)
2. [Root `src/` Services](#2-root-src-services)
3. [Exchange Services](#3-exchange-services)
4. [Handlers](#4-handlers)
5. [Microservices (`services/`)](#5-microservices-services)
6. [End-to-End Flow: Order Lifecycle](#6-end-to-end-flow-order-lifecycle)
7. [Why This Way? Alternatives & Problems](#7-why-this-way-alternatives--problems)
8. [npm Scripts Quick Reference](#8-npm-scripts-quick-reference)

---

## 1. Connection & Channel Model

### The AMQP Connection/Channel Hierarchy

```
TCP Connection (1 per process)
  └── Channel 1  (topology setup — closed after use)
  └── Channel 2  (publisher: direct)
  └── Channel 3  (publisher: fanout)
  └── Channel 4  (consumer: order.processing)
  └── Channel 5  (consumer: order.payment)
  └── ...one channel per publisher/consumer
```

### `ConnectionManager` — `src/infrastructure/connection.manager.ts`

**What it is:** Singleton that owns the ONE TCP connection to the RabbitMQ broker.

| Method | Called by | Purpose |
|---|---|---|
| `ConnectionManager.getInstance()` | `src/index.ts`, `ServiceBase` | Get/create the singleton |
| `getChannel()` | `MessagingFactory` | Create a new AMQP channel on the shared connection |
| `close()` | `RabbitMQFacade.shutdown()`, `ServiceBase` shutdown handler | Gracefully close the TCP connection |

**How it is called:**
```ts
const connectionManager = ConnectionManager.getInstance();
// internally calls: amqplib.connect(url, { heartbeat: 60 })
```

**Why singleton?** TCP connections to RabbitMQ are expensive (socket + TLS handshake). One connection per process is the AMQP best practice. Channels are multiplexed over that one connection.

**Reconnect logic:** On `close` or `error` events, `scheduleReconnect()` retries with exponential backoff up to 5 times (2s, 4s, 8s, 16s, 32s).

**What breaks if done wrong:**
- Creating a new connection per publish → exhausts broker file descriptors, crashes broker
- Sharing one channel between publisher and consumer → amqplib is NOT channel-thread-safe; messages get corrupted
- No heartbeat → broker silently drops idle connections after ~60s

---

### `MessagingFactory` — `src/messaging/messaging.factory.ts`

**What it is:** Abstract Factory that creates Publisher and Consumer instances, each backed by their own dedicated channel.

| Method | Returns | Used by |
|---|---|---|
| `createPublisher()` | `IPublisher` (backed by a new channel) | `RabbitMQFacade.initialize()`, each service |
| `createConsumer()` | `IConsumer` (backed by a new channel) | `RabbitMQFacade.startConsumers()`, each service |
| `createChannel()` | raw `Channel` | `TopologySetup`, advanced service use |

**Why one channel per publisher/consumer?**
Node.js is single-threaded, so there are no concurrent threads racing on a channel. The issue is subtler: amqplib channels carry internal async state (pending confirms, consumer tags, prefetch counters). Interleaving unrelated publish and consume lifecycles on the same channel — especially around back-pressure and `drain` events — can cause unexpected frame ordering and makes reasoning about failures much harder. Giving each long-lived publisher and consumer its own dedicated channel keeps their state completely independent and aligns with the amqplib documentation recommendation.

---

## 2. Root `src/` Services

### `src/infrastructure/logger.ts`

**What it is:** A Winston structured logger, exported as a singleton `logger`.

**Produces:** Console log lines in format `[YYYY-MM-DD HH:mm:ss] level: message {meta}`.

**Used by:** Every file in the project — imported as `import { logger } from '../infrastructure/logger'`.

**Why not `console.log`?** Winston adds timestamps, log levels, colorization, and metadata serialization. In production, transports can be swapped to ship logs to Datadog/CloudWatch without changing callers.

---

### `src/config/rabbitmq.config.ts`

**What it is:** Centralized connection configuration read from `.env`.

| Field | Default | Purpose |
|---|---|---|
| `url` | `amqp://guest:guest@localhost:5672` | Broker address |
| `heartbeat` | `60` | Seconds between AMQP heartbeats |
| `prefetch` | `10` | Global channel prefetch (overridden per-consumer) |

**Used by:** `ConnectionManager` only — nobody else touches raw connection config.

---

### `src/config/exchanges.config.ts`

**What it is:** The single source of truth for all exchange names, queue names, routing keys, and RPC queue names.

**Produces:** Typed constants (`ExchangeNames`, `QueueNames`, `DirectRoutingKeys`, `TopicRoutingKeys`, `RpcQueues`).

**Used by:** Every exchange service, every topology setup, every consumer and handler — they import queue/exchange names from here.

**Why?** If you rename a queue in two places independently, consumers and publishers silently disconnect. One canonical file = compile-time safety.

```ts
// What you use everywhere:
ExchangeNames.DIRECT          // 'app.direct'
QueueNames.ORDER_PROCESSING   // 'queue.order.processing'
DirectRoutingKeys.ORDER_CREATED // 'order.created'
RpcQueues.INVENTORY_CHECK     // 'rpc.inventory.check'
```

---

### `src/messaging/message.envelope.ts`

**What it is:** The standard wrapper around every message payload published to the broker.

```ts
interface MessageEnvelope<T> {
  messageId: string;    // UUID — for idempotency/deduplication
  timestamp: string;    // ISO-8601
  eventType: string;    // e.g. "OrderCreated"
  payload: T;           // The actual business data
  correlationId?: string; // For distributed tracing
}
```

**How it is created:**
```ts
const envelope = createEnvelope('OrderCreated', payload, correlationId);
// → { messageId: uuid(), timestamp: now, eventType, payload, correlationId }
```

**Consumed by:** Every handler receives `MessageEnvelope<T>` after `Consumer` parses the raw buffer.

**Why an envelope?** Without it, consumers get a raw buffer with no way to know what type the message is, when it was sent, or how to trace it. The `messageId` enables idempotency (detect duplicate deliveries).

---

### `src/messaging/publisher.ts`

**What it is:** The concrete implementation of `IPublisher` that wraps `channel.publish()`.

**How it is called:**
```ts
await publisher.publish(
  ExchangeNames.DIRECT,    // which exchange
  'order.created',          // routing key
  envelope,                 // MessageEnvelope
  { persistent: true }      // options
);
```

**What it does internally:**
- Serializes envelope to JSON buffer
- Sets `persistent: true` (messages survive broker restart)
- Sets `contentType: 'application/json'`
- Detects back-pressure: if `channel.publish()` returns `false`, awaits the `drain` event before continuing

**Produces:** Messages onto the RabbitMQ exchange.

**Why `persistent: true`?** Without it, messages live only in memory. If the broker restarts (or crashes), all unacked messages are lost.

---

### `src/messaging/consumer.ts`

**What it is:** Production-oriented consumer wrapping `channel.consume()`.

**How it is called:**
```ts
await consumer.consume(
  QueueNames.ORDER_PROCESSING,  // queue name
  handleOrderCreated,            // handler function
  { prefetch: 5, maxRetries: 3 }
);
```

**What it does internally:**
1. Sets per-consumer `prefetch` (limits unacked messages in flight)
2. Registers `channel.consume()` callback
3. Parses raw buffer → `MessageEnvelope<T>`
4. Calls handler
5. On success: `channel.ack(msg)` — removes from queue
6. On handler error: reads `x-death` header to count retries
   - If `retryCount < maxRetries`: `channel.nack(msg, false, true)` — requeues
   - If exceeded: `channel.nack(msg, false, false)` — routes to DLQ

**Why manual ack?** Auto-ack removes the message from the queue the moment it is delivered, even if your handler crashes. Manual ack guarantees at-least-once delivery.

---

### `src/messaging/rpc.ts`

**What it is:** Request/Reply over RabbitMQ using the built-in `amq.rabbitmq.reply-to` pseudo-queue.

**`RpcClient` — used by OrderService:**
```ts
const result = await rpcClient.call<InventoryCheckRequest, InventoryCheckResponse>(
  RpcQueues.INVENTORY_CHECK,  // server queue
  { items }                    // request payload
);
// blocks (Promise) until reply arrives or 8000ms timeout
```

**`RpcServer` — used by InventoryService:**
```ts
await rpcServer.listen<InventoryCheckRequest, InventoryCheckResponse>(
  RpcQueues.INVENTORY_CHECK,
  async (req) => {
    // compute and return response
    return { available: true, unavailableSkus: [] };
  }
);
```

**How the reply works:**
1. Client publishes to `rpc.inventory.check` with `correlationId` + `replyTo: amq.rabbitmq.reply-to`
2. Server processes, publishes response to `msg.properties.replyTo` with same `correlationId`
3. Client's pending map resolves the Promise matching the `correlationId`
4. If no reply in `timeoutMs` → Promise rejects

**Why `amq.rabbitmq.reply-to`?** Using this built-in pseudo-queue avoids creating exclusive reply queues per client call — zero queue setup overhead, exists per-connection automatically.

---

### `src/topology/topology.builder.ts`

**What it is:** Low-level builder wrapping raw amqplib topology calls behind intent-revealing methods.

| Method | Wraps | Purpose |
|---|---|---|
| `assertExchange(name, type)` | `channel.assertExchange` | Create/verify exchange (durable, no autoDelete) |
| `assertQueue(name, opts)` | `channel.assertQueue` | Create/verify queue (durable) |
| `assertDeadLetterSetup(queue)` | multiple calls | Creates `queue.dlx` exchange + `queue.dlq` queue, returns DLX name |
| `bindQueue(queue, exchange, key)` | `channel.bindQueue` | Bind queue to exchange with routing key |
| `bindQueueWithHeaders(queue, exchange, headers)` | `channel.bindQueue` with args | Bind using header-matching |

**Why a builder?** Raw amqplib calls are verbose and error-prone. The builder ensures `durable: true` and `autoDelete: false` everywhere, and provides a single place to enforce topology best practices.

---

### `src/topology/topology.setup.ts`

**What it is:** Orchestrates the complete broker topology via `TopologyBuilder`. Called once at startup.

```ts
await topology.setup();
// Internally calls:
//   setupDirectExchange()  → exchange + 3 queues + 3 DLX/DLQ pairs + bindings
//   setupFanoutExchange()  → exchange + 3 queues + bindings
//   setupTopicExchange()   → exchange + 4 queues + wildcard bindings
//   setupHeadersExchange() → exchange + 3 queues + header bindings
```

**Why run on every boot?** `assertExchange` and `assertQueue` are idempotent — they verify-or-create. Running them every startup ensures the topology is always in the correct state, even after broker restarts or new deployments.

---

### `src/facade/rabbitmq.facade.ts`

**What it is:** The single entry point to the entire messaging subsystem. Application code only ever touches this.

**Lifecycle:**
```ts
const mq = new RabbitMQFacade(connectionManager);
await mq.initialize();     // 1. sets up topology, 2. creates publishers
await mq.startConsumers(); // 3. registers all consumers
// ... use mq.direct, mq.fanout, mq.topic, mq.headers
await mq.shutdown();       // closes connection
```

**Public surface:**
```ts
mq.direct.publishOrderCreated(payload)
mq.fanout.broadcastUserRegistered(userId, email)
mq.topic.publishLog({ level: 'error', service: 'x', message: 'y' })
mq.headers.publishHighPriorityTask(payload, region)
```

**Why a Facade?** Without it, every caller would need to know about channels, topology, publishers, and consumer registration. The Facade hides all bootstrapping complexity and provides a clean, stable API surface.

---

## 3. Exchange Services

### `DirectExchangeService` — `src/exchanges/direct/`

**Exchange:** `app.direct` | **Type:** direct (exact routing key match)

| Method | Routing Key | Goes to Queue |
|---|---|---|
| `publishOrderCreated(payload)` | `order.created` | `queue.order.processing` |
| `publishOrderPaid(payload)` | `order.paid` | `queue.order.payment` |
| `publishOrderCancelled(payload)` | `order.cancelled` | `queue.order.cancellation` |

**When to use Direct:** When exactly ONE consumer type should handle the message. One routing key → one queue.

---

### `FanoutExchangeService` — `src/exchanges/fanout/`

**Exchange:** `app.fanout` | **Type:** fanout (broadcast, routing key ignored)

| Method | Goes to ALL queues |
|---|---|
| `broadcastUserRegistered(userId, email)` | `queue.fanout.analytics`, `queue.fanout.audit`, `queue.fanout.notification` |
| `broadcastCacheInvalidation(keys)` | same 3 queues |
| `broadcastSystemEvent(payload)` | same 3 queues |

**When to use Fanout:** When every subscriber must get a copy — analytics, audit, and notifications all need to react to the same event independently.

---

### `TopicExchangeService` — `src/exchanges/topic/`

**Exchange:** `app.topic` | **Type:** topic (wildcard routing key matching)

| Method | Routing Key | Matched by queues |
|---|---|---|
| `publishLog({ level: 'error', service: 'x' })` | `log.error.x` | `queue.topic.log.error` (pattern: `log.error.*`) |
| `publishAudit({ resource: 'payment', action: 'completed' })` | `audit.payment.completed` | `queue.topic.audit` (pattern: `audit.#`) |
| `publishNotification({ channel: 'email', templateId: 'welcome' })` | `notification.email.welcome` | `queue.topic.notification.email` (pattern: `notification.email.*`) |

**Wildcards:**
- `*` = exactly one word
- `#` = zero or more words

**When to use Topic:** When you need pattern-based fan-in/fan-out. A single `log.error.payment-service` message reaches both the error-log queue AND (if you had one) an all-logs queue.

---

### `HeadersExchangeService` — `src/exchanges/headers/`

**Exchange:** `app.headers` | **Type:** headers (routes on message headers, not routing key)

| Method | Headers sent | Queue matched |
|---|---|---|
| `publishHighPriorityTask(payload, region)` | `{ priority:'high', format:'json', region }` | `queue.headers.priority.high` (x-match: all, needs priority+format) |
| `publishLowPriorityTask(payload)` | `{ priority:'low', format:'json' }` | `queue.headers.priority.low` (x-match: all, needs priority) |
| `publishRegionalMessage(payload, region)` | `{ region, priority, format:'json' }` | `queue.headers.region.us` (x-match: any, needs region=us) |

**When to use Headers:** When routing decisions depend on message attributes (priority, region, content format) rather than a topic hierarchy.

---

## 4. Handlers

Each handler is a pure async function — the **Strategy pattern** applied to message processing.

| File | Handles Queue | Does |
|---|---|---|
| `handlers/direct/order.handlers.ts` | `queue.order.processing`, `.payment`, `.cancellation` | Simulate DB persist, payment record, cancellation |
| `handlers/fanout/fanout.handlers.ts` | `queue.fanout.analytics/audit/notification` | Analytics tracking, audit logging, push notification |
| `handlers/topic/topic.handlers.ts` | `queue.topic.audit`, `log.error`, `notification.email/sms` | Audit trail, on-call alerts, email/SMS dispatch |
| `handlers/headers/headers.handlers.ts` | `queue.headers.priority.high/low`, `region.us` | Fast-lane processing, background batch, US residency |

**Handler signature:**
```ts
async function handleOrderCreated(
  envelope: MessageEnvelope<OrderCreatedPayload>,
  rawMsg: ConsumeMessage,   // access raw headers, properties if needed
): Promise<void>
```

---

## 5. Microservices (`services/`)

### `ServiceBase` — `services/shared/service.base.ts`

Abstract base class all microservices extend. **Template Method pattern.**

```
start()
  ├─ create MessagingFactory (via ConnectionManager singleton)
  ├─ create topology channel → TopologySetup.setup() → close channel
  ├─ initialize()  ← subclass hook
  └─ registerShutdownHandlers() (SIGTERM, SIGINT, uncaughtException)
```

Every service gets connection, topology, and graceful shutdown for free.

---

### `OrderService` — Producer + RPC Client

**Produces:**
- `app.direct` / `order.created` → PaymentService + InventoryService
- `app.fanout` / `""` → AuditService + NotificationService

**Consumes:** Nothing (pure producer in this flow)

**RPC calls:** `rpc.inventory.check` → InventoryService (sync stock check before publishing)

**Flow per order:**
```
createOrder()
  1. RPC → InventoryService.checkStock() — blocks until response
  2. If available:
     a. Publish Direct: OrderCreated → queue.order.processing & queue.order.payment
     b. Publish Fanout: OrderCreated → all 3 fanout queues
  3. If not available: log and return (no event published)
```

---

### `InventoryService` — RPC Server + Consumer + Publisher

**Consumes:**
- RPC queue: `rpc.inventory.check` (sync — replies immediately)
- Direct queue: `queue.order.processing` (async — reserves stock)

**Produces:**
- RPC replies on `amq.rabbitmq.reply-to`
- `app.direct` / `order.cancelled` (if stock depleted during reservation)

**Dual role:** Participates in BOTH the sync RPC flow AND the async event stream — checks stock before the order is confirmed, then reserves stock when the order is confirmed.

---

### `PaymentService` — Consumer + Publisher (chained)

**Consumes:** `queue.order.payment` (bound to `order.created` key — receives new orders)

**Produces:**
- On success: `app.direct` / `order.paid` → triggers shipping/loyalty downstream
- On success: `app.topic` / `audit.payment.completed` → AuditService captures it
- On failure: `app.direct` / `order.cancelled`
- On failure: `app.topic` / `log.error.payment-service` → error alerting

**This demonstrates service chaining:** Order → Payment → (Shipping + Audit + Logging via downstream events)

---

### `AuditService` — Multi-queue Consumer (append-only)

**Consumes:**
- `queue.fanout.audit` — every broadcast event
- `queue.topic.audit` — structured domain audit events (`audit.#` pattern)
- `queue.topic.log.error` — error log events (triggers on-call alert)

**Produces:** Nothing — pure consumer. Appends to in-memory `AuditRecord[]`.

**Why two queues?**
- Fanout gives coarse-grained coverage (catch everything)
- Topic gives fine-grained structured audit events (exactly the events you care about with rich metadata)

---

### `NotificationService` — Multi-queue Consumer

**Consumes:**
- `queue.fanout.notification` — broadcast events (routes internally by `eventType`)
- `queue.topic.notification.email` — targeted email notifications
- `queue.topic.notification.sms` — targeted SMS notifications

**Produces:** Nothing — calls external delivery providers (SendGrid, Twilio stubs).

**Why two subscription models?**
- Fanout: "I care about every system event" (welcome emails on UserRegistered, etc.)
- Topic: "I care about specific delivery-channel events" (explicit email/SMS requests)

---

## 6. End-to-End Flow: Order Lifecycle

```
[HTTP/trigger] → OrderService.createOrder()
    │
    ├─ 1. RPC → rpc.inventory.check ──────────────► InventoryService (RpcServer)
    │          ◄── { available: true } ────────────
    │
    ├─ 2. Publish: app.direct / order.created
    │       ├──► queue.order.processing ──────────► InventoryService (reserves stock)
    │       └──► queue.order.payment ─────────────► PaymentService
    │                                                   │
    │                                          3. processPayment()
    │                                                   │
    │                                     ┌─ success ──┤
    │                                     │            └─ failure ──┐
    │                                     │                         │
    │                          Publish: order.paid      Publish: order.cancelled
    │                          Publish: audit.payment.completed  Publish: log.error.*
    │
    └─ 4. Publish: app.fanout / ""
            ├──► queue.fanout.analytics ───────────► Analytics handler
            ├──► queue.fanout.audit ───────────────► AuditService (records event)
            └──► queue.fanout.notification ────────► NotificationService (welcome/confirmation)
```

---

## 7. Why This Way? Alternatives & Problems

### Connection: Singleton vs. New Per Request

| Approach | Problem |
|---|---|
| ✅ Singleton connection | One TCP socket, channels multiplexed, efficient |
| ❌ New connection per publish | Exhausts broker FD limit (~65k), high latency per publish |
| ❌ New connection per request | AMQP handshake is ~100ms — adds unacceptable latency |

---

### Channel: One Per Publisher/Consumer vs. Shared

| Approach | Problem |
|---|---|
| ✅ Dedicated channel per publisher/consumer | Safe, independent back-pressure per channel |
| ❌ One shared channel for everything | amqplib is NOT thread-safe; concurrent publish+consume corrupts frames |
| ❌ Channel per message | Creating channels is cheap but not free; adds overhead and wastes resources |

---

### Message Acknowledgement: Manual vs. Auto-ack

| Approach | Problem |
|---|---|
| ✅ Manual ack after handler succeeds | At-least-once delivery guaranteed |
| ❌ Auto-ack (`noAck: true`) | Message removed from queue on delivery — if handler crashes, message is LOST forever |

---

### Dead Letter Queue: With vs. Without

| Approach | Problem |
|---|---|
| ✅ DLX/DLQ on every production queue | Poison messages caught, inspectable, replayable |
| ❌ No DLX | After `maxRetries`, `nack(false, false)` drops the message silently — data loss |

---

### Direct vs. Fanout (when to choose)

| Need | Use |
|---|---|
| One specific worker processes the message | **Direct** — exact routing key match |
| All consumers must receive every message | **Fanout** — broadcast |
| Pattern-based: some consumers get some messages | **Topic** — wildcard routing |
| Route by metadata attributes | **Headers** — attribute matching |

---

### RPC vs. Async Events

| Approach | When to use | Problem if wrong |
|---|---|---|
| ✅ RPC (sync) | Need a result before proceeding (stock check) | Tight coupling; if server down, caller blocks/fails |
| ✅ Async events | Fire-and-forget; consumers react independently | Cannot get a result synchronously |
| ❌ RPC for everything | Every call blocked waiting for reply → low throughput, cascading failures |
| ❌ Async for stock check | Order confirmed without knowing if stock exists → overselling |

---

### Envelope Pattern: With vs. Without

| Approach | Problem |
|---|---|
| ✅ `MessageEnvelope` with `messageId`, `timestamp`, `eventType` | Idempotency, tracing, type routing |
| ❌ Raw payload only | No way to deduplicate retried messages; no type info for consumers; no tracing |

---

## 8. npm Scripts Quick Reference

### Run the full monolith demo
```bash
npm run start           # Publishes all 4 exchange types + starts all consumers
```

### Run individual exchange demos (publisher only)
```bash
npm run demo:direct     # Publish order events via direct exchange
npm run demo:fanout     # Broadcast user registered event
npm run demo:topic      # Publish log + audit + notification via topic
npm run demo:headers    # Publish high/low priority tasks via headers
npm run demo:all        # All four in sequence
npm run consumer:start  # Start all consumers (pair with demos above)
```

### Run isolated microservices (requires broker running)
```bash
npm run docker:broker:up       # Start RabbitMQ broker only

# In separate terminals:
npm run svc:inventory          # Start first (RPC server must be up before OrderService)
npm run svc:payment
npm run svc:audit
npm run svc:notification
npm run svc:order              # Start last — triggers the order flow
```

### Docker (all services + broker)
```bash
npm run docker:all:up          # Build and start everything
npm run docker:all:logs        # Follow all logs
npm run docker:all:down        # Stop and remove containers
```

### Per-service isolated Docker (own network)
```bash
npm run docker:order:up
npm run docker:payment:up
# etc.
```

---

## File Map

```
src/
  config/
    rabbitmq.config.ts       ← connection settings from .env
    exchanges.config.ts      ← ALL exchange/queue/routing-key names (source of truth)
  infrastructure/
    connection.manager.ts    ← Singleton TCP connection + reconnect
    logger.ts                ← Winston structured logger
  messaging/
    message.envelope.ts      ← MessageEnvelope<T> type + createEnvelope() factory
    publisher.ts             ← IPublisher + Publisher (wraps channel.publish)
    consumer.ts              ← IConsumer + Consumer (wraps channel.consume, ack/nack/DLQ)
    messaging.factory.ts     ← Creates Publisher/Consumer/Channel instances
    rpc.ts                   ← RpcClient (caller) + RpcServer (handler)
  topology/
    topology.builder.ts      ← Low-level: assertExchange, assertQueue, bindQueue
    topology.setup.ts        ← Orchestrates all 4 exchange topologies at boot
  facade/
    rabbitmq.facade.ts       ← Single entry point: initialize + startConsumers + shutdown
  exchanges/
    direct/                  ← DirectExchangeService (publishOrderCreated/Paid/Cancelled)
    fanout/                  ← FanoutExchangeService (broadcastUserRegistered, etc.)
    topic/                   ← TopicExchangeService (publishLog, publishAudit, publishNotification)
    headers/                 ← HeadersExchangeService (publishHighPriority, publishRegional)
  handlers/
    direct/order.handlers.ts ← handleOrderCreated/Paid/Cancelled
    fanout/fanout.handlers.ts← handleFanoutAnalytics/Audit/Notification
    topic/topic.handlers.ts  ← handleTopicAudit/LogError/Email/SMS
    headers/headers.handlers.ts ← handleHighPriority/LowPriority/RegionalUs
  index.ts                   ← Monolith entry: init facade → start consumers → publish demo

services/
  shared/service.base.ts     ← Abstract base: connection + topology + shutdown lifecycle
  order-service/index.ts     ← RPC client + Direct + Fanout publisher
  inventory-service/index.ts ← RPC server + Direct consumer + publisher
  payment-service/index.ts   ← Direct consumer → publishes OrderPaid + Topic audit/log
  notification-service/index.ts ← Fanout + Topic email/SMS consumer
  audit-service/index.ts     ← Fanout + Topic audit + Topic error consumer
```
