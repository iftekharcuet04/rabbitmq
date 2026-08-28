# RabbitMQ Best Practices — Node.js / TypeScript

> Production-oriented RabbitMQ architecture and best-practices reference implementation,
> showcasing all four AMQP exchange types with SOLID principles, Factory, Strategy, Facade, and DI patterns.
> Some production concerns (cluster configuration, observability pipelines, publisher confirms,
> operational monitoring) depend on your specific deployment environment and are not fully covered here.

---

## Quick Start

```bash
# 1. Copy env
cp .env.example .env

# 2. Install dependencies
npm install

# 3. Start RabbitMQ (Docker)
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management

# 4. Start consumers (terminal 1)
npm run consumer:start

# 5. Run demos (terminal 2)
npm run demo:all          # all exchanges
npm run demo:direct       # direct only
npm run demo:fanout       # fanout only
npm run demo:topic        # topic only
npm run demo:headers      # headers only
```

---

## Project Structure

```
src/
├── config/
│   ├── rabbitmq.config.ts       # AMQP connection config (env-driven)
│   └── exchanges.config.ts      # ALL exchange names, routing keys, queue names
│
├── infrastructure/
│   ├── connection.manager.ts    # Singleton AMQP connection + exponential back-off
│   └── logger.ts                # Structured winston logger
│
├── topology/
│   ├── topology.builder.ts      # Low-level Facade: assertExchange/Queue/bind/headers
│   └── topology.setup.ts        # High-level Facade: full broker topology bootstrap
│
├── messaging/
│   ├── message.envelope.ts      # Envelope pattern: messageId, timestamp, eventType
│   ├── publisher.ts             # Publisher with back-pressure + persistence
│   ├── consumer.ts              # Consumer with manual ack + retry + DLQ routing
│   └── messaging.factory.ts     # Abstract Factory: creates publishers & consumers
│
├── exchanges/
│   ├── direct/                  # DirectExchangeService  (order events)
│   ├── fanout/                  # FanoutExchangeService  (broadcasts)
│   ├── topic/                   # TopicExchangeService   (log/audit/notify)
│   └── headers/                 # HeadersExchangeService (priority/region)
│
├── handlers/
│   ├── direct/order.handlers.ts
│   ├── fanout/fanout.handlers.ts
│   ├── topic/topic.handlers.ts
│   └── headers/headers.handlers.ts
│
├── facade/
│   └── rabbitmq.facade.ts       # Top-level Facade + DI wiring point
│
├── demos/
│   ├── direct-exchange.demo.ts
│   ├── fanout-exchange.demo.ts
│   ├── topic-exchange.demo.ts
│   ├── headers-exchange.demo.ts
│   ├── all-exchanges.demo.ts
│   └── consumer-runner.demo.ts
│
└── index.ts                     # Application entry point
```

---

## Exchange Decision Matrix

| Exchange | Routing Mechanism | Use Case | Example Here |
|---|---|---|---|
| **Direct** | Exact routing key match | Task queues, point-to-point commands | Order lifecycle events |
| **Fanout** | Broadcasts to ALL queues | Cache invalidation, audit, push notifications | System event broadcast |
| **Topic** | Wildcard pattern (`*`, `#`) | Multi-tenant logging, selective subscriptions | Log levels, audit, notifications |
| **Headers** | Message header attributes | Priority routing, region/GDPR, content negotiation | Priority & region task queues |

---

## Design Patterns Applied

### 🏭 Abstract Factory — `MessagingFactory`
Creates `Publisher` and `Consumer` instances each backed by a **dedicated channel**.
Node.js is single-threaded, so the concern isn't concurrent thread access — it's that each channel carries internal async state (consumer tags, prefetch counters, pending confirms). Mixing unrelated publisher and consumer lifecycles on the same channel makes back-pressure and failure reasoning much harder; a dedicated channel per responsibility is the amqplib-recommended practice.

### 🎭 Facade — `RabbitMQFacade` + `TopologyBuilder` + `TopologySetup`
- `TopologyBuilder` — wraps raw amqplib calls behind an intent-revealing API.
- `TopologySetup` — orchestrates the full topology (exchanges + queues + bindings) in one idempotent call.
- `RabbitMQFacade` — single entry point. Application code calls `facade.direct.publishOrderCreated(...)`.

### ♟️ Strategy — Message Handlers
Each handler function is a **strategy** for processing one event type.
Handlers are injected into `Consumer.consume()` — swappable without touching core infrastructure.

### 🔌 Singleton — `ConnectionManager`
One TCP connection shared by the entire process. Channels are created per publisher/consumer from this single connection.

### ✉️ Envelope Pattern — `MessageEnvelope`
Every message carries: `messageId` (UUID), `timestamp`, `eventType`, `correlationId`.
Enables idempotency checks, distributed tracing, and structured logging.

---

## SOLID Principles

| Principle | Where Applied |
|---|---|
| **S**RP | Each class has one job: `ConnectionManager` ↔ connection, `Publisher` ↔ publishing, `TopologyBuilder` ↔ topology |
| **O**CP | New exchange services / handlers added without modifying existing code |
| **L**SP | `Publisher` implements `IPublisher`; swap for `ConfirmPublisher` without breaking callers |
| **I**SP | `IPublisher`, `IConsumer`, `IConnectionManager` — minimal focused interfaces |
| **D**IP | `RabbitMQFacade` depends on `IConnectionManager`, not `ConnectionManager` concrete class |

---

## Best Practices Implemented

- ✅ **One channel per publisher/consumer** — independent async state per channel; avoids back-pressure and lifecycle conflicts
- ✅ **Durable exchanges & queues** — survive broker restart
- ✅ **Persistent messages** — survive broker restart
- ✅ **Manual acknowledgement** — ack only after successful processing
- ✅ **Dead-Letter Exchange (DLX) + Dead-Letter Queue (DLQ)** — every direct queue wired to DLX
- ✅ **Retry with nack + requeue** — up to `maxRetries` before routing to DLQ
- ✅ **Per-consumer prefetch** — back-pressure control preventing consumer overload
- ✅ **Back-pressure on publish** — detects channel buffer full, awaits `drain` event
- ✅ **Exponential back-off reconnect** — handles broker restarts gracefully
- ✅ **Heartbeat** — detects dead TCP connections
- ✅ **Graceful shutdown** — SIGTERM/SIGINT handled cleanly
- ✅ **MessageEnvelope** — every message has `messageId`, `timestamp`, `eventType`, `correlationId`
- ✅ **Idempotent topology** — `assertExchange`/`assertQueue` safe to run on every boot
- ✅ **Structured logging** — winston with timestamp, level, contextual metadata
- ✅ **Environment-driven config** — no hardcoded values

---

## RabbitMQ Management UI

After starting RabbitMQ via Docker:

```
http://localhost:15672
Username: guest
Password: guest
```

You'll see all 4 exchanges (`app.direct`, `app.fanout`, `app.topic`, `app.headers`) and all 13 queues created automatically.
