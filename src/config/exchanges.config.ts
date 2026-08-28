/**
 * Canonical exchange & queue topology definitions.
 * Changing topology in one place automatically propagates everywhere.
 */
export const ExchangeNames = {
  DIRECT: 'app.direct',
  FANOUT: 'app.fanout',
  TOPIC: 'app.topic',
  HEADERS: 'app.headers',
} as const;

export type ExchangeName = (typeof ExchangeNames)[keyof typeof ExchangeNames];

export const ExchangeTypes = {
  DIRECT: 'direct',
  FANOUT: 'fanout',
  TOPIC: 'topic',
  HEADERS: 'headers',
} as const;

export type ExchangeType = (typeof ExchangeTypes)[keyof typeof ExchangeTypes];

// ─── Direct Exchange Routing Keys ────────────────────────────────────────────
export const DirectRoutingKeys = {
  ORDER_CREATED: 'order.created',
  ORDER_PAID: 'order.paid',
  ORDER_CANCELLED: 'order.cancelled',
} as const;

// ─── Topic Exchange Routing Patterns ─────────────────────────────────────────
export const TopicRoutingKeys = {
  AUDIT_ALL: 'audit.#',
  LOG_ERROR: 'log.error.*',
  LOG_ALL: 'log.#',
  NOTIFICATION_EMAIL: 'notification.email.*',
  NOTIFICATION_SMS: 'notification.sms.*',
} as const;

// ─── Queue Names ──────────────────────────────────────────────────────────────
export const QueueNames = {
  // Direct queues
  ORDER_PROCESSING: 'queue.order.processing',
  ORDER_PAYMENT: 'queue.order.payment',
  ORDER_CANCELLATION: 'queue.order.cancellation',

  // Fanout queues (broadcast)
  FANOUT_ANALYTICS: 'queue.fanout.analytics',
  FANOUT_AUDIT: 'queue.fanout.audit',
  FANOUT_NOTIFICATION: 'queue.fanout.notification',

  // Topic queues
  TOPIC_AUDIT: 'queue.topic.audit',
  TOPIC_LOG_ERROR: 'queue.topic.log.error',
  TOPIC_NOTIFICATION_EMAIL: 'queue.topic.notification.email',
  TOPIC_NOTIFICATION_SMS: 'queue.topic.notification.sms',

  // Headers queues
  HEADERS_PRIORITY_HIGH: 'queue.headers.priority.high',
  HEADERS_PRIORITY_LOW: 'queue.headers.priority.low',
  HEADERS_REGION_US: 'queue.headers.region.us',
} as const;

export type QueueName = (typeof QueueNames)[keyof typeof QueueNames];

// ─── RPC Queue Names ──────────────────────────────────────────────────────────
// Used by RpcServer (service-to-service synchronous request/reply via amq.rabbitmq.reply-to)
export const RpcQueues = {
  INVENTORY_CHECK: 'rpc.inventory.check',
  PRICING_CALCULATE: 'rpc.pricing.calculate',
} as const;

export type RpcQueue = (typeof RpcQueues)[keyof typeof RpcQueues];
