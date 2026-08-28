import { Channel } from 'amqplib';
import { TopologyBuilder } from './topology.builder';
import {
  ExchangeNames,
  ExchangeTypes,
  QueueNames,
  DirectRoutingKeys,
  TopicRoutingKeys,
} from '../config/exchanges.config';
import { logger } from '../infrastructure/logger';

/**
 * TopologySetup — orchestrates the full broker topology on startup.
 *
 * Pattern: Facade — hides the complexity of asserting every
 * exchange, queue, and binding behind a single `setup()` call.
 *
 * Every resource is asserted idempotently, so this is safe to
 * run on every application boot.
 */
export class TopologySetup {
  private readonly builder: TopologyBuilder;

  constructor(channel: Channel) {
    this.builder = new TopologyBuilder(channel);
  }

  /** Full topology bootstrap. Call once per application start. */
  async setup(): Promise<void> {
    logger.info('Setting up RabbitMQ topology...');
    await this.setupDirectExchange();
    await this.setupFanoutExchange();
    await this.setupTopicExchange();
    await this.setupHeadersExchange();
    logger.info('Topology setup complete.');
  }

  // ─── Direct Exchange ─────────────────────────────────────────────────────
  /**
   * DIRECT EXCHANGE — Best for: task queues & point-to-point routing.
   *
   * A message is delivered to queues whose binding key EXACTLY matches
   * the routing key. Ideal for work queues (one consumer per task type).
   */
  private async setupDirectExchange(): Promise<void> {
    await this.builder.assertExchange(ExchangeNames.DIRECT, ExchangeTypes.DIRECT);

    // DLX setup for each queue (best practice)
    const orderDlx = await this.builder.assertDeadLetterSetup(QueueNames.ORDER_PROCESSING);
    const paymentDlx = await this.builder.assertDeadLetterSetup(QueueNames.ORDER_PAYMENT);
    const cancelDlx = await this.builder.assertDeadLetterSetup(QueueNames.ORDER_CANCELLATION);

    await this.builder.assertQueue(QueueNames.ORDER_PROCESSING, {
      arguments: { 'x-dead-letter-exchange': orderDlx, 'x-message-ttl': 60000 },
    });
    await this.builder.assertQueue(QueueNames.ORDER_PAYMENT, {
      arguments: { 'x-dead-letter-exchange': paymentDlx },
    });
    await this.builder.assertQueue(QueueNames.ORDER_CANCELLATION, {
      arguments: { 'x-dead-letter-exchange': cancelDlx },
    });

    await this.builder.bindQueue(QueueNames.ORDER_PROCESSING, ExchangeNames.DIRECT, DirectRoutingKeys.ORDER_CREATED);
    await this.builder.bindQueue(QueueNames.ORDER_PAYMENT, ExchangeNames.DIRECT, DirectRoutingKeys.ORDER_PAID);
    await this.builder.bindQueue(QueueNames.ORDER_CANCELLATION, ExchangeNames.DIRECT, DirectRoutingKeys.ORDER_CANCELLED);

    logger.info('Direct exchange topology ready.');
  }

  // ─── Fanout Exchange ─────────────────────────────────────────────────────
  /**
   * FANOUT EXCHANGE — Best for: broadcast / pub-sub.
   *
   * Delivers a copy of the message to EVERY bound queue,
   * ignoring routing keys entirely.
   * Ideal for: notifications, cache invalidation, audit trails.
   */
  private async setupFanoutExchange(): Promise<void> {
    await this.builder.assertExchange(ExchangeNames.FANOUT, ExchangeTypes.FANOUT);

    await this.builder.assertQueue(QueueNames.FANOUT_ANALYTICS);
    await this.builder.assertQueue(QueueNames.FANOUT_AUDIT);
    await this.builder.assertQueue(QueueNames.FANOUT_NOTIFICATION);

    // Fanout ignores routing keys — pass empty string
    await this.builder.bindQueue(QueueNames.FANOUT_ANALYTICS, ExchangeNames.FANOUT, '');
    await this.builder.bindQueue(QueueNames.FANOUT_AUDIT, ExchangeNames.FANOUT, '');
    await this.builder.bindQueue(QueueNames.FANOUT_NOTIFICATION, ExchangeNames.FANOUT, '');

    logger.info('Fanout exchange topology ready.');
  }

  // ─── Topic Exchange ───────────────────────────────────────────────────────
  /**
   * TOPIC EXCHANGE — Best for: pattern-based multi-tenant routing.
   *
   * Routing keys use dot-notation words. Wildcards:
   *   `*` matches exactly ONE word.
   *   `#` matches ZERO OR MORE words.
   * Ideal for: logging systems, multi-tenant event streams.
   */
  private async setupTopicExchange(): Promise<void> {
    await this.builder.assertExchange(ExchangeNames.TOPIC, ExchangeTypes.TOPIC);

    await this.builder.assertQueue(QueueNames.TOPIC_AUDIT);
    await this.builder.assertQueue(QueueNames.TOPIC_LOG_ERROR);
    await this.builder.assertQueue(QueueNames.TOPIC_NOTIFICATION_EMAIL);
    await this.builder.assertQueue(QueueNames.TOPIC_NOTIFICATION_SMS);

    // audit queue captures every audit.* message via `#`
    await this.builder.bindQueue(QueueNames.TOPIC_AUDIT, ExchangeNames.TOPIC, TopicRoutingKeys.AUDIT_ALL);
    // log.error queue captures log.error.<anything>
    await this.builder.bindQueue(QueueNames.TOPIC_LOG_ERROR, ExchangeNames.TOPIC, TopicRoutingKeys.LOG_ERROR);
    // email/sms notification queues via specific patterns
    await this.builder.bindQueue(QueueNames.TOPIC_NOTIFICATION_EMAIL, ExchangeNames.TOPIC, TopicRoutingKeys.NOTIFICATION_EMAIL);
    await this.builder.bindQueue(QueueNames.TOPIC_NOTIFICATION_SMS, ExchangeNames.TOPIC, TopicRoutingKeys.NOTIFICATION_SMS);

    logger.info('Topic exchange topology ready.');
  }

  // ─── Headers Exchange ─────────────────────────────────────────────────────
  /**
   * HEADERS EXCHANGE — Best for: attribute-based / metadata routing.
   *
   * Routes based on message HEADERS (not routing key).
   * `x-match: all` → AND logic (all headers must match).
   * `x-match: any` → OR logic (at least one header must match).
   * Ideal for: priority queues, region-based routing, feature flags.
   */
  private async setupHeadersExchange(): Promise<void> {
    await this.builder.assertExchange(ExchangeNames.HEADERS, ExchangeTypes.HEADERS);

    await this.builder.assertQueue(QueueNames.HEADERS_PRIORITY_HIGH);
    await this.builder.assertQueue(QueueNames.HEADERS_PRIORITY_LOW);
    await this.builder.assertQueue(QueueNames.HEADERS_REGION_US);

    // High-priority: BOTH priority=high AND format=json must be present (x-match: all)
    await this.builder.bindQueueWithHeaders(
      QueueNames.HEADERS_PRIORITY_HIGH,
      ExchangeNames.HEADERS,
      { priority: 'high', format: 'json' },
      true, // x-match: all
    );

    // Low-priority: just priority=low header (x-match: all)
    await this.builder.bindQueueWithHeaders(
      QueueNames.HEADERS_PRIORITY_LOW,
      ExchangeNames.HEADERS,
      { priority: 'low' },
      true,
    );

    // Region US: EITHER region=us OR region=us-east (x-match: any)
    await this.builder.bindQueueWithHeaders(
      QueueNames.HEADERS_REGION_US,
      ExchangeNames.HEADERS,
      { region: 'us', 'x-match': 'any' },
      false, // x-match: any
    );

    logger.info('Headers exchange topology ready.');
  }
}
