import { IConnectionManager } from '../infrastructure/connection.manager';
import { MessagingFactory } from '../messaging/messaging.factory';
import { TopologySetup } from '../topology/topology.setup';

import { DirectExchangeService } from '../exchanges/direct/direct-exchange.service';
import { FanoutExchangeService } from '../exchanges/fanout/fanout-exchange.service';
import { TopicExchangeService } from '../exchanges/topic/topic-exchange.service';
import { HeadersExchangeService } from '../exchanges/headers/headers-exchange.service';

import { QueueNames } from '../config/exchanges.config';
import { logger } from '../infrastructure/logger';

import { handleOrderCreated, handleOrderPaid, handleOrderCancelled } from '../handlers/direct/order.handlers';
import { handleFanoutAnalytics, handleFanoutAudit, handleFanoutNotification } from '../handlers/fanout/fanout.handlers';
import { handleTopicAudit, handleTopicLogError, handleTopicNotificationEmail, handleTopicNotificationSms } from '../handlers/topic/topic.handlers';
import { handleHighPriorityTask, handleLowPriorityTask, handleRegionalUsTask } from '../handlers/headers/headers.handlers';

/**
 * RabbitMQFacade — Facade + DI Container pattern.
 *
 * Hides the entire bootstrapping complexity behind a clean, unified surface.
 * Application code only ever interacts with this facade — it never touches
 * amqplib, channel management, or topology details directly.
 *
 * Pattern justifications:
 *   Facade:   Single entry point to the entire messaging subsystem.
 *   DI:       All dependencies are injected, not constructed internally.
 *   Factory:  MessagingFactory isolates channel creation.
 *
 * SOLID:
 *   S – Coordinates startup; delegates all domain work to specialised services.
 *   O – New exchange services plugged in without modifying bootstrap logic.
 *   D – High-level app code depends on this facade, not amqplib.
 */
export class RabbitMQFacade {
  private factory!: MessagingFactory;

  // ─── Published service surface ────────────────────────────────────────────
  public direct!: DirectExchangeService;
  public fanout!: FanoutExchangeService;
  public topic!: TopicExchangeService;
  public headers!: HeadersExchangeService;

  constructor(private readonly connectionManager: IConnectionManager) {}

  /**
   * Bootstrap the full messaging infrastructure.
   * Must be called ONCE before using any publish/consume methods.
   */
  async initialize(): Promise<void> {
    logger.info('Initializing RabbitMQ facade...');
    this.factory = new MessagingFactory(this.connectionManager);

    // 1. Set up the complete broker topology (idempotent)
    const topologyChannel = await this.factory.createChannel();
    const topology = new TopologySetup(topologyChannel);
    await topology.setup();
    await topologyChannel.close();

    // 2. Create dedicated publishers (one channel per publisher)
    const directPublisher = await this.factory.createPublisher();
    const fanoutPublisher = await this.factory.createPublisher();
    const topicPublisher = await this.factory.createPublisher();
    const headersPublisher = await this.factory.createPublisher();

    // 3. Instantiate domain services with injected publishers
    this.direct = new DirectExchangeService(directPublisher);
    this.fanout = new FanoutExchangeService(fanoutPublisher);
    this.topic = new TopicExchangeService(topicPublisher);
    this.headers = new HeadersExchangeService(headersPublisher);

    logger.info('RabbitMQ facade initialized. All publishers ready.');
  }

  /**
   * Start all consumers.
   * Each consumer gets its own dedicated channel (amqplib best practice).
   */
  async startConsumers(): Promise<void> {
    logger.info('Starting all consumers...');

    // ─── Direct consumers ──────────────────────────────────────────────────
    const directConsumer1 = await this.factory.createConsumer();
    await directConsumer1.consume(QueueNames.ORDER_PROCESSING, handleOrderCreated, { prefetch: 5 });

    const directConsumer2 = await this.factory.createConsumer();
    await directConsumer2.consume(QueueNames.ORDER_PAYMENT, handleOrderPaid, { prefetch: 5 });

    const directConsumer3 = await this.factory.createConsumer();
    await directConsumer3.consume(QueueNames.ORDER_CANCELLATION, handleOrderCancelled, { prefetch: 5 });

    // ─── Fanout consumers ──────────────────────────────────────────────────
    const fanoutConsumer1 = await this.factory.createConsumer();
    await fanoutConsumer1.consume(QueueNames.FANOUT_ANALYTICS, handleFanoutAnalytics, { prefetch: 10 });

    const fanoutConsumer2 = await this.factory.createConsumer();
    await fanoutConsumer2.consume(QueueNames.FANOUT_AUDIT, handleFanoutAudit, { prefetch: 10 });

    const fanoutConsumer3 = await this.factory.createConsumer();
    await fanoutConsumer3.consume(QueueNames.FANOUT_NOTIFICATION, handleFanoutNotification, { prefetch: 5 });

    // ─── Topic consumers ───────────────────────────────────────────────────
    const topicConsumer1 = await this.factory.createConsumer();
    await topicConsumer1.consume(QueueNames.TOPIC_AUDIT, handleTopicAudit, { prefetch: 10 });

    const topicConsumer2 = await this.factory.createConsumer();
    await topicConsumer2.consume(QueueNames.TOPIC_LOG_ERROR, handleTopicLogError, { prefetch: 10 });

    const topicConsumer3 = await this.factory.createConsumer();
    await topicConsumer3.consume(QueueNames.TOPIC_NOTIFICATION_EMAIL, handleTopicNotificationEmail, { prefetch: 5 });

    const topicConsumer4 = await this.factory.createConsumer();
    await topicConsumer4.consume(QueueNames.TOPIC_NOTIFICATION_SMS, handleTopicNotificationSms, { prefetch: 5 });

    // ─── Headers consumers ─────────────────────────────────────────────────
    const headersConsumer1 = await this.factory.createConsumer();
    await headersConsumer1.consume(QueueNames.HEADERS_PRIORITY_HIGH, handleHighPriorityTask, { prefetch: 20 });

    const headersConsumer2 = await this.factory.createConsumer();
    await headersConsumer2.consume(QueueNames.HEADERS_PRIORITY_LOW, handleLowPriorityTask, { prefetch: 3 });

    const headersConsumer3 = await this.factory.createConsumer();
    await headersConsumer3.consume(QueueNames.HEADERS_REGION_US, handleRegionalUsTask, { prefetch: 10 });

    logger.info('All consumers started and listening.');
  }

  /** Graceful shutdown. */
  async shutdown(): Promise<void> {
    await this.connectionManager.close();
    logger.info('RabbitMQ facade shut down.');
  }
}
