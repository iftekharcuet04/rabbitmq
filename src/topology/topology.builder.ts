import { Channel, Options } from 'amqplib';
import { ExchangeName, ExchangeType, QueueName } from '../config/exchanges.config';
import { logger } from '../infrastructure/logger';

/**
 * TopologyBuilder — Builder / Facade pattern.
 *
 * Wraps raw amqplib calls behind a clean, intent-revealing API.
 * Every exchange-assert, queue-assert, and binding is idempotent.
 *
 * SOLID:
 *   S – Only responsible for topology (not publishing or consuming).
 *   O – New topology helpers can be added without changing existing ones.
 */
export class TopologyBuilder {
  constructor(private readonly channel: Channel) {}

  /**
   * Assert (create-or-verify) an exchange on the broker.
   * durable: true  → survives broker restart.
   * autoDelete: false → stays alive even with no queues bound.
   */
  async assertExchange(
    name: ExchangeName,
    type: ExchangeType,
    options: Options.AssertExchange = {},
  ): Promise<void> {
    await this.channel.assertExchange(name, type, {
      durable: true,
      autoDelete: false,
      ...options,
    });
    logger.debug(`Exchange asserted: [${type}] "${name}"`);
  }

  /**
   * Assert a durable queue with optional Dead-Letter Exchange support.
   */
  async assertQueue(
    name: QueueName,
    options: Options.AssertQueue = {},
  ): Promise<void> {
    await this.channel.assertQueue(name, {
      durable: true,
      autoDelete: false,
      ...options,
    });
    logger.debug(`Queue asserted: "${name}"`);
  }

  /**
   * Assert a Dead-Letter Queue + Exchange pair, then return the DLX name.
   * Best practice: every production queue should have a DLX.
   */
  async assertDeadLetterSetup(queueName: string): Promise<string> {
    const dlxName = `${queueName}.dlx`;
    const dlqName = `${queueName}.dlq`;

    await this.channel.assertExchange(dlxName, 'direct', { durable: true });
    await this.channel.assertQueue(dlqName, { durable: true });
    await this.channel.bindQueue(dlqName, dlxName, '');

    logger.debug(`Dead-letter setup: DLX="${dlxName}", DLQ="${dlqName}"`);
    return dlxName;
  }

  /** Bind a queue to an exchange with a routing key (direct / topic). */
  async bindQueue(
    queue: QueueName,
    exchange: ExchangeName,
    routingKey: string,
  ): Promise<void> {
    await this.channel.bindQueue(queue, exchange, routingKey);
    logger.debug(`Bound "${queue}" → "${exchange}" [key="${routingKey}"]`);
  }

  /**
   * Bind a queue to a headers exchange using argument-matching.
   * `x-match: all`  → ALL headers must match (AND logic).
   * `x-match: any`  → ANY header must match (OR logic).
   */
  async bindQueueWithHeaders(
    queue: QueueName,
    exchange: ExchangeName,
    headers: Record<string, unknown>,
    matchAll = true,
  ): Promise<void> {
    const args: Record<string, unknown> = {
      'x-match': matchAll ? 'all' : 'any',
      ...headers,
    };
    await this.channel.bindQueue(queue, exchange, '', args);
    logger.debug(`Headers-bound "${queue}" → "${exchange}"`, { headers: args });
  }
}
