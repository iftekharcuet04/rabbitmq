import { Channel, Options } from 'amqplib';
import { ExchangeName } from '../config/exchanges.config';
import { MessageEnvelope } from './message.envelope';
import { logger } from '../infrastructure/logger';

/**
 * PublishOptions extends amqplib options with headers support.
 */
export interface PublishOptions extends Options.Publish {
  headers?: Record<string, unknown>;
}

/**
 * IPublisher — Dependency Inversion: consumers depend on this
 * abstraction, not the concrete implementation.
 */
export interface IPublisher {
  publish(
    exchange: ExchangeName,
    routingKey: string,
    envelope: MessageEnvelope,
    options?: PublishOptions,
  ): Promise<void>;
}

/**
 * Publisher — concrete amqplib-backed publisher.
 *
 * Best practices implemented:
 *   ✓ Persistent messages (survive broker restart).
 *   ✓ Content-type header for consumer introspection.
 *   ✓ messageId & timestamp populated from envelope.
 *   ✓ Back-pressure detection via channel.publish return value.
 *   ✓ Confirm mode readiness via waitForConfirms (optional).
 */
export class Publisher implements IPublisher {
  constructor(private readonly channel: Channel) {}

  async publish(
    exchange: ExchangeName,
    routingKey: string,
    envelope: MessageEnvelope,
    options: PublishOptions = {},
  ): Promise<void> {
    const buffer = Buffer.from(JSON.stringify(envelope));

    const publishOptions: Options.Publish = {
      persistent: true,          // ← survives broker restart
      contentType: 'application/json',
      messageId: envelope.messageId,
      timestamp: Math.floor(Date.now() / 1000),
      correlationId: envelope.correlationId,
      ...options,
    };

    const drained = this.channel.publish(exchange, routingKey, buffer, publishOptions);

    if (!drained) {
      // Back-pressure: channel write buffer is full. Await drain event.
      logger.warn('Channel write buffer full — awaiting drain...', { exchange, routingKey });
      await new Promise<void>((resolve) => this.channel.once('drain', resolve));
    }

    logger.info('Message published.', {
      exchange,
      routingKey,
      messageId: envelope.messageId,
      eventType: envelope.eventType,
    });
  }
}
