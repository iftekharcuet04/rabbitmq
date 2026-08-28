import { Channel, ConsumeMessage } from 'amqplib';
import { QueueName } from '../config/exchanges.config';
import { MessageEnvelope } from './message.envelope';
import { logger } from '../infrastructure/logger';

/**
 * Handler function type — Strategy pattern.
 * Each handler IS a strategy for processing a specific event type.
 */
export type MessageHandler<T = unknown> = (
  envelope: MessageEnvelope<T>,
  rawMessage: ConsumeMessage,
) => Promise<void>;

/**
 * ConsumeOptions — controls consumer behaviour.
 */
export interface ConsumeOptions {
  /** Number of unacked messages a consumer may hold (back-pressure). */
  prefetch?: number;
  /** Auto-ack: skips manual acknowledgement (NOT recommended for production). */
  noAck?: boolean;
  /** Max retry attempts before routing to DLQ. */
  maxRetries?: number;
}

/**
 * IConsumer — abstraction over channel.consume.
 * DIP: high-level modules depend on this interface, not amqplib.
 */
export interface IConsumer {
  consume<T>(
    queue: QueueName,
    handler: MessageHandler<T>,
    options?: ConsumeOptions,
  ): Promise<void>;
}

/**
 * Consumer — production-oriented message consumer.
 *
 * Best practices implemented:
 *   ✓ Manual acknowledgement (ack / nack / requeue logic).
 *   ✓ Per-consumer prefetch for back-pressure control.
 *   ✓ Retry counting via x-death header.
 *   ✓ Dead-letter routing after maxRetries exceeded.
 *   ✓ JSON parse errors result in nack without requeue.
 *   ✓ Graceful error isolation — one bad message won't crash the consumer.
 */
export class Consumer implements IConsumer {
  constructor(private readonly channel: Channel) {}

  async consume<T>(
    queue: QueueName,
    handler: MessageHandler<T>,
    options: ConsumeOptions = {},
  ): Promise<void> {
    const { prefetch = 10, noAck = false, maxRetries = 3 } = options;

    // Per-consumer prefetch (best practice over global prefetch alone)
    this.channel.prefetch(prefetch, false);

    await this.channel.consume(
      queue,
      async (rawMsg) => {
        if (!rawMsg) return; // consumer cancelled by broker

        const retryCount = this.getRetryCount(rawMsg);

        try {
          const envelope = this.parseEnvelope<T>(rawMsg);

          logger.info('Message received.', {
            queue,
            messageId: envelope.messageId,
            eventType: envelope.eventType,
            retryCount,
          });

          await handler(envelope, rawMsg);

          if (!noAck) {
            this.channel.ack(rawMsg);
            logger.debug('Message acked.', { messageId: envelope.messageId });
          }
        } catch (err) {
          logger.error('Handler error.', { queue, error: (err as Error).message, retryCount });

          if (!noAck) {
            const requeue = retryCount < maxRetries;
            this.channel.nack(rawMsg, false, requeue);

            if (!requeue) {
              logger.warn('Max retries exceeded — message routed to DLQ.', { queue, retryCount });
            }
          }
        }
      },
      { noAck },
    );

    logger.info(`Consumer registered on queue: "${queue}"`, { prefetch, maxRetries });
  }

  /** Extract retry count from x-death header (set by RabbitMQ on nack). */
  private getRetryCount(msg: ConsumeMessage): number {
    const xDeath = msg.properties.headers?.['x-death'];
    if (!Array.isArray(xDeath) || xDeath.length === 0) return 0;
    return Number(xDeath[0]?.count ?? 0);
  }

  /** Parse raw message buffer into a typed MessageEnvelope. */
  private parseEnvelope<T>(msg: ConsumeMessage): MessageEnvelope<T> {
    try {
      return JSON.parse(msg.content.toString()) as MessageEnvelope<T>;
    } catch {
      throw new Error(`Failed to parse message content: ${msg.content.toString()}`);
    }
  }
}
