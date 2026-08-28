import { IPublisher, PublishOptions } from '../../messaging/publisher';
import { ExchangeNames } from '../../config/exchanges.config';
import { createEnvelope } from '../../messaging/message.envelope';
import { logger } from '../../infrastructure/logger';

// ─── Domain Types ─────────────────────────────────────────────────────────────
export type Priority = 'high' | 'low';
export type Region = 'us' | 'eu' | 'ap';
export type Format = 'json' | 'xml';

export interface TaskPayload {
  taskId: string;
  taskType: string;
  data: Record<string, unknown>;
}

/**
 * HeadersExchangeService — Attribute-based routing via message headers.
 *
 * WHY HEADERS EXCHANGE HERE?
 *   Headers exchange ignores routing keys entirely — it matches on
 *   message HEADER attributes instead. This decouples routing logic
 *   from the message type hierarchy.
 *
 *   `x-match: all` → ALL specified headers must be present AND equal (AND).
 *   `x-match: any` → AT LEAST ONE header must match (OR).
 *
 *   Best fit when:
 *     • Routing decisions depend on message metadata, not topic.
 *     • Multiple orthogonal dimensions (priority × region × format).
 *     • You want consumers to declare their own matching criteria.
 *
 * Use-cases:
 *   • Priority-based task routing (high-priority → dedicated workers).
 *   • Region-based message routing (GDPR compliance).
 *   • Content-negotiation routing (json consumers vs xml consumers).
 *
 * SOLID:
 *   S – Responsible only for headers-based publishing.
 *   O – New header dimensions don't change existing publish methods.
 *   D – Depends on IPublisher.
 */
export class HeadersExchangeService {
  constructor(private readonly publisher: IPublisher) {}

  /**
   * Publish a high-priority task.
   * Matches queues with headers: { priority: 'high', format: 'json', x-match: 'all' }
   */
  async publishHighPriorityTask(
    payload: TaskPayload,
    region: Region = 'us',
    correlationId?: string,
  ): Promise<void> {
    const envelope = createEnvelope('HighPriorityTask', payload, correlationId);

    const options: PublishOptions = {
      persistent: true,
      priority: 10,
      headers: {
        priority: 'high',
        format: 'json',
        region,
      },
    };

    // Routing key is irrelevant for headers exchange — pass empty string
    await this.publisher.publish(ExchangeNames.HEADERS, '', envelope, options);
    logger.info('High-priority task published.', { taskId: payload.taskId, region });
  }

  /**
   * Publish a low-priority background task.
   * Matches queues with headers: { priority: 'low', x-match: 'all' }
   */
  async publishLowPriorityTask(
    payload: TaskPayload,
    correlationId?: string,
  ): Promise<void> {
    const envelope = createEnvelope('LowPriorityTask', payload, correlationId);

    const options: PublishOptions = {
      persistent: true,
      priority: 1,
      headers: {
        priority: 'low',
        format: 'json',
      },
    };

    await this.publisher.publish(ExchangeNames.HEADERS, '', envelope, options);
    logger.info('Low-priority task published.', { taskId: payload.taskId });
  }

  /**
   * Publish a region-specific message.
   * Matches queues with headers: { region: 'us', x-match: 'any' }
   */
  async publishRegionalMessage(
    payload: TaskPayload,
    region: Region,
    priority: Priority = 'low',
    correlationId?: string,
  ): Promise<void> {
    const envelope = createEnvelope('RegionalTask', payload, correlationId);

    const options: PublishOptions = {
      persistent: true,
      headers: {
        region,
        priority,
        format: 'json',
      },
    };

    await this.publisher.publish(ExchangeNames.HEADERS, '', envelope, options);
    logger.info('Regional message published.', { taskId: payload.taskId, region, priority });
  }
}
