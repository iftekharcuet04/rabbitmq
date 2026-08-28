import { IPublisher } from '../../messaging/publisher';
import { ExchangeNames } from '../../config/exchanges.config';
import { createEnvelope } from '../../messaging/message.envelope';
import { logger } from '../../infrastructure/logger';

// ─── Domain Types ─────────────────────────────────────────────────────────────
export interface SystemEventPayload {
  source: string;
  eventName: string;
  data: Record<string, unknown>;
}

/**
 * FanoutExchangeService — Broadcast publisher.
 *
 * WHY FANOUT EXCHANGE HERE?
 *   Fanout delivers a copy of every message to ALL bound queues regardless
 *   of routing key (routing key is ignored). This is the best fit when
 *   multiple independent consumers MUST all receive every message.
 *
 * Use-cases:
 *   • Cache invalidation (every node must invalidate).
 *   • Audit logging (every event logged to multiple audit stores).
 *   • Push notifications (analytics + notification + audit simultaneously).
 *
 * SOLID:
 *   S – Only broadcasts system-level events.
 *   D – Depends on IPublisher abstraction.
 */
export class FanoutExchangeService {
  constructor(private readonly publisher: IPublisher) {}

  /**
   * Broadcast an event to ALL subscribers simultaneously.
   * Routing key is empty string (broker ignores it for fanout).
   */
  async broadcastSystemEvent(payload: SystemEventPayload, correlationId?: string): Promise<void> {
    const envelope = createEnvelope('SystemEvent', payload, correlationId);

    await this.publisher.publish(
      ExchangeNames.FANOUT,
      '',   // ← routing key ignored by fanout exchange
      envelope,
      { persistent: true },
    );

    logger.info('System event broadcast.', { eventName: payload.eventName, source: payload.source });
  }

  async broadcastUserRegistered(userId: string, email: string): Promise<void> {
    const payload: SystemEventPayload = {
      source: 'auth-service',
      eventName: 'UserRegistered',
      data: { userId, email, registeredAt: new Date().toISOString() },
    };
    await this.broadcastSystemEvent(payload);
  }

  async broadcastCacheInvalidation(keys: string[]): Promise<void> {
    const payload: SystemEventPayload = {
      source: 'cache-service',
      eventName: 'CacheInvalidated',
      data: { keys, invalidatedAt: new Date().toISOString() },
    };
    await this.broadcastSystemEvent(payload);
  }
}
