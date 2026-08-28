import * as dotenv from 'dotenv';
dotenv.config();

import { ServiceBase } from '../shared/service.base';
import { Consumer } from '../../src/messaging/consumer';
import { QueueNames } from '../../src/config/exchanges.config';
import { MessageEnvelope } from '../../src/messaging/message.envelope';
import { logger } from '../../src/infrastructure/logger';
import { ConsumeMessage } from 'amqplib';

// ─── Domain Types ─────────────────────────────────────────────────────────────
interface AuditRecord {
  id: string;
  timestamp: string;
  eventType: string;
  messageId: string;
  source: 'fanout' | 'topic';
  payload: unknown;
}

/**
 * AuditService — Multi-queue Consumer (Fanout + Topic)
 *
 * Communication patterns:
 *   1. Fanout Consumer → captures EVERY event in the system (source: fanout)
 *   2. Topic Consumer  → captures structured audit events (source: topic, pattern: audit.#)
 *
 * This service demonstrates the "append-only audit log" pattern:
 *   - Every consumed message is written to an in-memory log (production: append-only DB / S3)
 *   - Messages from different exchanges are normalised into a single AuditRecord format
 *   - The audit log is immutable — we NEVER update, only append
 *
 * Key insight: using BOTH fanout AND topic lets AuditService:
 *   - Capture broad broadcast events via fanout (coarse-grained)
 *   - Capture fine-grained domain audit events via topic (fine-grained)
 */
class AuditService extends ServiceBase {
  private readonly auditLog: AuditRecord[] = [];
  private logCounter = 0;

  constructor() {
    super('audit-service');
  }

  protected async initialize(): Promise<void> {
    // ── Fanout Consumer: catch-all broadcast subscriber ──────────────────────
    const fanoutChannel = await this.factory.createChannel();
    const fanoutConsumer = new Consumer(fanoutChannel);

    await fanoutConsumer.consume<unknown>(
      QueueNames.FANOUT_AUDIT,
      async (envelope: MessageEnvelope<unknown>, _raw: ConsumeMessage) => {
        this.appendAuditRecord(envelope, 'fanout');
      },
      { prefetch: 20, maxRetries: 2 },
    );

    // ── Topic Consumer: structured audit events (audit.#) ───────────────────
    const topicChannel = await this.factory.createChannel();
    const topicConsumer = new Consumer(topicChannel);

    await topicConsumer.consume<unknown>(
      QueueNames.TOPIC_AUDIT,
      async (envelope: MessageEnvelope<unknown>, _raw: ConsumeMessage) => {
        this.appendAuditRecord(envelope, 'topic');
      },
      { prefetch: 20, maxRetries: 2 },
    );

    // ── Topic Consumer: error log alerts ─────────────────────────────────────
    const errorChannel = await this.factory.createChannel();
    const errorConsumer = new Consumer(errorChannel);

    await errorConsumer.consume<{ level: string; service: string; message: string }>(
      QueueNames.TOPIC_LOG_ERROR,
      async (envelope, _raw) => {
        const { level, service, message } = envelope.payload;
        logger.warn('[audit-service] 🚨 ERROR LOG ALERT — triggering on-call notification.', {
          level, service, message, messageId: envelope.messageId,
        });
        this.appendAuditRecord(envelope, 'topic');
        // In production: call PagerDuty / OpsGenie API here
      },
      { prefetch: 10, maxRetries: 3 },
    );

    logger.info('[audit-service] Consuming fanout.audit + topic.audit + topic.log.error');

    // Periodically print the audit log summary
    setInterval(() => this.printAuditSummary(), 10000);
  }

  private appendAuditRecord(envelope: MessageEnvelope<unknown>, source: 'fanout' | 'topic'): void {
    const record: AuditRecord = {
      id: `audit-${++this.logCounter}`,
      timestamp: new Date().toISOString(),
      eventType: envelope.eventType,
      messageId: envelope.messageId,
      source,
      payload: envelope.payload,
    };

    this.auditLog.push(record);

    logger.info('[audit-service] 📝 Audit record appended.', {
      id: record.id,
      eventType: record.eventType,
      source,
      totalRecords: this.auditLog.length,
    });
  }

  private printAuditSummary(): void {
    if (this.auditLog.length === 0) return;

    const byType = this.auditLog.reduce<Record<string, number>>((acc, r) => {
      acc[r.eventType] = (acc[r.eventType] ?? 0) + 1;
      return acc;
    }, {});

    logger.info('[audit-service] 📊 Audit summary:', {
      totalRecords: this.auditLog.length,
      byEventType: byType,
    });
  }
}

new AuditService().start().catch((err) => {
  logger.error('audit-service failed to start.', { error: err.message });
  process.exit(1);
});
