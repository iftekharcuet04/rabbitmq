import { ConsumeMessage } from 'amqplib';
import { MessageEnvelope } from '../../messaging/message.envelope';
import { SystemEventPayload } from '../../exchanges/fanout/fanout-exchange.service';
import { logger } from '../../infrastructure/logger';

/**
 * Fanout handlers — each queue bound to the fanout exchange gets
 * its own handler, independently processing the SAME broadcast.
 *
 * This models the "independent subscriber" pattern:
 *   analytics doesn't care what notification does, and vice versa.
 */
export async function handleFanoutAnalytics(
  envelope: MessageEnvelope<SystemEventPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  logger.info('[Analytics] Processing broadcast event.', {
    eventName: envelope.payload.eventName,
    source: envelope.payload.source,
    messageId: envelope.messageId,
  });
  // ← Track event in analytics database / data warehouse
  await simulateWork(40);
}

export async function handleFanoutAudit(
  envelope: MessageEnvelope<SystemEventPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  logger.info('[Audit] Logging broadcast event.', {
    eventName: envelope.payload.eventName,
    source: envelope.payload.source,
    messageId: envelope.messageId,
  });
  // ← Write immutable audit trail to audit log store
  await simulateWork(20);
}

export async function handleFanoutNotification(
  envelope: MessageEnvelope<SystemEventPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  logger.info('[Notification] Sending broadcast notification.', {
    eventName: envelope.payload.eventName,
    data: envelope.payload.data,
  });
  // ← Send push notification / webhook
  await simulateWork(60);
}

function simulateWork(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
