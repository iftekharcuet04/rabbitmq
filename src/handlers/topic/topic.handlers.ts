import { ConsumeMessage } from 'amqplib';
import { MessageEnvelope } from '../../messaging/message.envelope';
import { LogPayload, AuditPayload, NotificationPayload } from '../../exchanges/topic/topic-exchange.service';
import { logger } from '../../infrastructure/logger';

/**
 * Topic exchange handlers.
 *
 * Multiple queues may receive the SAME message if multiple routing patterns
 * match (e.g. a `log.error.auth` message reaches BOTH `log.error.*` AND `log.#`).
 * Each handler processes only what it cares about.
 */
export async function handleTopicAudit(
  envelope: MessageEnvelope<AuditPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { actorId, action, resource, resourceId } = envelope.payload;

  logger.info('[TopicAudit] Captured audit event.', {
    actorId,
    action,
    resource,
    resourceId,
    messageId: envelope.messageId,
  });
  // ← Persist to append-only audit log
  await simulateWork(25);
}

export async function handleTopicLogError(
  envelope: MessageEnvelope<LogPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { level, service, message, context } = envelope.payload;

  logger.warn('[TopicLogError] Error log received — alerting on-call.', {
    level,
    service,
    message,
    context,
  });
  // ← Trigger PagerDuty / OpsGenie alert for errors
  await simulateWork(15);
}

export async function handleTopicNotificationEmail(
  envelope: MessageEnvelope<NotificationPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { recipientId, templateId, data } = envelope.payload;

  logger.info('[TopicEmail] Sending email notification.', { recipientId, templateId });
  // ← Call email service (SendGrid, SES, etc.)
  await simulateWork(80);
  logger.info(`Email sent to recipient ${recipientId} via template ${templateId}.`);
}

export async function handleTopicNotificationSms(
  envelope: MessageEnvelope<NotificationPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { recipientId, templateId } = envelope.payload;

  logger.info('[TopicSMS] Sending SMS notification.', { recipientId, templateId });
  // ← Call SMS provider (Twilio, SNS, etc.)
  await simulateWork(60);
  logger.info(`SMS sent to recipient ${recipientId}.`);
}

function simulateWork(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
