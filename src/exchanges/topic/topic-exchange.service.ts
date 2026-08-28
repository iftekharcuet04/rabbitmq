import { IPublisher } from '../../messaging/publisher';
import { ExchangeNames } from '../../config/exchanges.config';
import { createEnvelope } from '../../messaging/message.envelope';
import { logger } from '../../infrastructure/logger';

// ─── Domain Types ─────────────────────────────────────────────────────────────
export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'fatal';
export type NotificationChannel = 'email' | 'sms' | 'push';

export interface LogPayload {
  level: LogLevel;
  service: string;
  message: string;
  context?: Record<string, unknown>;
}

export interface NotificationPayload {
  channel: NotificationChannel;
  recipientId: string;
  templateId: string;
  data: Record<string, unknown>;
}

export interface AuditPayload {
  actorId: string;
  action: string;
  resource: string;
  resourceId: string;
  changes?: Record<string, unknown>;
}

/**
 * TopicExchangeService — Pattern-based routing.
 *
 * WHY TOPIC EXCHANGE HERE?
 *   Topic exchange uses glob-style wildcard patterns on routing keys.
 *   A single message can match multiple queues simultaneously (unlike direct).
 *   This makes it ideal when consumers care about message CATEGORIES.
 *
 *   Routing key convention:  <domain>.<subdomain>.<detail>
 *
 *   Examples:
 *     "log.error.auth"    → matches log.error.* and log.#
 *     "audit.user.login"  → matches audit.# (captured by audit queue)
 *     "notification.email.welcome" → matches notification.email.*
 *
 * Use-cases:
 *   • Multi-tenant logging systems.
 *   • Selective audit capture.
 *   • Notification routing per channel.
 *
 * SOLID:
 *   S – Only responsible for topic-based event publishing.
 *   O – New routing key patterns added without changing existing ones.
 *   D – Depends on IPublisher.
 */
export class TopicExchangeService {
  constructor(private readonly publisher: IPublisher) {}

  // ─── Logging ────────────────────────────────────────────────────────────
  async publishLog(payload: LogPayload, correlationId?: string): Promise<void> {
    // Routing key: "log.<level>.<service>"
    // `log.error.*` queue will receive log.error.<anything>
    // `log.#` queue will receive ALL log messages
    const routingKey = `log.${payload.level}.${payload.service}`;
    const envelope = createEnvelope('LogEvent', payload, correlationId);

    await this.publisher.publish(ExchangeNames.TOPIC, routingKey, envelope);
    logger.info('Log event published.', { routingKey, level: payload.level });
  }

  // ─── Audit ───────────────────────────────────────────────────────────────
  async publishAudit(payload: AuditPayload, correlationId?: string): Promise<void> {
    // Routing key: "audit.<resource>.<action>"
    // `audit.#` queue receives ALL audit events
    const routingKey = `audit.${payload.resource}.${payload.action}`;
    const envelope = createEnvelope('AuditEvent', payload, correlationId);

    await this.publisher.publish(ExchangeNames.TOPIC, routingKey, envelope);
    logger.info('Audit event published.', { routingKey, actor: payload.actorId });
  }

  // ─── Notifications ────────────────────────────────────────────────────────
  async publishNotification(payload: NotificationPayload, correlationId?: string): Promise<void> {
    // Routing key: "notification.<channel>.<templateId>"
    // email queue: notification.email.*
    // sms queue:   notification.sms.*
    const routingKey = `notification.${payload.channel}.${payload.templateId}`;
    const envelope = createEnvelope('NotificationEvent', payload, correlationId);

    await this.publisher.publish(ExchangeNames.TOPIC, routingKey, envelope);
    logger.info('Notification event published.', { routingKey, channel: payload.channel });
  }
}
