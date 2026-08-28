import * as dotenv from 'dotenv';
dotenv.config();

import { ServiceBase } from '../shared/service.base';
import { Consumer } from '../../src/messaging/consumer';
import { QueueNames } from '../../src/config/exchanges.config';
import { MessageEnvelope } from '../../src/messaging/message.envelope';
import { logger } from '../../src/infrastructure/logger';
import { ConsumeMessage } from 'amqplib';

// ─── Domain Types ─────────────────────────────────────────────────────────────
interface BroadcastPayload {
  source: string;
  eventName: string;
  data: Record<string, unknown>;
}

interface OrderPayload {
  orderId?: string;
  customerId?: string;
  amount?: number;
  [key: string]: unknown;
}

/**
 * NotificationService — Fanout Consumer + Topic Consumer
 *
 * Communication patterns:
 *   1. Fanout Consumer   → receives EVERY broadcast event (UserRegistered, OrderCreated, etc.)
 *   2. Topic Consumer    → receives email & SMS notification events selectively
 *
 * This service demonstrates TWO simultaneous subscription models on the SAME service:
 *   - Fanout for "I care about every event" (cross-cutting concerns)
 *   - Topic for "I care about specific notification channels"
 *
 * Real-world: NotificationService would call SendGrid, Twilio, FCM, etc.
 */
class NotificationService extends ServiceBase {
  constructor() {
    super('notification-service');
  }

  protected async initialize(): Promise<void> {
    // ── Fanout Consumer: receive all broadcast events ────────────────────────
    const fanoutChannel = await this.factory.createChannel();
    const fanoutConsumer = new Consumer(fanoutChannel);

    await fanoutConsumer.consume<BroadcastPayload | OrderPayload>(
      QueueNames.FANOUT_NOTIFICATION,
      async (envelope: MessageEnvelope<BroadcastPayload | OrderPayload>, _raw: ConsumeMessage) => {
        const { eventType, payload } = envelope;
        logger.info('[notification-service] Broadcast event received.', { eventType });

        // Route notification based on event type
        switch (eventType) {
          case 'UserRegistered':
            await this.sendWelcomeEmail((payload as Record<string, unknown>).email as string ?? 'unknown');
            break;
          case 'OrderCreated':
            await this.sendOrderConfirmation((payload as OrderPayload).orderId ?? 'unknown');
            break;
          case 'SystemEvent':
            logger.debug('[notification-service] System event — no user notification needed.', { eventType });
            break;
          default:
            logger.debug('[notification-service] Unhandled broadcast event type.', { eventType });
        }
      },
      { prefetch: 10, maxRetries: 3 },
    );

    // ── Topic Consumer: Email notifications ──────────────────────────────────
    const emailChannel = await this.factory.createChannel();
    const emailConsumer = new Consumer(emailChannel);

    await emailConsumer.consume<Record<string, unknown>>(
      QueueNames.TOPIC_NOTIFICATION_EMAIL,
      async (envelope: MessageEnvelope<Record<string, unknown>>, _raw: ConsumeMessage) => {
        const { recipientId, templateId } = envelope.payload;
        logger.info('[notification-service] Email notification queued.', { recipientId, templateId });
        await this.sendEmail(String(recipientId), String(templateId), envelope.payload);
      },
      { prefetch: 5, maxRetries: 2 },
    );

    // ── Topic Consumer: SMS notifications ────────────────────────────────────
    const smsChannel = await this.factory.createChannel();
    const smsConsumer = new Consumer(smsChannel);

    await smsConsumer.consume<Record<string, unknown>>(
      QueueNames.TOPIC_NOTIFICATION_SMS,
      async (envelope: MessageEnvelope<Record<string, unknown>>, _raw: ConsumeMessage) => {
        const { recipientId, templateId } = envelope.payload;
        logger.info('[notification-service] SMS notification queued.', { recipientId, templateId });
        await this.sendSms(String(recipientId), String(templateId), envelope.payload);
      },
      { prefetch: 5, maxRetries: 2 },
    );

    logger.info('[notification-service] Listening on fanout + email + SMS topic queues.');
  }

  // ── Notification delivery stubs ─────────────────────────────────────────────
  private async sendWelcomeEmail(email: string): Promise<void> {
    await this.simulateDelay(100);
    logger.info('[notification-service] ✉️  Welcome email sent.', { to: email });
  }

  private async sendOrderConfirmation(orderId: string): Promise<void> {
    await this.simulateDelay(80);
    logger.info('[notification-service] 📦  Order confirmation notification sent.', { orderId });
  }

  private async sendEmail(recipientId: string, templateId: string, data: Record<string, unknown>): Promise<void> {
    await this.simulateDelay(150);
    logger.info('[notification-service] ✉️  Email sent via SendGrid.', { recipientId, templateId, dataKeys: Object.keys(data) });
  }

  private async sendSms(recipientId: string, templateId: string, _data: Record<string, unknown>): Promise<void> {
    await this.simulateDelay(100);
    logger.info('[notification-service] 📱  SMS sent via Twilio.', { recipientId, templateId });
  }

  private simulateDelay(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

new NotificationService().start().catch((err) => {
  logger.error('notification-service failed to start.', { error: err.message });
  process.exit(1);
});
