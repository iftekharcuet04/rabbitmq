import * as dotenv from 'dotenv';
dotenv.config();

import { ServiceBase } from '../shared/service.base';
import { Consumer } from '../../src/messaging/consumer';
import { Publisher } from '../../src/messaging/publisher';
import { QueueNames, ExchangeNames, DirectRoutingKeys } from '../../src/config/exchanges.config';
import { MessageEnvelope, createEnvelope } from '../../src/messaging/message.envelope';
import { logger } from '../../src/infrastructure/logger';
import { ConsumeMessage } from 'amqplib';
import { v4 as uuidv4 } from 'uuid';

// ─── Domain Types ─────────────────────────────────────────────────────────────
interface OrderItem { sku: string; qty: number; price: number }
interface OrderCreatedPayload {
  orderId: string; customerId: string; amount: number; currency: string; items: OrderItem[];
}
interface OrderPaidPayload {
  orderId: string; paymentId: string; amount: number; paidAt: string;
}

/**
 * PaymentService — Direct Exchange Consumer + Publisher
 *
 * Communication patterns:
 *   1. Direct Consumer → listens on queue.order.payment (bound to order.paid routing key)
 *      Wait — actually payment service consumes order.created to INITIATE payment.
 *      We add it to ORDER_PAYMENT queue which is bound to order.paid.
 *      But for the flow: PaymentService should consume ORDER_PROCESSING queue too.
 *
 * Corrected flow:
 *   - Consumes queue.order.payment (bound to order.paid in Direct exchange)
 *   - But for a real flow, payment processes order.created → publishes order.paid
 *
 * Pattern used here:
 *   - Consumes OrderCreated via a DEDICATED payment queue
 *   - Simulates payment gateway call
 *   - Publishes OrderPaid → triggers downstream (shipping, loyalty points, etc.)
 *   - Topic publishes a log event (for the topic exchange audit chain)
 *
 * This shows service chaining: Order → Payment → (Shipping + Loyalty via topic)
 */
class PaymentService extends ServiceBase {
  private publisher!: Publisher;
  private topicPublisher!: Publisher;

  constructor() {
    super('payment-service');
  }

  protected async initialize(): Promise<void> {
    // ── Consumer: process incoming orders ───────────────────────────────────
    const consumerChannel = await this.factory.createChannel();
    const consumer = new Consumer(consumerChannel);

    // ── Publisher: emit OrderPaid after successful payment ──────────────────
    const pubChannel = await this.factory.createChannel();
    this.publisher = new Publisher(pubChannel);

    // ── Topic Publisher: emit audit + log events ────────────────────────────
    const topicChannel = await this.factory.createChannel();
    this.topicPublisher = new Publisher(topicChannel);

    await consumer.consume<OrderCreatedPayload>(
      QueueNames.ORDER_PAYMENT,        // bound to order.created → payment processes it
      async (envelope: MessageEnvelope<OrderCreatedPayload>, _raw: ConsumeMessage) => {
        const { orderId, customerId, amount, currency } = envelope.payload;

        logger.info('[payment-service] Processing payment for order.', { orderId, amount, currency });

        // Simulate payment gateway call (Stripe / Adyen)
        const paymentResult = await this.processPayment(orderId, amount, currency);

        if (paymentResult.success) {
          // ── Publish OrderPaid → Direct exchange ─────────────────────────
          const paidPayload: OrderPaidPayload = {
            orderId,
            paymentId: paymentResult.paymentId,
            amount,
            paidAt: new Date().toISOString(),
          };
          const paidEnvelope = createEnvelope('OrderPaid', paidPayload, envelope.correlationId);
          await this.publisher.publish(ExchangeNames.DIRECT, DirectRoutingKeys.ORDER_PAID, paidEnvelope);

          // ── Topic: publish audit event (routing key: audit.payment.completed) ──
          const auditEnvelope = createEnvelope('PaymentCompleted', {
            actorId: 'payment-service',
            action: 'completed',
            resource: 'payment',
            resourceId: paymentResult.paymentId,
            changes: { orderId, amount, currency },
          });
          await this.topicPublisher.publish(ExchangeNames.TOPIC, `audit.payment.completed`, auditEnvelope);

          logger.info('[payment-service] Payment successful. OrderPaid published.', {
            orderId,
            paymentId: paymentResult.paymentId,
          });
        } else {
          // ── Publish OrderCancelled on payment failure ─────────────────
          const cancelEnvelope = createEnvelope('OrderCancelled', {
            orderId,
            reason: `Payment failed: ${paymentResult.reason}`,
            cancelledAt: new Date().toISOString(),
          }, envelope.correlationId);
          await this.publisher.publish(ExchangeNames.DIRECT, DirectRoutingKeys.ORDER_CANCELLED, cancelEnvelope);

          // Topic: log error
          const errEnvelope = createEnvelope('LogEvent', {
            level: 'error',
            service: 'payment-service',
            message: 'Payment gateway failure',
            context: { orderId, reason: paymentResult.reason },
          });
          await this.topicPublisher.publish(ExchangeNames.TOPIC, 'log.error.payment-service', errEnvelope);

          logger.error('[payment-service] Payment failed. OrderCancelled published.', { orderId, reason: paymentResult.reason });
        }
      },
      { prefetch: 5, maxRetries: 3 },
    );

    logger.info('[payment-service] Consumer ready on queue.order.payment');
  }

  /** Simulates calling an external payment gateway. */
  private async processPayment(
    orderId: string,
    amount: number,
    currency: string,
  ): Promise<{ success: boolean; paymentId: string; reason?: string }> {
    // Simulate network latency
    await new Promise((r) => setTimeout(r, 200 + Math.random() * 300));

    // 90% success rate simulation
    const success = Math.random() > 0.1;

    if (success) {
      return { success: true, paymentId: `PAY-${uuidv4().substring(0, 8).toUpperCase()}` };
    }
    return { success: false, paymentId: '', reason: 'Insufficient funds' };
  }
}

new PaymentService().start().catch((err) => {
  logger.error('payment-service failed to start.', { error: err.message });
  process.exit(1);
});
