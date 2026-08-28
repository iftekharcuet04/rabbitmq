import { ConsumeMessage } from 'amqplib';
import { MessageEnvelope } from '../../messaging/message.envelope';
import { OrderCreatedPayload, OrderPaidPayload, OrderCancelledPayload } from '../../exchanges/direct/direct-exchange.service';
import { logger } from '../../infrastructure/logger';

/**
 * OrderCreatedHandler — Strategy for handling OrderCreated events.
 *
 * Each handler encapsulates a single processing algorithm (Strategy pattern).
 * They are injected into the Consumer — easily swappable without touching core.
 */
export async function handleOrderCreated(
  envelope: MessageEnvelope<OrderCreatedPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { orderId, customerId, amount, currency, items } = envelope.payload;

  logger.info('Processing OrderCreated.', {
    orderId,
    customerId,
    amount,
    currency,
    itemCount: items.length,
    correlationId: envelope.correlationId,
  });

  // ← Real implementation: persist to DB, trigger downstream workflows, etc.
  await simulateWork(50);
  logger.info(`Order ${orderId} processing complete.`);
}

export async function handleOrderPaid(
  envelope: MessageEnvelope<OrderPaidPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { orderId, paymentId, amount } = envelope.payload;

  logger.info('Processing OrderPaid.', { orderId, paymentId, amount });

  await simulateWork(30);
  logger.info(`Payment for order ${orderId} recorded.`);
}

export async function handleOrderCancelled(
  envelope: MessageEnvelope<OrderCancelledPayload>,
  _rawMsg: ConsumeMessage,
): Promise<void> {
  const { orderId, reason } = envelope.payload;

  logger.info('Processing OrderCancelled.', { orderId, reason });

  await simulateWork(20);
  logger.info(`Order ${orderId} cancellation processed.`);
}

function simulateWork(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
