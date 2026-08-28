import { IPublisher } from '../../messaging/publisher';
import { ExchangeNames, DirectRoutingKeys } from '../../config/exchanges.config';
import { createEnvelope } from '../../messaging/message.envelope';
import { logger } from '../../infrastructure/logger';

// ─── Domain Types ─────────────────────────────────────────────────────────────
export interface OrderCreatedPayload {
  orderId: string;
  customerId: string;
  amount: number;
  currency: string;
  items: Array<{ sku: string; qty: number; price: number }>;
}

export interface OrderPaidPayload {
  orderId: string;
  paymentId: string;
  amount: number;
  paidAt: string;
}

export interface OrderCancelledPayload {
  orderId: string;
  reason: string;
  cancelledAt: string;
}

/**
 * DirectExchangeService — Strategy pattern applied at the service level.
 *
 * WHY DIRECT EXCHANGE HERE?
 *   Direct exchange provides exact routing key matching, making it ideal
 *   for task queues where a specific worker handles a specific event.
 *   E.g. `order.created` → ONLY OrderProcessingWorker receives it.
 *
 * Use-cases: order processing, job dispatching, command patterns.
 *
 * SOLID:
 *   S – Only publishes order-domain events.
 *   I – Exposes only order-specific methods (not a god-service).
 *   D – Depends on IPublisher abstraction.
 */
export class DirectExchangeService {
  constructor(private readonly publisher: IPublisher) {}

  async publishOrderCreated(payload: OrderCreatedPayload, correlationId?: string): Promise<void> {
    const envelope = createEnvelope('OrderCreated', payload, correlationId);

    await this.publisher.publish(
      ExchangeNames.DIRECT,
      DirectRoutingKeys.ORDER_CREATED,  // ← exact key match
      envelope,
      {
        persistent: true,
        priority: 5,
      },
    );

    logger.info('OrderCreated event published.', { orderId: payload.orderId });
  }

  async publishOrderPaid(payload: OrderPaidPayload, correlationId?: string): Promise<void> {
    const envelope = createEnvelope('OrderPaid', payload, correlationId);

    await this.publisher.publish(
      ExchangeNames.DIRECT,
      DirectRoutingKeys.ORDER_PAID,
      envelope,
      { persistent: true },
    );

    logger.info('OrderPaid event published.', { orderId: payload.orderId });
  }

  async publishOrderCancelled(payload: OrderCancelledPayload, correlationId?: string): Promise<void> {
    const envelope = createEnvelope('OrderCancelled', payload, correlationId);

    await this.publisher.publish(
      ExchangeNames.DIRECT,
      DirectRoutingKeys.ORDER_CANCELLED,
      envelope,
      { persistent: true },
    );

    logger.info('OrderCancelled event published.', { orderId: payload.orderId });
  }
}
