import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../infrastructure/connection.manager';
import { RabbitMQFacade } from '../facade/rabbitmq.facade';
import { logger } from '../infrastructure/logger';

/**
 * DIRECT EXCHANGE DEMO
 *
 * WHY DIRECT?  Exact routing key match → precise task dispatching.
 *
 * Topology:
 *   app.direct  ──[order.created]──►  queue.order.processing
 *               ──[order.paid]────►  queue.order.payment
 *               ──[order.cancelled]► queue.order.cancellation
 *
 * Run: npm run demo:direct
 */
async function runDirectDemo(): Promise<void> {
  const connection = ConnectionManager.getInstance();
  const facade = new RabbitMQFacade(connection);

  try {
    await facade.initialize();

    logger.info('=== DIRECT EXCHANGE DEMO ===');

    // Publish OrderCreated
    await facade.direct.publishOrderCreated(
      {
        orderId: 'ORD-001',
        customerId: 'CUST-42',
        amount: 149.99,
        currency: 'USD',
        items: [{ sku: 'SKU-A', qty: 2, price: 49.99 }, { sku: 'SKU-B', qty: 1, price: 50.01 }],
      },
      'corr-demo-001',
    );

    // Publish OrderPaid
    await facade.direct.publishOrderPaid(
      {
        orderId: 'ORD-001',
        paymentId: 'PAY-XYZ',
        amount: 149.99,
        paidAt: new Date().toISOString(),
      },
      'corr-demo-002',
    );

    // Publish OrderCancelled
    await facade.direct.publishOrderCancelled(
      {
        orderId: 'ORD-002',
        reason: 'Customer requested cancellation',
        cancelledAt: new Date().toISOString(),
      },
    );

    logger.info('Direct exchange demo complete — check queues in RabbitMQ Management UI.');
  } finally {
    await facade.shutdown();
  }
}

runDirectDemo().catch((err) => {
  logger.error('Direct demo failed.', { error: err.message });
  process.exit(1);
});
