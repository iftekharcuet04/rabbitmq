import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../infrastructure/connection.manager';
import { RabbitMQFacade } from '../facade/rabbitmq.facade';
import { logger } from '../infrastructure/logger';

/**
 * ALL-EXCHANGES DEMO
 *
 * Publishes sample messages through all four exchange types in sequence.
 * Run: npm run demo:all
 */
async function runAllExchangesDemo(): Promise<void> {
  const connection = ConnectionManager.getInstance();
  const facade = new RabbitMQFacade(connection);

  try {
    await facade.initialize();

    logger.info('========================================');
    logger.info('  ALL EXCHANGES DEMO');
    logger.info('========================================');

    // ── DIRECT: point-to-point task dispatching ──────────────────────────
    logger.info('\n--- [1/4] DIRECT EXCHANGE ---');
    await facade.direct.publishOrderCreated({
      orderId: 'ORD-ALL-001',
      customerId: 'CUST-1',
      amount: 299.00,
      currency: 'USD',
      items: [{ sku: 'PRO-PLAN', qty: 1, price: 299.00 }],
    });

    // ── FANOUT: broadcast to all consumers ───────────────────────────────
    logger.info('\n--- [2/4] FANOUT EXCHANGE ---');
    await facade.fanout.broadcastUserRegistered('usr-all-001', 'bob@example.com');

    // ── TOPIC: pattern-based routing ─────────────────────────────────────
    logger.info('\n--- [3/4] TOPIC EXCHANGE ---');
    await facade.topic.publishLog({ level: 'error', service: 'payments', message: 'Stripe timeout' });
    await facade.topic.publishAudit({
      actorId: 'sys-worker',
      action: 'create',
      resource: 'subscription',
      resourceId: 'sub-xyz',
    });

    // ── HEADERS: attribute-based routing ─────────────────────────────────
    logger.info('\n--- [4/4] HEADERS EXCHANGE ---');
    await facade.headers.publishHighPriorityTask({
      taskId: 'TASK-ALL-001',
      taskType: 'fraud-check',
      data: { orderId: 'ORD-ALL-001', riskScore: 0.85 },
    });

    logger.info('\n========================================');
    logger.info('  All exchange demos published successfully!');
    logger.info('  Run: npm run consumer:start  to consume.');
    logger.info('========================================');
  } finally {
    await facade.shutdown();
  }
}

runAllExchangesDemo().catch((err) => {
  logger.error('All-exchanges demo failed.', { error: err.message });
  process.exit(1);
});
