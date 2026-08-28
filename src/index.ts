import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from './infrastructure/connection.manager';
import { RabbitMQFacade } from './facade/rabbitmq.facade';
import { logger } from './infrastructure/logger';

/**
 * Application entry point.
 *
 * In a real microservice:
 *   1. Initialize the RabbitMQFacade (topology + publishers).
 *   2. Register consumers.
 *   3. Start your HTTP/gRPC server.
 *   4. Publish domain events as business operations occur.
 */
async function main(): Promise<void> {
  logger.info('Starting RabbitMQ Best-Practices Application...');

  const connectionManager = ConnectionManager.getInstance();
  const mq = new RabbitMQFacade(connectionManager);

  // Bootstrap topology + publishers
  await mq.initialize();

  // Start all consumers
  await mq.startConsumers();

  // Example: publish sample messages through each exchange type
  logger.info('Publishing demo messages through all exchange types...');

  // Direct: targeted task dispatch
  await mq.direct.publishOrderCreated({
    orderId: 'ORD-BOOT-001',
    customerId: 'CUST-001',
    amount: 99.95,
    currency: 'USD',
    items: [{ sku: 'ITEM-A', qty: 1, price: 99.95 }],
  });

  // Fanout: broadcast to all analytics/audit/notification subscribers
  await mq.fanout.broadcastUserRegistered('usr-boot-001', 'dev@example.com');

  // Topic: pattern-based — error log routes to error queue AND all-logs queue
  await mq.topic.publishLog({
    level: 'error',
    service: 'bootstrap',
    message: 'Boot-time test error log',
    context: { env: process.env.NODE_ENV },
  });

  // Headers: attribute-based routing by priority + region
  await mq.headers.publishHighPriorityTask({
    taskId: 'TASK-BOOT-001',
    taskType: 'health-check',
    data: { component: 'rabbitmq' },
  }, 'us');

  logger.info('Application running. Consumers active. Press Ctrl+C to stop.');

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`${signal} received — shutting down...`);
    await mq.shutdown();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('uncaughtException', (err) => {
    logger.error('Uncaught exception', { error: err.message, stack: err.stack });
    process.exit(1);
  });
  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection', { reason });
    process.exit(1);
  });
}

main().catch((err) => {
  logger.error('Application startup failed.', { error: err.message });
  process.exit(1);
});
