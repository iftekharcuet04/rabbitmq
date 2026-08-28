import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../infrastructure/connection.manager';
import { RabbitMQFacade } from '../facade/rabbitmq.facade';
import { logger } from '../infrastructure/logger';

/**
 * CONSUMER RUNNER
 *
 * Long-running process that starts ALL consumers across all four exchange types.
 * Handles graceful shutdown on SIGTERM / SIGINT.
 *
 * Run: npm run consumer:start
 */
async function startConsumers(): Promise<void> {
  const connection = ConnectionManager.getInstance();
  const facade = new RabbitMQFacade(connection);

  await facade.initialize();
  await facade.startConsumers();

  logger.info('All consumers running. Waiting for messages... (Ctrl+C to stop)');

  // ── Graceful shutdown ─────────────────────────────────────────────────────
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}. Shutting down gracefully...`);
    await facade.shutdown();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  // Keep the process alive
  await new Promise<void>(() => {});
}

startConsumers().catch((err) => {
  logger.error('Consumer runner failed.', { error: err.message });
  process.exit(1);
});
