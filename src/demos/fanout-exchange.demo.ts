import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../infrastructure/connection.manager';
import { RabbitMQFacade } from '../facade/rabbitmq.facade';
import { logger } from '../infrastructure/logger';

/**
 * FANOUT EXCHANGE DEMO
 *
 * WHY FANOUT?  One message → all subscribers simultaneously.
 *
 * Topology:
 *   app.fanout  ──► queue.fanout.analytics
 *               ──► queue.fanout.audit
 *               ──► queue.fanout.notification
 *
 * Run: npm run demo:fanout
 */
async function runFanoutDemo(): Promise<void> {
  const connection = ConnectionManager.getInstance();
  const facade = new RabbitMQFacade(connection);

  try {
    await facade.initialize();

    logger.info('=== FANOUT EXCHANGE DEMO ===');

    // Single broadcast → all 3 queues receive a copy
    await facade.fanout.broadcastUserRegistered('usr-123', 'alice@example.com');

    await facade.fanout.broadcastCacheInvalidation(['users:all', 'users:123', 'sessions:abc']);

    await facade.fanout.broadcastSystemEvent({
      source: 'deployment-service',
      eventName: 'DeploymentCompleted',
      data: { version: '2.5.0', environment: 'production', durationMs: 45000 },
    });

    logger.info('Fanout demo complete — all 3 queues received each message.');
  } finally {
    await facade.shutdown();
  }
}

runFanoutDemo().catch((err) => {
  logger.error('Fanout demo failed.', { error: err.message });
  process.exit(1);
});
