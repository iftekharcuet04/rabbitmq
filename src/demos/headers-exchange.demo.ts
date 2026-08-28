import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../infrastructure/connection.manager';
import { RabbitMQFacade } from '../facade/rabbitmq.facade';
import { logger } from '../infrastructure/logger';

/**
 * HEADERS EXCHANGE DEMO
 *
 * WHY HEADERS?  Route by message attributes, not routing key hierarchy.
 *
 * Bindings:
 *   queue.headers.priority.high → { priority: 'high', format: 'json', x-match: 'all' }
 *   queue.headers.priority.low  → { priority: 'low', x-match: 'all' }
 *   queue.headers.region.us     → { region: 'us', x-match: 'any' }
 *
 * Run: npm run demo:headers
 */
async function runHeadersDemo(): Promise<void> {
  const connection = ConnectionManager.getInstance();
  const facade = new RabbitMQFacade(connection);

  try {
    await facade.initialize();

    logger.info('=== HEADERS EXCHANGE DEMO ===');

    // High-priority task — headers: { priority: 'high', format: 'json' }
    // Matches: queue.headers.priority.high  (x-match: all, both headers present)
    // Also: queue.headers.region.us        (x-match: any, region=us matches)
    await facade.headers.publishHighPriorityTask(
      {
        taskId: 'TASK-HP-001',
        taskType: 'payment-settlement',
        data: { amount: 50000, currency: 'USD' },
      },
      'us',
    );

    // Low-priority background task — headers: { priority: 'low', format: 'json' }
    // Matches: queue.headers.priority.low  (x-match: all, priority=low matches)
    await facade.headers.publishLowPriorityTask({
      taskId: 'TASK-LP-002',
      taskType: 'report-generation',
      data: { reportType: 'monthly-summary', period: '2024-07' },
    });

    // EU regional message — headers: { region: 'eu', priority: 'low' }
    // Does NOT match queue.headers.region.us (region is 'eu', not 'us')
    await facade.headers.publishRegionalMessage(
      {
        taskId: 'TASK-EU-003',
        taskType: 'gdpr-data-export',
        data: { userId: 'usr-EU-789' },
      },
      'eu',
      'high',
    );

    // US regional message — headers: { region: 'us', priority: 'low' }
    // Matches: queue.headers.region.us (x-match: any, region=us)
    await facade.headers.publishRegionalMessage(
      {
        taskId: 'TASK-US-004',
        taskType: 'tax-calculation',
        data: { state: 'CA', revenue: 12000 },
      },
      'us',
    );

    logger.info('Headers demo complete — routing driven by message attributes, not keys.');
  } finally {
    await facade.shutdown();
  }
}

runHeadersDemo().catch((err) => {
  logger.error('Headers demo failed.', { error: err.message });
  process.exit(1);
});
