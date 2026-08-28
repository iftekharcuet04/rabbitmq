import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../infrastructure/connection.manager';
import { RabbitMQFacade } from '../facade/rabbitmq.facade';
import { logger } from '../infrastructure/logger';

/**
 * TOPIC EXCHANGE DEMO
 *
 * WHY TOPIC?  Wildcard pattern routing — one message can match MULTIPLE queues.
 *
 * Routing key patterns:
 *   "log.error.auth"         → matches "log.error.*" (error queue) AND "log.#" (all logs)
 *   "audit.user.login"       → matches "audit.#" (audit queue)
 *   "notification.email.xyz" → matches "notification.email.*" (email queue)
 *
 * Run: npm run demo:topic
 */
async function runTopicDemo(): Promise<void> {
  const connection = ConnectionManager.getInstance();
  const facade = new RabbitMQFacade(connection);

  try {
    await facade.initialize();

    logger.info('=== TOPIC EXCHANGE DEMO ===');

    // Error log → matches log.error.* queue AND any log.# subscriber
    await facade.topic.publishLog({
      level: 'error',
      service: 'auth',
      message: 'JWT validation failed',
      context: { userId: 'usr-99', ip: '10.0.0.5' },
    });

    // Info log → only matches log.# (NOT log.error.*)
    await facade.topic.publishLog({
      level: 'info',
      service: 'api-gateway',
      message: 'Request received',
      context: { path: '/orders', method: 'POST' },
    });

    // Audit event → matches audit.# queue
    await facade.topic.publishAudit({
      actorId: 'admin-007',
      action: 'update',
      resource: 'user',
      resourceId: 'usr-123',
      changes: { role: { from: 'viewer', to: 'admin' } },
    });

    // Email notification → matches notification.email.* queue
    await facade.topic.publishNotification({
      channel: 'email',
      recipientId: 'usr-456',
      templateId: 'welcome',
      data: { firstName: 'Alice', activationLink: 'https://app.example.com/activate/abc' },
    });

    // SMS notification → matches notification.sms.* queue
    await facade.topic.publishNotification({
      channel: 'sms',
      recipientId: 'usr-789',
      templateId: 'otp-verify',
      data: { otp: '482910', expiresIn: '5 minutes' },
    });

    logger.info('Topic demo complete — see routing pattern matches above.');
  } finally {
    await facade.shutdown();
  }
}

runTopicDemo().catch((err) => {
  logger.error('Topic demo failed.', { error: err.message });
  process.exit(1);
});
