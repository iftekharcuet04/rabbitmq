import { ConsumeMessage } from 'amqplib';
import { MessageEnvelope } from '../../messaging/message.envelope';
import { TaskPayload } from '../../exchanges/headers/headers-exchange.service';
import { logger } from '../../infrastructure/logger';

/**
 * Headers exchange handlers.
 *
 * Each queue was bound with specific header-match criteria.
 * The consumer handlers reflect what that specific "class" of worker does.
 */
export async function handleHighPriorityTask(
  envelope: MessageEnvelope<TaskPayload>,
  rawMsg: ConsumeMessage,
): Promise<void> {
  const { taskId, taskType, data } = envelope.payload;
  const headers = rawMsg.properties.headers ?? {};

  logger.info('[Headers:HighPriority] Processing high-priority task.', {
    taskId,
    taskType,
    priority: headers['priority'],
    format: headers['format'],
    region: headers['region'],
    messageId: envelope.messageId,
  });

  // ← Route to fast-lane processing pool
  await simulateWork(10); // Minimal latency for high-priority
  logger.info(`High-priority task ${taskId} complete.`);
}

export async function handleLowPriorityTask(
  envelope: MessageEnvelope<TaskPayload>,
  rawMsg: ConsumeMessage,
): Promise<void> {
  const { taskId, taskType } = envelope.payload;
  const headers = rawMsg.properties.headers ?? {};

  logger.info('[Headers:LowPriority] Processing low-priority background task.', {
    taskId,
    taskType,
    priority: headers['priority'],
    messageId: envelope.messageId,
  });

  // ← Process in batch or at off-peak hours
  await simulateWork(200); // Background — slower is fine
  logger.info(`Low-priority task ${taskId} complete.`);
}

export async function handleRegionalUsTask(
  envelope: MessageEnvelope<TaskPayload>,
  rawMsg: ConsumeMessage,
): Promise<void> {
  const { taskId } = envelope.payload;
  const headers = rawMsg.properties.headers ?? {};

  logger.info('[Headers:RegionUS] Processing US-region task.', {
    taskId,
    region: headers['region'],
    messageId: envelope.messageId,
  });

  // ← Apply US data residency rules
  await simulateWork(50);
  logger.info(`US-region task ${taskId} complete.`);
}

function simulateWork(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
