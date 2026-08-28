import { v4 as uuidv4 } from 'uuid';

/**
 * Base message envelope — every message published carries this metadata.
 * Follows the Envelope pattern for guaranteed observability fields.
 */
export interface MessageEnvelope<T = unknown> {
  /** Unique message identifier for idempotency / deduplication. */
  messageId: string;
  /** ISO-8601 timestamp of when the message was created. */
  timestamp: string;
  /** Logical event name (e.g. "OrderCreated"). */
  eventType: string;
  /** Arbitrary business payload. */
  payload: T;
  /** Optional correlation id (useful for distributed tracing). */
  correlationId?: string;
}

/**
 * Factory function: builds a fully-hydrated MessageEnvelope.
 * Using a factory keeps construction logic in one place (DRY + SRP).
 */
export function createEnvelope<T>(
  eventType: string,
  payload: T,
  correlationId?: string,
): MessageEnvelope<T> {
  return {
    messageId: uuidv4(),
    timestamp: new Date().toISOString(),
    eventType,
    payload,
    correlationId,
  };
}
