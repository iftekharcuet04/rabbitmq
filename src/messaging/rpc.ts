import { Channel, ConsumeMessage } from 'amqplib';
import { v4 as uuidv4 } from 'uuid';
import { logger } from '../infrastructure/logger';

/**
 * RpcClient — Request/Reply (RPC) pattern over RabbitMQ.
 *
 * Pattern: each RPC call publishes to a well-known queue with a
 * unique `correlationId` and a `replyTo` queue (`amq.rabbitmq.reply-to`
 * — the built-in Direct Reply-to queue, no setup needed).
 *
 * The server processes the request and publishes to `msg.properties.replyTo`
 * with the same `correlationId`.
 *
 * Best practice: use `amq.rabbitmq.reply-to` (pseudo-queue) instead of
 * creating an exclusive reply queue per client — zero overhead, per-connection.
 *
 * SOLID:
 *   S – Only handles the RPC call/response protocol.
 *   D – Depends on raw Channel; injected by caller.
 */
export class RpcClient {
  private readonly pendingCalls = new Map<
    string,
    { resolve: (v: unknown) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }
  >();

  constructor(
    private readonly channel: Channel,
    private readonly timeoutMs: number = 5000,
  ) {}

  /** Start consuming from the Direct Reply-to queue. Call ONCE per channel. */
  async start(): Promise<void> {
    // `noAck: true` is REQUIRED for amq.rabbitmq.reply-to
    await this.channel.consume(
      'amq.rabbitmq.reply-to',
      (msg) => {
        if (!msg) return;
        const correlationId = msg.properties.correlationId as string;
        const pending = this.pendingCalls.get(correlationId);
        if (!pending) return;

        clearTimeout(pending.timer);
        this.pendingCalls.delete(correlationId);

        try {
          const result = JSON.parse(msg.content.toString()) as unknown;
          pending.resolve(result);
        } catch (err) {
          pending.reject(new Error('RPC response parse error'));
        }
      },
      { noAck: true },
    );
    logger.debug('RpcClient listening on amq.rabbitmq.reply-to');
  }

  /**
   * Send an RPC call and await the response.
   * @param queue   - The RPC server queue name
   * @param payload - Request payload (will be JSON-serialised)
   * @returns       - Typed response payload
   */
  async call<TRequest, TResponse>(queue: string, payload: TRequest): Promise<TResponse> {
    const correlationId = uuidv4();

    return new Promise<TResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingCalls.delete(correlationId);
        reject(new Error(`RPC timeout after ${this.timeoutMs}ms — queue: ${queue}`));
      }, this.timeoutMs);

      this.pendingCalls.set(correlationId, {
        resolve: resolve as (v: unknown) => void,
        reject,
        timer,
      });

      this.channel.sendToQueue(
        queue,
        Buffer.from(JSON.stringify(payload)),
        {
          correlationId,
          replyTo: 'amq.rabbitmq.reply-to',
          contentType: 'application/json',
          persistent: false,           // RPC replies are ephemeral
          expiration: String(this.timeoutMs),
        },
      );

      logger.debug('RPC call sent.', { queue, correlationId });
    });
  }
}

/**
 * RpcServer — processes RPC requests and sends replies.
 *
 * Pattern: consume from a well-known queue, call the handler,
 * publish the response to `msg.properties.replyTo`.
 */
export type RpcHandler<TReq, TRes> = (request: TReq) => Promise<TRes>;

export class RpcServer {
  constructor(private readonly channel: Channel) {}

  async listen<TReq, TRes>(
    queue: string,
    handler: RpcHandler<TReq, TRes>,
    prefetch = 1,
  ): Promise<void> {
    // Assert the RPC server queue (durable so it survives restarts)
    await this.channel.assertQueue(queue, { durable: true });
    this.channel.prefetch(prefetch);

    await this.channel.consume(queue, async (msg: ConsumeMessage | null) => {
      if (!msg) return;

      try {
        const request = JSON.parse(msg.content.toString()) as TReq;
        const response = await handler(request);

        if (msg.properties.replyTo) {
          this.channel.sendToQueue(
            msg.properties.replyTo,
            Buffer.from(JSON.stringify(response)),
            {
              correlationId: msg.properties.correlationId,
              contentType: 'application/json',
            },
          );
        }

        this.channel.ack(msg);
        logger.debug('RPC request handled.', { queue, correlationId: msg.properties.correlationId });
      } catch (err) {
        logger.error('RPC handler error.', { queue, error: (err as Error).message });
        // nack without requeue — bad requests should not loop
        this.channel.nack(msg, false, false);
      }
    });

    logger.info(`RPC server listening on queue: "${queue}"`);
  }
}
