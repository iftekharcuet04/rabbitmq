import amqplib, { Channel, Options, Connection } from 'amqplib';
import type { ChannelModel } from 'amqplib';
import { rabbitmqConfig } from '../config/rabbitmq.config';
import { logger } from './logger';

/**
 * ConnectionManager — Singleton pattern.
 *
 * Responsibilities:
 *   - Maintains ONE TCP connection to the broker (expensive to open).
 *   - Provides a pool-like channel factory (channels are cheap).
 *   - Handles reconnection with exponential back-off.
 *
 * SOLID:
 *   S – Only manages the AMQP connection lifecycle.
 *   O – Reconnect strategy is injectable (open for extension).
 *   L – Can be swapped with any IConnectionManager implementation.
 *   I – Exposes a minimal surface area.
 *   D – Consumers depend on the IConnectionManager abstraction.
 *
 * NOTE: amqplib v0.10+ returns a ChannelModel from connect().
 *       ChannelModel holds the connection + createChannel factory.
 */
export interface IConnectionManager {
  getChannel(): Promise<Channel>;
  close(): Promise<void>;
}

export class ConnectionManager implements IConnectionManager {
  private static instance: ConnectionManager;
  private model: ChannelModel | null = null;
  private readonly maxRetries = 5;
  private readonly retryDelayMs = 2000;

  private constructor(private readonly url: string) {}

  /** Singleton accessor — DI containers should prefer injecting the interface. */
  public static getInstance(): ConnectionManager {
    if (!ConnectionManager.instance) {
      ConnectionManager.instance = new ConnectionManager(rabbitmqConfig.url);
    }
    return ConnectionManager.instance;
  }

  /** Obtain a dedicated channel. Callers must close it when done. */
  public async getChannel(): Promise<Channel> {
    const model = await this.getModel();
    const channel = await model.createChannel();
    channel.prefetch(rabbitmqConfig.prefetch);
    return channel;
  }

  private async getModel(): Promise<ChannelModel> {
    if (this.model) return this.model;
    return this.connect();
  }

  private async connect(attempt = 1): Promise<ChannelModel> {
    try {
      logger.info(`Connecting to RabbitMQ (attempt ${attempt})...`, { url: this.url });
      const opts: Options.Connect = { heartbeat: rabbitmqConfig.heartbeat };
      const model = await amqplib.connect(this.url, opts);
      this.model = model;

      model.on('close', () => {
        logger.warn('RabbitMQ connection closed. Scheduling reconnect...');
        this.model = null;
        this.scheduleReconnect();
      });

      model.on('error', (err: Error) => {
        logger.error('RabbitMQ connection error.', { error: err.message });
        this.model = null;
      });

      logger.info('RabbitMQ connected successfully.');
      return model;
    } catch (err) {
      if (attempt >= this.maxRetries) {
        logger.error('Max reconnect attempts reached. Giving up.', { error: (err as Error).message });
        throw err;
      }
      const delay = this.retryDelayMs * Math.pow(2, attempt - 1);
      logger.warn(`Connection failed. Retrying in ${delay}ms...`, { attempt });
      await this.sleep(delay);
      return this.connect(attempt + 1);
    }
  }

  private scheduleReconnect(): void {
    setTimeout(() => this.connect().catch(() => {}), this.retryDelayMs);
  }

  public async close(): Promise<void> {
    if (this.model) {
      await this.model.close();
      this.model = null;
      logger.info('RabbitMQ connection closed.');
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
