import { Channel } from 'amqplib';
import { IConnectionManager } from '../infrastructure/connection.manager';
import { Publisher, IPublisher } from './publisher';
import { Consumer, IConsumer } from './consumer';

/**
 * MessagingFactory — Abstract Factory pattern.
 *
 * Creates Publisher and Consumer instances backed by a fresh channel.
 * Each publisher/consumer gets its OWN channel — amqplib best practice.
 * (One channel per publisher, one per consumer — channels are NOT thread-safe.)
 *
 * SOLID:
 *   S – Only responsible for creating messaging components.
 *   O – New component types can be added (e.g. ConfirmPublisher) without
 *       modifying existing factory methods.
 *   D – Depends on IConnectionManager, not the concrete class.
 */
export interface IMessagingFactory {
  createPublisher(): Promise<IPublisher>;
  createConsumer(): Promise<IConsumer>;
  createChannel(): Promise<Channel>;
}

export class MessagingFactory implements IMessagingFactory {
  constructor(private readonly connectionManager: IConnectionManager) {}

  /** Returns a Publisher backed by a dedicated channel. */
  async createPublisher(): Promise<IPublisher> {
    const channel = await this.connectionManager.getChannel();
    return new Publisher(channel);
  }

  /** Returns a Consumer backed by a dedicated channel. */
  async createConsumer(): Promise<IConsumer> {
    const channel = await this.connectionManager.getChannel();
    return new Consumer(channel);
  }

  /** Exposes a raw channel for advanced use-cases (e.g. TopologySetup). */
  async createChannel(): Promise<Channel> {
    return this.connectionManager.getChannel();
  }
}
