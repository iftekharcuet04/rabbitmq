import * as dotenv from 'dotenv';
dotenv.config();

import { ConnectionManager } from '../../src/infrastructure/connection.manager';
import { MessagingFactory } from '../../src/messaging/messaging.factory';
import { TopologySetup } from '../../src/topology/topology.setup';
import { logger } from '../../src/infrastructure/logger';

/**
 * ServiceBase — Abstract base class for all microservices.
 *
 * Pattern: Template Method — defines the lifecycle skeleton:
 *   1. connect → 2. setup topology → 3. initialize (subclass hook) → 4. run
 *
 * Each subclass implements `initialize()` to register its own consumers/RPC.
 *
 * SOLID:
 *   S – Owns startup/shutdown lifecycle only.
 *   O – Subclasses extend `initialize()` without modifying this base.
 *   L – All service subclasses are substitutable.
 *   D – Depends on `IConnectionManager` abstraction.
 */
export abstract class ServiceBase {
  protected factory!: MessagingFactory;
  protected readonly serviceName: string;
  private connectionManager = ConnectionManager.getInstance();

  constructor(serviceName: string) {
    this.serviceName = serviceName;
  }

  /** Template method — orchestrates the full boot sequence. */
  async start(): Promise<void> {
    logger.info(`[${this.serviceName}] Starting...`);

    this.factory = new MessagingFactory(this.connectionManager);

    // Step 1: Assert topology (idempotent — safe on every boot)
    const topologyChannel = await this.factory.createChannel();
    await new TopologySetup(topologyChannel).setup();
    await topologyChannel.close();

    // Step 2: Service-specific initialization (hook for subclasses)
    await this.initialize();

    logger.info(`[${this.serviceName}] Ready.`);
    this.registerShutdownHandlers();
  }

  /** Subclasses register their consumers, RPC servers, and publishers here. */
  protected abstract initialize(): Promise<void>;

  private registerShutdownHandlers(): void {
    const shutdown = async (signal: string) => {
      logger.info(`[${this.serviceName}] ${signal} received — shutting down...`);
      await this.connectionManager.close();
      process.exit(0);
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('uncaughtException', (err) => {
      logger.error(`[${this.serviceName}] Uncaught exception`, { error: err.message });
      process.exit(1);
    });
    process.on('unhandledRejection', (reason) => {
      logger.error(`[${this.serviceName}] Unhandled rejection`, { reason });
      process.exit(1);
    });
  }
}
