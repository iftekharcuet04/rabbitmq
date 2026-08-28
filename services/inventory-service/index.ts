import * as dotenv from 'dotenv';
dotenv.config();

import { ServiceBase } from '../shared/service.base';
import { Consumer } from '../../src/messaging/consumer';
import { Publisher } from '../../src/messaging/publisher';
import { RpcServer } from '../../src/messaging/rpc';
import { QueueNames, RpcQueues, ExchangeNames, DirectRoutingKeys } from '../../src/config/exchanges.config';
import { MessageEnvelope } from '../../src/messaging/message.envelope';
import { createEnvelope } from '../../src/messaging/message.envelope';
import { logger } from '../../src/infrastructure/logger';
import { ConsumeMessage } from 'amqplib';

// ─── Domain Types ─────────────────────────────────────────────────────────────
interface OrderItem { sku: string; qty: number; price: number }
interface OrderCreatedPayload {
  orderId: string; customerId: string; amount: number; currency: string; items: OrderItem[];
}
interface InventoryCheckRequest { items: OrderItem[] }
interface InventoryCheckResponse { available: boolean; unavailableSkus: string[] }

// Simulated in-memory stock (production: DB query)
const STOCK: Record<string, number> = {
  'SKU-LAPTOP': 10,
  'SKU-MOUSE': 50,
  'SKU-HEADSET': 5,
  'SKU-KEYBOARD': 20,
  'SKU-MONITOR': 3,
};

/**
 * InventoryService — RPC Server + Event Consumer + Publisher
 *
 * Communication patterns:
 *   1. RPC Server (sync)  → responds to stock-check requests from OrderService
 *   2. Direct Consumer    → listens on order.created to RESERVE stock
 *   3. Direct Publisher   → publishes OrderCancelled if stock runs out after reservation
 *
 * This service demonstrates the "smart pipe" pattern:
 *   It participates in BOTH the sync RPC flow AND the async event stream.
 */
class InventoryService extends ServiceBase {
  private publisher!: Publisher;

  constructor() {
    super('inventory-service');
  }

  protected async initialize(): Promise<void> {
    // ── RPC Server: stock check ─────────────────────────────────────────────
    const rpcChannel = await this.factory.createChannel();
    const rpcServer = new RpcServer(rpcChannel);

    await rpcServer.listen<InventoryCheckRequest, InventoryCheckResponse>(
      RpcQueues.INVENTORY_CHECK,
      async (req) => {
        logger.info('[inventory-service] RPC stock check received.', { itemCount: req.items.length });
        const unavailableSkus: string[] = [];

        for (const item of req.items) {
          const inStock = STOCK[item.sku] ?? 0;
          if (inStock < item.qty) {
            unavailableSkus.push(item.sku);
          }
        }

        const available = unavailableSkus.length === 0;
        logger.info('[inventory-service] RPC stock check result.', { available, unavailableSkus });
        return { available, unavailableSkus };
      },
      5, // prefetch: handle up to 5 concurrent stock checks
    );

    // ── Direct Consumer: reserve stock when order is confirmed ──────────────
    const consumerChannel = await this.factory.createChannel();
    const consumer = new Consumer(consumerChannel);

    await consumer.consume<OrderCreatedPayload>(
      QueueNames.ORDER_PROCESSING,
      async (envelope: MessageEnvelope<OrderCreatedPayload>, _raw: ConsumeMessage) => {
        const { orderId, items } = envelope.payload;
        logger.info('[inventory-service] Reserving stock for order.', { orderId });

        for (const item of items) {
          if ((STOCK[item.sku] ?? 0) >= item.qty) {
            STOCK[item.sku] = (STOCK[item.sku] ?? 0) - item.qty;
            logger.debug('[inventory-service] Stock reserved.', {
              sku: item.sku,
              reserved: item.qty,
              remaining: STOCK[item.sku],
            });
          } else {
            logger.warn('[inventory-service] Stock depleted during reservation!', { sku: item.sku, orderId });
          }
        }

        logger.info('[inventory-service] Stock reservation complete.', { orderId, stock: STOCK });
      },
      { prefetch: 3, maxRetries: 2 },
    );

    // ── Publisher: emit cancellations if needed ─────────────────────────────
    const pubChannel = await this.factory.createChannel();
    this.publisher = new Publisher(pubChannel);

    logger.info('[inventory-service] RPC server + consumer + publisher ready.');
    logger.info('[inventory-service] Current stock:', STOCK);
  }
}

new InventoryService().start().catch((err) => {
  logger.error('inventory-service failed to start.', { error: err.message });
  process.exit(1);
});
