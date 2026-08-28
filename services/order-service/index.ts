import * as dotenv from 'dotenv';
dotenv.config();

import { ServiceBase } from '../shared/service.base';
import { Publisher } from '../../src/messaging/publisher';
import { RpcClient } from '../../src/messaging/rpc';
import { ExchangeNames, DirectRoutingKeys, RpcQueues } from '../../src/config/exchanges.config';
import { createEnvelope } from '../../src/messaging/message.envelope';
import { logger } from '../../src/infrastructure/logger';
import { v4 as uuidv4 } from 'uuid';

// ─── Domain Types ─────────────────────────────────────────────────────────────
interface OrderItem { sku: string; qty: number; price: number }

interface CreateOrderRequest {
  customerId: string;
  items: OrderItem[];
}

interface InventoryCheckRequest { items: OrderItem[] }
interface InventoryCheckResponse {
  available: boolean;
  unavailableSkus: string[];
}

/**
 * OrderService — Event Publisher + RPC Client
 *
 * Communication patterns demonstrated:
 *   1. RPC (sync-like)    → calls InventoryService to check stock BEFORE confirming
 *   2. Direct Exchange    → publishes OrderCreated to payment & inventory queues
 *   3. Fanout Exchange    → broadcasts OrderCreated so audit + analytics get it too
 *
 * Flow:
 *   createOrder()
 *     ├─ [RPC]    inventory-service.checkStock(items) → { available: true }
 *     ├─ [DIRECT] app.direct / order.created  → payment-service
 *     └─ [FANOUT] app.fanout / ""             → audit-service, notification-service
 */
class OrderService extends ServiceBase {
  private publisher!: Publisher;
  private fanoutPublisher!: Publisher;
  private rpcClient!: RpcClient;

  constructor() {
    super('order-service');
  }

  protected async initialize(): Promise<void> {
    // Dedicated channel per publisher (best practice)
    const pubChannel = await this.factory.createChannel();
    this.publisher = new Publisher(pubChannel);

    const fanoutChannel = await this.factory.createChannel();
    this.fanoutPublisher = new Publisher(fanoutChannel);

    // RPC client on its own channel
    const rpcChannel = await this.factory.createChannel();
    this.rpcClient = new RpcClient(rpcChannel, 8000);
    await this.rpcClient.start();

    logger.info('[order-service] Publisher and RPC client ready.');

    // Simulate placing 3 orders with a delay between each
    await this.simulateOrders();
  }

  private async simulateOrders(): Promise<void> {
    const orders: CreateOrderRequest[] = [
      {
        customerId: 'CUST-001',
        items: [{ sku: 'SKU-LAPTOP', qty: 1, price: 1299.99 }, { sku: 'SKU-MOUSE', qty: 2, price: 29.99 }],
      },
      {
        customerId: 'CUST-002',
        items: [{ sku: 'SKU-HEADSET', qty: 1, price: 199.99 }],
      },
      {
        customerId: 'CUST-003',
        items: [{ sku: 'SKU-KEYBOARD', qty: 1, price: 89.99 }, { sku: 'SKU-MONITOR', qty: 1, price: 449.99 }],
      },
    ];

    for (const order of orders) {
      await this.sleep(1500);
      await this.createOrder(order).catch((err) =>
        logger.error('[order-service] Order failed.', { error: err.message }),
      );
    }

    logger.info('[order-service] All simulated orders placed. Keeping alive for further messages...');
    // Keep process alive (in production this would be triggered by HTTP/gRPC)
    await new Promise<void>(() => {});
  }

  private async createOrder(req: CreateOrderRequest): Promise<void> {
    const correlationId = uuidv4();
    const orderId = `ORD-${uuidv4().substring(0, 8).toUpperCase()}`;

    logger.info('[order-service] Creating order...', { orderId, customerId: req.customerId });

    // ── Step 1: RPC — Check inventory synchronously BEFORE confirming order ──
    logger.info('[order-service] Checking inventory via RPC...', { orderId });

    const stockCheck = await this.rpcClient.call<InventoryCheckRequest, InventoryCheckResponse>(
      RpcQueues.INVENTORY_CHECK,
      { items: req.items },
    );

    if (!stockCheck.available) {
      logger.warn('[order-service] Order rejected — insufficient stock.', {
        orderId,
        unavailableSkus: stockCheck.unavailableSkus,
      });
      return;
    }

    logger.info('[order-service] Inventory confirmed. Publishing OrderCreated.', { orderId });

    const amount = req.items.reduce((sum, i) => sum + i.price * i.qty, 0);
    const payload = { orderId, customerId: req.customerId, amount, currency: 'USD', items: req.items };

    // ── Step 2: Direct Exchange → PaymentService receives via order.created ──
    const directEnvelope = createEnvelope('OrderCreated', payload, correlationId);
    await this.publisher.publish(ExchangeNames.DIRECT, DirectRoutingKeys.ORDER_CREATED, directEnvelope);

    // ── Step 3: Fanout → AuditService + NotificationService receive the same event ──
    const fanoutEnvelope = createEnvelope('OrderCreated', { orderId, customerId: req.customerId, amount }, correlationId);
    await this.fanoutPublisher.publish(ExchangeNames.FANOUT, '', fanoutEnvelope);

    logger.info('[order-service] Order created and broadcast successfully.', { orderId });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

new OrderService().start().catch((err) => {
  logger.error('order-service failed to start.', { error: err.message });
  process.exit(1);
});
