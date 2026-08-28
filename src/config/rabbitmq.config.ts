import * as dotenv from 'dotenv';
dotenv.config();

/**
 * Centralized RabbitMQ configuration.
 * Single Responsibility: owns all AMQP connection & channel settings.
 */
export interface RabbitMQConfig {
  url: string;
  heartbeat: number;
  prefetch: number;
}

export const rabbitmqConfig: RabbitMQConfig = {
  url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
  heartbeat: parseInt(process.env.RABBITMQ_HEARTBEAT ?? '60', 10),
  prefetch: parseInt(process.env.RABBITMQ_PREFETCH ?? '10', 10),
};
