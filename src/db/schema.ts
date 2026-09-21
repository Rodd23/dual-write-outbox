import { pgTable, uuid, varchar, numeric, timestamp, jsonb  } from "drizzle-orm/pg-core";

/**
 * Tabela de domínio "normal". Representa o dado real da aplicação.
 * É escrita na MESMA transação que a tabela outbox_events.
 */

export const orders = pgTable("orders", {
    id: uuid("id").primaryKey().defaultRandom(),
    customerId: uuid("customer_id").notNull(),
    status: varchar("status", { length: 32 }).notNull().default("CREATED"),
    totalAmount: numeric("total_amount", { precision: 12, scale: 2 }).notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
});

/**
 * Tabela Outbox seguindo o formato esperado pelo Debezium Outbox Event Router:
 * https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
 *
 * - aggregatetype: usado pelo Debezium para rotear o evento para o tópico Kafka
 *   (ex: tópico final = "outbox.event.order")
 * - aggregateid: usado como chave da mensagem Kafka (garante ordenação por agregado)
 * - type: tipo do evento (ex: "OrderCreated", "OrderCancelled")
 * - payload: corpo do evento em JSON, publicado como valor da mensagem Kafka
 */

export const outboxEvents = pgTable("outbox_events", {
    id: uuid("id").primaryKey().defaultRandom(),
    aggregatetype: varchar("aggregate_type", { length: 64 }).notNull(),
    aggregateid: varchar("aggregate_id", { length: 64 }).notNull(),
    type: varchar("type", { length: 64 }).notNull(),
    payload: jsonb("payload").notNull(),
    timestamp: timestamp("timestamp", { withTimezone: true }).notNull().defaultNow(),
});