import { eq } from "drizzle-orm";
import { db } from "../../db/client.js";
import { orders, outboxEvents } from "../../db/schema.js";

interface CreateOrderInput {
	customerId: string;
	totalAmount: string;
}

export async function createOrder(input: CreateOrderInput) {
	return db.transaction(async (tx) => {
		const [order] = await tx
			.insert(orders)
			.values({
				customerId: input.customerId,
				totalAmount: input.totalAmount,
				status: "CREATED",
			})
			.returning();

		await tx.insert(outboxEvents).values({
			aggregatetype: "order",
			aggregateid: order.id,
			type: "OrderCreated",
			payload: {
				orderId: order.id,
				customerId: order.customerId,
				totalAmount: order.totalAmount,
				status: order.status,
				createdAt: order.createdAt,
			},
		});

		return order;
	});
}

export async function cancelOrder(orderId: string) {
	return db.transaction(async (tx) => {
		const [order] = await tx
			.update(orders)
			.set({ status: "CANCELLED", updatedAt: new Date() })
			.where(eq(orders.id, orderId))
			.returning();

		if (!order) {
			return null;
		}

		await tx.insert(outboxEvents).values({
			aggregatetype: "order",
			aggregateid: order.id,
			type: "OrderCancelled",
			payload: {
				orderId: order.id,
				status: order.status,
				updatedAt: order.updatedAt,
			},
		});

		return order;
	});
}
