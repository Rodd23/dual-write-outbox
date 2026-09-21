import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { createOrder, cancelOrder } from "./order.service.js";

const createOrderSchema = z.object({
    customerId: z.string().uuid(),
    totalAmount: z.string().regex(/^\d+(\.\d{1,2})?$/, "Formato inválido, use ex: 199.90"),
});

export async function orderRoutes(app: FastifyInstance) {
    app.post("/orders", async (request, reply) => {
        const body = createOrderSchema.parse(request.body);
        const order = await createOrder(body);
        return reply.status(201).send(order);
    });

    app.post("/orders/:id/cancel", async (request, reply) => {
        const { id } = z.object({ id: z.string().uuid() }).parse(request.params);
        const order = await cancelOrder(id);

        if(!order) {
            return reply.status(404).send({ message: "Pedido não encontrado" });
        }

        return reply.status(200).send(order);
    })
}