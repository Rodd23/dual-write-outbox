import "dotenv/config";
import Fastify from "fastify";
import sensible from "@fastify/sensible";
import { orderRoutes } from "./modules/orders/order.routes.js";

const app = Fastify({ logger: true });

await app.register(sensible);
await app.register(orderRoutes);

app.get("/health", async () => ({ status: "ok" }));

const port = Number(process.env.PORT) || 3000;

app
	.listen({ port, host: "0.0.0.0" })
	.then(() => app.log.info(`Server is running on port ${port}`))
	.catch((err) => {
		app.log.error(err);
		process.exit(1);
	});
