import "dotenv/config";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import { db, pool } from "./client.js";

async function main() {
    console.log("Rodando migrations...");
    await migrate(db, { migrationsFolder: "drizzle" });
    console.log("Migrations finalizadas!");
    await pool.end();
}

main().catch((err) => {
    console.error("Erro ao rodar migrations:", err);
    process.exit(1);
});
