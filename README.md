# dual-write-outbox

Implementação de referência do **padrão Transactional Outbox** usando **Fastify**, **PostgreSQL**, **Drizzle ORM** e **Debezium** (CDC) publicando eventos no **Kafka**.

O objetivo do projeto é resolver o clássico problema de "dual write": como garantir que uma mudança de estado no banco de dados e a publicação do evento correspondente aconteçam de forma atômica, sem depender de um 2PC (two-phase commit) entre o banco e o message broker.

## Índice

- [O problema que o projeto resolve](#o-problema-que-o-projeto-resolve)
- [Como funciona](#como-funciona)
- [Stack](#stack)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Como rodar](#como-rodar)
- [API](#api)
- [Testando o fluxo ponta a ponta](#testando-o-fluxo-ponta-a-ponta)
- [Detalhes de implementação](#detalhes-de-implementação)
- [Scripts disponíveis](#scripts-disponíveis)

## O problema que o projeto resolve

Em sistemas orientados a eventos é comum precisar fazer duas coisas ao mesmo tempo:

1. Persistir uma mudança de estado (ex: criar um pedido) no banco de dados.
2. Publicar um evento correspondente (ex: `OrderCreated`) em um message broker (ex: Kafka) para que outros serviços reajam.

Se essas duas escritas forem feitas separadamente — primeiro o commit no banco, depois o publish no Kafka — existe uma janela em que uma pode ter sucesso e a outra falhar (queda do processo, timeout de rede, etc.), deixando o sistema em um estado inconsistente. Esse é o problema do **dual write**.

O **Transactional Outbox Pattern** resolve isso escrevendo o evento em uma tabela `outbox_events` **na mesma transação SQL** que a escrita de domínio. Como as duas escritas fazem parte da mesma transação, elas são atômicas: ou as duas acontecem, ou nenhuma acontece. Um processo separado (nesse caso, o **Debezium**, via Change Data Capture) lê o WAL do Postgres, detecta as novas linhas na tabela `outbox_events` e as publica no Kafka de forma assíncrona e confiável.

## Como funciona

```
┌─────────────┐        1 transação SQL        ┌──────────────────────┐
│   Cliente   │  POST /orders                 │      PostgreSQL      │
│  (HTTP)     ├───────────────────────────────▶│  ┌─────────────┐    │
└─────────────┘                                │  │   orders    │    │
                                                │  └─────────────┘    │
                                                │  ┌─────────────┐    │
                                                │  │outbox_events│    │
                                                │  └──────┬──────┘    │
                                                └─────────┼───────────┘
                                                          │ WAL (logical replication)
                                                          ▼
                                                ┌──────────────────────┐
                                                │  Debezium Connector   │
                                                │  (Kafka Connect)      │
                                                │  EventRouter SMT      │
                                                └──────────┬────────────┘
                                                           ▼
                                                ┌──────────────────────┐
                                                │        Kafka          │
                                                │ tópico: outbox.event. │
                                                │         <aggregate>   │
                                                └──────────────────────┘
```

1. O cliente chama `POST /orders`.
2. O `order.service.ts` abre **uma única transação** no Postgres e insere:
   - a linha de domínio na tabela `orders`;
   - o evento correspondente (`OrderCreated`) na tabela `outbox_events`.
3. O Postgres roda com `wal_level=logical`, o que permite ao Debezium ler as mudanças via *logical replication slot* (`dwo_outbox_slot`).
4. O conector Debezium (`register-outbox-connector.json`) monitora apenas a tabela `outbox_events` (`table.include.list`) e usa a transformação **[Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)** para:
   - rotear cada evento para um tópico Kafka com base na coluna `aggregate_type` (`outbox.event.order`);
   - usar `aggregate_id` como chave da mensagem, garantindo ordenação por agregado;
   - publicar o conteúdo da coluna `payload` como o corpo (value) da mensagem.
5. Consumidores externos (fora do escopo deste repo) leem o tópico Kafka e reagem ao evento.

A tabela `outbox_events` funciona como uma fila de "eventos pendentes de publicação" só do ponto de vista lógico — na prática ela nunca precisa ser limpa manualmente pela aplicação: o Debezium lê o WAL, não a tabela em si, então o writer não sabe nem se importa se o Debezium já processou a linha.

## Stack

| Camada              | Tecnologia                                             |
|----------------------|--------------------------------------------------------|
| HTTP server          | [Fastify 5](https://fastify.dev/)                       |
| Validação            | [Zod 4](https://zod.dev/)                               |
| ORM / migrations     | [Drizzle ORM](https://orm.drizzle.team/) + Drizzle Kit  |
| Banco de dados        | PostgreSQL 16 (imagem `debezium/postgres`, com `wal_level=logical`) |
| CDC / Outbox relay    | [Debezium](https://debezium.io/) 2.7 (Kafka Connect)     |
| Message broker         | Apache Kafka (modo KRaft, sem Zookeeper)                 |
| Linguagem             | TypeScript (executado com `tsx`)                         |
| Orquestração local     | Docker Compose                                          |

## Estrutura do projeto

```
.
├── docker-compose.yml            # Postgres + Kafka + Kafka Connect (Debezium)
├── debezium/
│   ├── register-outbox-connector.json     # payload para registrar o conector
│   └── update-outbox-connector-config.json# payload para atualizar a config do conector
├── drizzle/                      # migrations geradas pelo drizzle-kit
├── drizzle.config.ts             # config do drizzle-kit (schema, output, dialect)
├── src/
│   ├── server.ts                 # bootstrap do Fastify
│   ├── db/
│   │   ├── client.ts             # pool do pg + instância do Drizzle
│   │   ├── schema.ts             # tabelas `orders` e `outbox_events`
│   │   └── migrate.ts            # script para rodar as migrations
│   └── modules/
│       └── orders/
│           ├── order.routes.ts   # rotas HTTP (POST /orders, POST /orders/:id/cancel)
│           └── order.service.ts  # regra de negócio + escrita transacional no outbox
└── package.json
```

## Pré-requisitos

- [Node.js](https://nodejs.org/) 20+
- [Docker](https://www.docker.com/) e Docker Compose
- `curl` (ou Postman/Insomnia) para testar a API e registrar o conector Debezium

## Como rodar

### 1. Subir a infraestrutura (Postgres + Kafka + Kafka Connect)

```bash
docker compose up -d
```

Isso sobe três serviços:

- `dwo-postgres` — Postgres na porta `5432`, já configurado com `wal_level=logical` (necessário para CDC).
- `dwo-kafka` — Kafka em modo KRaft (sem Zookeeper), exposto em `localhost:9092`.
- `dwo-connect` — Kafka Connect (imagem `debezium/connect`), exposto em `localhost:8083`, onde o conector Debezium será registrado.

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

O valor padrão já aponta para o Postgres subido pelo Docker Compose acima.

### 3. Instalar dependências e rodar as migrations

```bash
npm install
npm run db:migrate
```

Isso cria as tabelas `orders` e `outbox_events` no banco.

### 4. Registrar o conector Debezium

Com o Kafka Connect no ar (`http://localhost:8083`), registre o conector que fará o CDC da tabela `outbox_events`:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/register-outbox-connector.json
```

Para checar o status do conector:

```bash
curl http://localhost:8083/connectors/orders-outbox-connector/status
```

Para atualizar a configuração de um conector já registrado (usa apenas o objeto `config`, sem o `name`):

```bash
curl -X PUT http://localhost:8083/connectors/orders-outbox-connector/config \
  -H "Content-Type: application/json" \
  -d @debezium/update-outbox-connector-config.json
```

### 5. Subir a API

```bash
npm run dev
```

O servidor sobe em `http://localhost:3000` (ou na porta definida em `PORT`).

## API

### `GET /health`

Healthcheck simples.

```
200 OK
{ "status": "ok" }
```

### `POST /orders`

Cria um pedido. Na mesma transação, grava o registro em `orders` e o evento `OrderCreated` em `outbox_events`.

**Body:**

```json
{
  "customerId": "5f8d0d55-6c1e-4b0e-9a0b-3f0c2e4a1234",
  "totalAmount": "199.90"
}
```

**Resposta `201 Created`:**

```json
{
  "id": "…",
  "customerId": "5f8d0d55-6c1e-4b0e-9a0b-3f0c2e4a1234",
  "status": "CREATED",
  "totalAmount": "199.90",
  "createdAt": "...",
  "updatedAt": "..."
}
```

### `POST /orders/:id/cancel`

Cancela um pedido existente. Na mesma transação, atualiza o `status` para `CANCELLED` em `orders` e grava o evento `OrderCancelled` em `outbox_events`.

- `200 OK` com o pedido atualizado, se encontrado.
- `404 Not Found` (`{ "message": "Pedido não encontrado" }`), se o `id` não existir.

## Testando o fluxo ponta a ponta

1. Crie um tópico consumidor de teste (opcional — o Kafka Connect já cria o tópico automaticamente no primeiro evento):

   ```bash
   docker exec -it dwo-kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic outbox.event.order \
     --from-beginning
   ```

2. Em outro terminal, crie um pedido:

   ```bash
   curl -X POST http://localhost:3000/orders \
     -H "Content-Type: application/json" \
     -d '{"customerId": "5f8d0d55-6c1e-4b0e-9a0b-3f0c2e4a1234", "totalAmount": "199.90"}'
   ```

3. A mensagem `OrderCreated` deve aparecer no consumidor do Kafka poucos instantes depois — o tempo entre o commit no Postgres e a mensagem chegar no Kafka é o *lag* do CDC, tipicamente na casa dos milissegundos.

## Detalhes de implementação

- **Atomicidade:** `createOrder` e `cancelOrder` (em [order.service.ts](src/modules/orders/order.service.ts)) usam `db.transaction(...)` do Drizzle para garantir que a escrita de domínio e a escrita no outbox sejam atômicas.
- **Formato da tabela outbox:** o schema de `outbox_events` segue exatamente o formato esperado pelo [Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html) do Debezium: `aggregatetype`, `aggregateid`, `type`, `payload`, `timestamp`.
- **Roteamento de tópico:** `transforms.outbox.route.topic.replacement` usa o valor de `aggregate_type` para montar o nome do tópico (`outbox.event.order`), então novos agregados (ex: `payment`, `shipment`) publicam automaticamente em tópicos próprios sem precisar reconfigurar o conector.
- **Ordenação:** `aggregate_id` é usado como chave da mensagem Kafka, o que garante que todas as mensagens de um mesmo agregado (ex: mesmo pedido) caem na mesma partição e são consumidas em ordem.
- **A aplicação nunca lê a tabela outbox de volta** — ela só escreve. A leitura/consumo é responsabilidade do Debezium (via WAL) e dos consumidores Kafka a jusante, o que mantém a tabela desacoplada da lógica de negócio.

## Scripts disponíveis

| Script               | Descrição                                          |
|-----------------------|-----------------------------------------------------|
| `npm run dev`          | Sobe a API em modo watch (`tsx watch src/server.ts`) |
| `npm run build`        | Compila o TypeScript para `dist/`                    |
| `npm start`            | Roda a build compilada (`node dist/server.js`)       |
| `npm run db:generate`  | Gera uma nova migration a partir do `schema.ts`      |
| `npm run db:migrate`   | Aplica as migrations pendentes no banco               |
| `npm run db:studio`    | Abre o [Drizzle Studio](https://orm.drizzle.team/drizzle-studio/overview) para inspecionar o banco |
