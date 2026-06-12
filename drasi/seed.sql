-- Seed schema + data for the Drasi MCP Events demo.
-- Runs once at first container start via /docker-entrypoint-initdb.d/.
-- The docker entrypoint executes this against database "demo" as user "demo"
-- (a superuser, so it already has REPLICATION privileges).

CREATE TABLE orders (
    id       serial PRIMARY KEY,
    customer text    NOT NULL,
    total    numeric NOT NULL,
    status   text    NOT NULL
);

-- FULL replica identity makes UPDATE/DELETE WAL records carry the complete
-- old row, which keeps the Drasi source's change events self-contained.
ALTER TABLE orders REPLICA IDENTITY FULL;

-- Rows both above and below the high-value threshold (total > 1000).
INSERT INTO orders (customer, total, status) VALUES
    ('alice', 1500.00, 'open'),      -- above threshold: in query result set
    ('bob',    250.00, 'open'),      -- below threshold
    ('carol', 2200.50, 'shipped'),   -- above threshold: in query result set
    ('dave',   999.99, 'pending');   -- below threshold (boundary, NOT > 1000)

-- The Drasi postgres source consumes logical replication through this
-- publication (publicationName in server.yaml). It must exist before the
-- source starts; the replication slot ("drasi_slot") is created automatically
-- by the source itself (CREATE_REPLICATION_SLOT, idempotent on restart).
CREATE PUBLICATION drasi_publication FOR TABLE orders;
