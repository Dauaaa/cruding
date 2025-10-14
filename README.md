# TODOs

- [ ] implement a worker that subscribes to WAL and has interface like: subscribe to these Models and these events and is responsible for parsing the WAL into some concrete rust struct
- [ ] implement a worker that subscribes to redis topics
- [ ] think of a way to solve workflows that should be done only once but many handlers could try doing it
    - gossip (all handlers share some state and coordinate)
    - specialized handlers (have specialized handlers that work in parallel to CRUD handlers)
- [ ] implement automatic outbox table that registers events on commit using postgres constructs, example:
```pgsql
CREATE TABLE event_outbox (
  id           bigserial PRIMARY KEY,
  topic        text NOT NULL,           -- e.g., "customer:123"
  event_type   text NOT NULL,           -- e.g., "order.created"
  entity_id    text NOT NULL,
  payload      jsonb,                   -- keep small; enrich later
  commit_ts    timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);

CREATE INDEX ON event_outbox (published_at) WHERE published_at IS NULL;

CREATE OR REPLACE FUNCTION orders_after_ins_outbox()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO event_outbox (topic, event_type, entity_id, payload)
  SELECT format('customer:%s', o.customer_id),
         'order.created',
         o.id::text,
         to_jsonb(o) - 'internal_field'
  FROM new_rows o;

  -- one poke per statement, not per row
  PERFORM pg_notify('outbox_poke', 'orders');
  RETURN NULL;
END$$;

CREATE TRIGGER orders_after_ins
AFTER INSERT ON orders
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION orders_after_ins_outbox();
```
- [ ] think of a way to implement notification for clients (for live updates)
