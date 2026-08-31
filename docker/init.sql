-- Demo table. REPLICA IDENTITY FULL, so UPDATE/DELETE carry the full old
-- tuple (before_kind = "full").
CREATE TABLE public.users (
    id    BIGINT PRIMARY KEY,
    name  TEXT,
    email TEXT,
    bio   TEXT
);
ALTER TABLE public.users REPLICA IDENTITY FULL;

-- STORAGE EXTERNAL disables compression: any value larger than ~2 KB is
-- guaranteed to move out to TOAST. Without this, pglz would compress the
-- test string back into the row, and the 'u' marker would never appear in
-- an UPDATE.
ALTER TABLE public.users ALTER COLUMN bio SET STORAGE EXTERNAL;

-- Second table with REPLICA IDENTITY DEFAULT, to capture fixtures with
-- before_kind = "key" (only the PK in the old tuple).
CREATE TABLE public.items (
    id    BIGINT PRIMARY KEY,
    title TEXT,
    qty   INT
);

CREATE PUBLICATION pgcdc_pub FOR TABLE public.users, public.items;

SELECT pg_create_logical_replication_slot('pgcdc_slot', 'pgoutput');
