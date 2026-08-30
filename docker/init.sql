-- Демонстрационная таблица. REPLICA IDENTITY FULL, чтобы в UPDATE/DELETE
-- приходил полный старый кортеж (before_kind = "full").
CREATE TABLE public.users (
    id    BIGINT PRIMARY KEY,
    name  TEXT,
    email TEXT,
    bio   TEXT
);
ALTER TABLE public.users REPLICA IDENTITY FULL;

-- STORAGE EXTERNAL отключает сжатие: любое значение больше ~2 КБ
-- гарантированно уезжает в TOAST. Без этого pglz сожмёт тестовую строку
-- обратно в строку, и маркер 'u' в UPDATE никогда не появится.
ALTER TABLE public.users ALTER COLUMN bio SET STORAGE EXTERNAL;

-- Вторая таблица с REPLICA IDENTITY DEFAULT, чтобы снять фикстуры
-- с before_kind = "key" (в старом кортеже только PK).
CREATE TABLE public.items (
    id    BIGINT PRIMARY KEY,
    title TEXT,
    qty   INT
);

CREATE PUBLICATION pgcdc_pub FOR TABLE public.users, public.items;

SELECT pg_create_logical_replication_slot('pgcdc_slot', 'pgoutput');
