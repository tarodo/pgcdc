-- Fixture 1: single INSERT (RELATION + BEGIN + INSERT + COMMIT)
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);

-- Fixture 2: UPDATE under REPLICA IDENTITY FULL, full old tuple ('O')
UPDATE users SET name = 'Bob' WHERE id = 1;

-- Fixture 3: DELETE under REPLICA IDENTITY FULL
DELETE FROM users WHERE id = 1;

-- Fixture 4: REPLICA IDENTITY DEFAULT. UPDATE carries no old tuple,
-- DELETE carries only the key ('K')
INSERT INTO items VALUES (10, 'Widget', 5);
UPDATE items SET qty = 7 WHERE id = 10;
DELETE FROM items WHERE id = 10;

-- Fixture 5: TOAST. bio has STORAGE EXTERNAL, so there's no compression
-- and the 9600 characters are guaranteed to move out of the row.
INSERT INTO users
SELECT 2, 'Carol', 'carol@example.com',
       (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300));
-- UPDATE doesn't touch bio, so the new tuple will carry the 'u' marker
UPDATE users SET name = 'Caroline' WHERE id = 2;

-- Fixture 6: multi-statement transaction
BEGIN;
INSERT INTO users VALUES (3, 'Dave', 'dave@example.com', NULL);
UPDATE users SET email = 'dave2@example.com' WHERE id = 3;
DELETE FROM users WHERE id = 3;
COMMIT;

-- Fixture 7: rollback. Nothing at all should arrive.
BEGIN;
INSERT INTO users VALUES (999, 'Ghost', 'ghost@example.com', NULL);
ROLLBACK;
