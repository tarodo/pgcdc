-- Фикстура 1: одиночный INSERT (RELATION + BEGIN + INSERT + COMMIT)
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', NULL);

-- Фикстура 2: UPDATE при REPLICA IDENTITY FULL, старый кортеж целиком ('O')
UPDATE users SET name = 'Bob' WHERE id = 1;

-- Фикстура 3: DELETE при REPLICA IDENTITY FULL
DELETE FROM users WHERE id = 1;

-- Фикстура 4: REPLICA IDENTITY DEFAULT. В UPDATE старого кортежа нет,
-- в DELETE приходит только ключ ('K')
INSERT INTO items VALUES (10, 'Widget', 5);
UPDATE items SET qty = 7 WHERE id = 10;
DELETE FROM items WHERE id = 10;

-- Фикстура 5: TOAST. bio имеет STORAGE EXTERNAL, значит сжатия нет
-- и 9600 символов гарантированно уезжают из строки.
INSERT INTO users
SELECT 2, 'Carol', 'carol@example.com',
       (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 300));
-- UPDATE не трогает bio, значит в новом кортеже придёт маркер 'u'
UPDATE users SET name = 'Caroline' WHERE id = 2;

-- Фикстура 6: многострочная транзакция
BEGIN;
INSERT INTO users VALUES (3, 'Dave', 'dave@example.com', NULL);
UPDATE users SET email = 'dave2@example.com' WHERE id = 3;
DELETE FROM users WHERE id = 3;
COMMIT;

-- Фикстура 7: откат. Не должно прийти вообще ничего.
BEGIN;
INSERT INTO users VALUES (999, 'Ghost', 'ghost@example.com', NULL);
ROLLBACK;
