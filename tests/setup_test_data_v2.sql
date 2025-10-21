-- SQL script to create test DuckLake databases with delete files
-- Run this with DuckDB to generate test data

INSTALL ducklake;
LOAD ducklake;

-- Test 1: Table without delete files (backward compatibility)
ATTACH 'ducklake:tests/test_data/no_deletes.ducklake' AS no_deletes;

CREATE TABLE no_deletes.users (
    id INT,
    name VARCHAR,
    email VARCHAR
);

INSERT INTO no_deletes.users VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com'),
    (3, 'Charlie', 'charlie@example.com'),
    (4, 'Diana', 'diana@example.com');

SELECT * FROM no_deletes.users;

-- Test 2: Table with DELETE operations (creates delete files)
ATTACH 'ducklake:tests/test_data/with_deletes.ducklake' AS with_deletes;

CREATE TABLE with_deletes.products (
    id INT,
    name VARCHAR,
    price DECIMAL(10,2),
    in_stock BOOLEAN
);

INSERT INTO with_deletes.products VALUES
    (1, 'Laptop', 999.99, true),
    (2, 'Mouse', 25.50, true),
    (3, 'Keyboard', 75.00, true),
    (4, 'Monitor', 299.99, false),
    (5, 'Webcam', 89.99, true);

-- Delete some rows to create delete files
DELETE FROM with_deletes.products WHERE id = 2;
DELETE FROM with_deletes.products WHERE id = 4;

SELECT 'After deletes:' AS info;
SELECT * FROM with_deletes.products;

-- Test 3: Table with UPDATE operations (creates delete + insert)
ATTACH 'ducklake:tests/test_data/with_updates.ducklake' AS with_updates;

CREATE TABLE with_updates.inventory (
    id INT,
    product_name VARCHAR,
    quantity INT,
    last_updated TIMESTAMP
);

INSERT INTO with_updates.inventory VALUES
    (1, 'Widget A', 100, '2024-01-01 10:00:00'),
    (2, 'Widget B', 200, '2024-01-01 10:00:00'),
    (3, 'Widget C', 150, '2024-01-01 10:00:00');

-- Update some rows (creates delete files for old versions)
UPDATE with_updates.inventory SET quantity = 120, last_updated = '2024-01-02 15:30:00' WHERE id = 1;
UPDATE with_updates.inventory SET quantity = 180, last_updated = '2024-01-02 16:00:00' WHERE id = 3;

SELECT 'After updates:' AS info;
SELECT * FROM with_updates.inventory ORDER BY id;

-- Test 4: Filter pushdown correctness with deletes
-- Verifies that WHERE filters are applied AFTER delete filtering
ATTACH 'ducklake:tests/test_data/filter_pushdown.ducklake' AS filter_pushdown;

CREATE TABLE filter_pushdown.items (
    id INT,
    value VARCHAR
);

-- Insert 5 rows with IDs [1,2,3,4,5]
INSERT INTO filter_pushdown.items VALUES
    (1, 'one'),
    (2, 'two'),
    (3, 'three'),
    (4, 'four'),
    (5, 'five');

-- Delete row with id=3 (position 2 in the file, 0-indexed)
DELETE FROM filter_pushdown.items WHERE id = 3;

SELECT 'After delete (id=3):' AS info;
SELECT * FROM filter_pushdown.items ORDER BY id;
