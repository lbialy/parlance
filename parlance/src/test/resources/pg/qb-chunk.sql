DROP TABLE IF EXISTS qb_item;

CREATE TABLE qb_item (
    id BIGINT PRIMARY KEY,
    amount INT NOT NULL
);

-- Insert 100 rows (id 1-100, amount = id * 10)
INSERT INTO qb_item
SELECT x, x * 10 FROM generate_series(1, 100) AS x;
