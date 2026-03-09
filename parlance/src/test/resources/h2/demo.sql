DROP TABLE IF EXISTS demo_order;
DROP TABLE IF EXISTS demo_address;
DROP TABLE IF EXISTS demo_profile;
DROP TABLE IF EXISTS demo_user;

CREATE TABLE demo_user (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE demo_profile (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    bio VARCHAR(200) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES demo_user(id)
);

CREATE TABLE demo_address (
    id BIGINT PRIMARY KEY,
    profile_id BIGINT NOT NULL,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(200) NOT NULL,
    FOREIGN KEY (profile_id) REFERENCES demo_profile(id)
);

CREATE TABLE demo_order (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product VARCHAR(200) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES demo_user(id)
);

-- Users (by created_at desc: Frank, Eve, Dave(inactive), Carol, Bob, Alice)
INSERT INTO demo_user VALUES (1, 'Alice', TRUE, TIMESTAMP '2025-01-01 00:00:00');
INSERT INTO demo_user VALUES (2, 'Bob', TRUE, TIMESTAMP '2025-02-01 00:00:00');
INSERT INTO demo_user VALUES (3, 'Carol', TRUE, TIMESTAMP '2025-03-01 00:00:00');
INSERT INTO demo_user VALUES (4, 'Dave', FALSE, TIMESTAMP '2025-04-01 00:00:00');
INSERT INTO demo_user VALUES (5, 'Eve', TRUE, TIMESTAMP '2025-05-01 00:00:00');
INSERT INTO demo_user VALUES (6, 'Frank', TRUE, TIMESTAMP '2025-06-01 00:00:00');

-- Profiles (one per active user)
INSERT INTO demo_profile VALUES (1, 1, 'Alice bio');
INSERT INTO demo_profile VALUES (2, 2, 'Bob bio');
INSERT INTO demo_profile VALUES (3, 3, 'Carol bio');
INSERT INTO demo_profile VALUES (5, 5, 'Eve bio');
INSERT INTO demo_profile VALUES (6, 6, 'Frank bio');

-- Addresses
INSERT INTO demo_address VALUES (1, 2, 'Warsaw', 'Marszalkowska');
INSERT INTO demo_address VALUES (2, 2, 'Krakow', 'Florianska');
INSERT INTO demo_address VALUES (3, 3, 'Warsaw', 'Nowy Swiat');
INSERT INTO demo_address VALUES (4, 5, 'Warsaw', 'Pulawska');
INSERT INTO demo_address VALUES (5, 5, 'Gdansk', 'Dluga');

-- Orders
INSERT INTO demo_order VALUES (1, 2, 'Widget');
INSERT INTO demo_order VALUES (2, 2, 'Gadget');
INSERT INTO demo_order VALUES (3, 3, 'Doohickey');
INSERT INTO demo_order VALUES (4, 5, 'Thingamajig');
