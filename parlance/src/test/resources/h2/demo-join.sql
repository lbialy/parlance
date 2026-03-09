DROP TABLE IF EXISTS dmj_address;
DROP TABLE IF EXISTS dmj_profile;
DROP TABLE IF EXISTS dmj_user;

CREATE TABLE dmj_user (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP
);

CREATE TABLE dmj_profile (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    bio VARCHAR(200) NOT NULL,
    deleted_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES dmj_user(id)
);

CREATE TABLE dmj_address (
    id BIGINT PRIMARY KEY,
    profile_id BIGINT NOT NULL,
    city VARCHAR(100) NOT NULL,
    street VARCHAR(200) NOT NULL,
    zip_code VARCHAR(20) NOT NULL,
    deleted_at TIMESTAMP,
    FOREIGN KEY (profile_id) REFERENCES dmj_profile(id)
);

-- Users
-- Alice: active but soft-deleted → excluded by SoftDeletes
INSERT INTO dmj_user VALUES (1, 'Alice', TRUE, TIMESTAMP '2025-01-01 00:00:00', TIMESTAMP '2025-06-01 00:00:00');
-- Bob: active, not deleted → valid
INSERT INTO dmj_user VALUES (2, 'Bob', TRUE, TIMESTAMP '2025-02-01 00:00:00', NULL);
-- Carol: active, not deleted → valid
INSERT INTO dmj_user VALUES (3, 'Carol', TRUE, TIMESTAMP '2025-03-01 00:00:00', NULL);
-- Dave: inactive → excluded by ActiveOnly
INSERT INTO dmj_user VALUES (4, 'Dave', FALSE, TIMESTAMP '2025-04-01 00:00:00', NULL);
-- Eve: active, not deleted → valid
INSERT INTO dmj_user VALUES (5, 'Eve', TRUE, TIMESTAMP '2025-05-01 00:00:00', NULL);
-- Frank: active, not deleted but profile is soft-deleted → excluded at profile join
INSERT INTO dmj_user VALUES (6, 'Frank', TRUE, TIMESTAMP '2025-06-15 00:00:00', NULL);

-- Profiles
INSERT INTO dmj_profile VALUES (1, 1, 'Alice bio', NULL);
INSERT INTO dmj_profile VALUES (2, 2, 'Bob bio', NULL);
INSERT INTO dmj_profile VALUES (3, 3, 'Carol bio', NULL);
INSERT INTO dmj_profile VALUES (5, 5, 'Eve bio', NULL);
-- Frank's profile is soft-deleted
INSERT INTO dmj_profile VALUES (6, 6, 'Frank bio', TIMESTAMP '2025-07-01 00:00:00');

-- Addresses
-- Bob: 1 Warsaw, 1 Krakow
INSERT INTO dmj_address VALUES (1, 2, 'Warsaw', 'Marszalkowska', '00-001', NULL);
INSERT INTO dmj_address VALUES (2, 2, 'Krakow', 'Florianska', '31-019', NULL);
-- Carol: 1 Warsaw
INSERT INTO dmj_address VALUES (3, 3, 'Warsaw', 'Nowy Swiat', '00-500', NULL);
-- Eve: 2 Warsaw (different zip codes for ordering), 1 Gdansk, 1 soft-deleted Warsaw
INSERT INTO dmj_address VALUES (4, 5, 'Warsaw', 'Pulawska', '02-600', NULL);
INSERT INTO dmj_address VALUES (5, 5, 'Gdansk', 'Dluga', '80-827', NULL);
INSERT INTO dmj_address VALUES (6, 5, 'Warsaw', 'Mokotowska', '00-100', NULL);
INSERT INTO dmj_address VALUES (7, 5, 'Warsaw', 'Deleted Street', '00-999', TIMESTAMP '2025-08-01 00:00:00');
