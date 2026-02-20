-- Bug 1a: Pivot first() stitching inconsistency
-- Pivot table has NO unique constraint, allowing duplicate entries
drop table if exists bg_user_role;
drop table if exists bg_role;
drop table if exists bg_user;

create table bg_user (id bigint primary key, name varchar(100) not null);
create table bg_role (id bigint primary key, name varchar(100) not null);
create table bg_user_role (
    user_id bigint not null,
    role_id bigint not null,
    foreign key (user_id) references bg_user(id),
    foreign key (role_id) references bg_role(id)
);

insert into bg_user values (1, 'Eve');
insert into bg_role values (1, 'alpha'), (2, 'beta'), (3, 'gamma');
-- Duplicate pivot entry: Eve -> alpha appears twice
insert into bg_user_role values (1, 1), (1, 1);

-- Bug 1a: Through first() ordering inconsistency
-- Multiple articles per person, with PK ordering that differs from stitching order.
-- NO FK constraint on bg_article so H2 returns by PK order (not FK index order).
drop table if exists bg_article;
drop table if exists bg_person;
drop table if exists bg_country;

create table bg_country (id bigint primary key, name varchar(100) not null);
create table bg_person (
    id bigint primary key,
    country_id bigint not null,
    name varchar(100) not null,
    foreign key (country_id) references bg_country(id)
);
create table bg_article (
    id bigint primary key,
    person_id bigint not null,
    title varchar(200) not null
    -- No FK constraint: H2 won't have a person_id index, returns by PK
);

-- Person 10 (Alice) has articles 10 and 30
-- Person 20 (Bob) has article 20
-- Intermediate pairs by person PK: [(1,10), (1,20)]
-- run() stitches: pair(1,10)->[A10,A30], pair(1,20)->[A20] = [A10, A30, A20]
-- first() fetchTargets by article PK: [A10, A20, A30] = different interleaving!
insert into bg_country values (1, 'Testland');
insert into bg_person values (10, 1, 'Alice'), (20, 1, 'Bob');
insert into bg_article values (10, 10, 'Alice first'), (20, 20, 'Bob writes'), (30, 10, 'Alice second');
