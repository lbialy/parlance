-- Bug 1a: Pivot first() stitching inconsistency
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
    -- No FK constraint: returns by PK
);

insert into bg_country values (1, 'Testland');
insert into bg_person values (10, 1, 'Alice'), (20, 1, 'Bob');
insert into bg_article values (10, 10, 'Alice first'), (20, 20, 'Bob writes'), (30, 10, 'Alice second');
