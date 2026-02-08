-- hasManyThrough: Country → User → Post
-- UK: 3 posts (via Alice+Bob), US: 1 post (via Charlie), France: 0 (Dave, no posts), Japan: 0 (no users)

drop table if exists th_post;
drop table if exists th_user;
drop table if exists th_country;

create table th_country (
    id bigint primary key,
    name varchar(100) not null
);

create table th_user (
    id bigint primary key,
    country_id bigint not null,
    name varchar(100) not null,
    foreign key (country_id) references th_country(id)
);

create table th_post (
    id bigint primary key,
    user_id bigint not null,
    title varchar(200) not null,
    foreign key (user_id) references th_user(id)
);

insert into th_country values (1, 'UK');
insert into th_country values (2, 'US');
insert into th_country values (3, 'France');
insert into th_country values (4, 'Japan');

insert into th_user values (1, 1, 'Alice');
insert into th_user values (2, 1, 'Bob');
insert into th_user values (3, 2, 'Charlie');
insert into th_user values (4, 3, 'Dave');

insert into th_post values (1, 1, 'Hello from Alice');
insert into th_post values (2, 1, 'Alice writes again');
insert into th_post values (3, 2, 'Bob here');
insert into th_post values (4, 3, 'Charlie says hi');

-- hasOneThrough: Mechanic → Car → Owner
-- Mech1: has owner (via Car1→OwnerA), Mech2: car but no owner, Mech3: no car

drop table if exists th_owner;
drop table if exists th_car;
drop table if exists th_mechanic;

create table th_mechanic (
    id bigint primary key,
    name varchar(100) not null
);

create table th_car (
    id bigint primary key,
    mechanic_id bigint not null,
    model varchar(100) not null,
    foreign key (mechanic_id) references th_mechanic(id)
);

create table th_owner (
    id bigint primary key,
    car_id bigint not null,
    name varchar(100) not null,
    foreign key (car_id) references th_car(id)
);

insert into th_mechanic values (1, 'Mech1');
insert into th_mechanic values (2, 'Mech2');
insert into th_mechanic values (3, 'Mech3');

insert into th_car values (1, 1, 'Toyota');
insert into th_car values (2, 2, 'Honda');

insert into th_owner values (1, 1, 'OwnerA');
