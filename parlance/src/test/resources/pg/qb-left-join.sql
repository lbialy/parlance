drop table if exists lj_book;
drop table if exists lj_author;
drop table if exists lj_country;

create table lj_country (
    id bigint primary key,
    name varchar(100) not null
);

create table lj_author (
    id bigint primary key,
    name varchar(100) not null,
    country_id bigint,
    foreign key (country_id) references lj_country(id)
);

create table lj_book (
    id bigint primary key,
    author_id bigint,
    title varchar(200) not null,
    foreign key (author_id) references lj_author(id)
);

insert into lj_country values (1, 'UK'), (2, 'USA');

insert into lj_author values (1, 'Tolkien', 1);
insert into lj_author values (2, 'Asimov', 2);
insert into lj_author values (3, 'Lonely', null);

insert into lj_book values (1, 1, 'The Hobbit');
insert into lj_book values (2, 2, 'Foundation');
insert into lj_book values (3, null, 'Anonymous Tales');
