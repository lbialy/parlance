drop table if exists mj_book;
drop table if exists mj_author;
drop table if exists mj_publisher;
drop table if exists mj_country;

create table mj_country (
    id bigint primary key,
    name varchar(100) not null
);

create table mj_publisher (
    id bigint primary key,
    name varchar(100) not null
);

create table mj_author (
    id bigint primary key,
    name varchar(100) not null,
    country_id bigint not null,
    foreign key (country_id) references mj_country(id)
);

create table mj_book (
    id bigint primary key,
    author_id bigint not null,
    publisher_id bigint not null,
    title varchar(200) not null,
    foreign key (author_id) references mj_author(id),
    foreign key (publisher_id) references mj_publisher(id)
);

insert into mj_country values (1, 'UK'), (2, 'USA'), (3, 'Russia');
insert into mj_publisher values (1, 'Allen & Unwin'), (2, 'Gnome Press'), (3, 'Chilton Books');
insert into mj_author values (1, 'Tolkien', 1), (2, 'Asimov', 2), (3, 'Herbert', 2);
insert into mj_book values (1, 1, 1, 'The Hobbit'), (2, 1, 1, 'The Silmarillion'),
                            (3, 2, 2, 'Foundation'), (4, 2, 2, 'I, Robot'),
                            (5, 3, 3, 'Dune');
