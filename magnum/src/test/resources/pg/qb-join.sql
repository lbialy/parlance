drop table if exists jn_book;
drop table if exists jn_author;

create table jn_author (
    id bigint primary key,
    name varchar(100) not null
);

create table jn_book (
    id bigint primary key,
    author_id bigint not null,
    title varchar(200) not null,
    foreign key (author_id) references jn_author(id)
);

insert into jn_author values (1, 'Tolkien');
insert into jn_author values (2, 'Asimov');
insert into jn_author values (3, 'Herbert');

insert into jn_book values (1, 1, 'The Hobbit');
insert into jn_book values (2, 1, 'The Silmarillion');
insert into jn_book values (3, 2, 'Foundation');
insert into jn_book values (4, 2, 'I, Robot');
insert into jn_book values (5, 3, 'Dune');
