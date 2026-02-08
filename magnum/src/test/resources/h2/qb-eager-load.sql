drop table if exists el_book;
drop table if exists el_author;

create table el_author (
    id bigint primary key,
    name varchar(100) not null
);

create table el_book (
    id bigint primary key,
    author_id bigint not null,
    title varchar(200) not null,
    foreign key (author_id) references el_author(id)
);

-- Tolkien: 2 books, Asimov: 2 books, Herbert: 1 book, Rowling: 0 books
insert into el_author values (1, 'Tolkien');
insert into el_author values (2, 'Asimov');
insert into el_author values (3, 'Herbert');
insert into el_author values (4, 'Rowling');

insert into el_book values (1, 1, 'The Hobbit');
insert into el_book values (2, 1, 'The Silmarillion');
insert into el_book values (3, 2, 'Foundation');
insert into el_book values (4, 2, 'I, Robot');
insert into el_book values (5, 3, 'Dune');
