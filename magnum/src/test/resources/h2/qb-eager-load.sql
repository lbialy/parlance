drop table if exists el_review;
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

create table el_review (
    id bigint primary key,
    book_id bigint not null,
    score int not null,
    body varchar(200) not null,
    foreign key (book_id) references el_book(id)
);

-- Reviews: The Hobbit has 2, Foundation has 1, Dune has 1
insert into el_review values (1, 1, 5, 'A masterpiece');
insert into el_review values (2, 1, 4, 'Very good');
insert into el_review values (3, 3, 5, 'Brilliant');
insert into el_review values (4, 5, 3, 'Dense but rewarding');
