-- Authors and books (for Relationship tests)
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

-- Users, roles, pivot (for BelongsToMany tests)
drop table if exists pv_user_role;
drop table if exists pv_role;
drop table if exists pv_user;

create table pv_user (
    id bigint primary key,
    name varchar(100) not null
);

create table pv_role (
    id bigint primary key,
    name varchar(100) not null
);

create table pv_user_role (
    user_id bigint not null,
    role_id bigint not null,
    primary key (user_id, role_id),
    foreign key (user_id) references pv_user(id),
    foreign key (role_id) references pv_role(id)
);

-- Alice: admin+editor, Bob: editor, Charlie: no roles, Dave: admin+editor+viewer
insert into pv_user values (1, 'Alice');
insert into pv_user values (2, 'Bob');
insert into pv_user values (3, 'Charlie');
insert into pv_user values (4, 'Dave');

insert into pv_role values (1, 'admin');
insert into pv_role values (2, 'editor');
insert into pv_role values (3, 'viewer');

insert into pv_user_role values (1, 1);
insert into pv_user_role values (1, 2);
insert into pv_user_role values (2, 2);
insert into pv_user_role values (4, 1);
insert into pv_user_role values (4, 2);
insert into pv_user_role values (4, 3);
