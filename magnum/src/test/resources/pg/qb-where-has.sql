-- Authors, books, and reviews (for Relationship tests)
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
    body varchar(500),
    foreign key (book_id) references el_book(id)
);

-- The Hobbit: score 5, The Silmarillion: score 3, Foundation: score 4, Dune: score 5
insert into el_review values (1, 1, 5, 'Great');
insert into el_review values (2, 2, 3, 'Dense');
insert into el_review values (3, 3, 4, 'Classic');
insert into el_review values (4, 5, 5, 'Masterpiece');

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

-- Eloquent-style deep nesting test:
drop table if exists cl_trip_user;
drop table if exists cl_trip_owner;
drop table if exists cl_checklist_item;
drop table if exists cl_checklist;
drop table if exists cl_trip;
drop table if exists cl_person;

create table cl_person (
    id bigint primary key,
    name varchar(100) not null
);

create table cl_trip (
    id bigint primary key,
    destination varchar(200) not null
);

create table cl_trip_owner (
    trip_id bigint not null,
    person_id bigint not null,
    primary key (trip_id, person_id),
    foreign key (trip_id) references cl_trip(id),
    foreign key (person_id) references cl_person(id)
);

create table cl_trip_user (
    trip_id bigint not null,
    person_id bigint not null,
    primary key (trip_id, person_id),
    foreign key (trip_id) references cl_trip(id),
    foreign key (person_id) references cl_person(id)
);

create table cl_checklist (
    id bigint primary key,
    trip_id bigint not null,
    title varchar(200) not null,
    foreign key (trip_id) references cl_trip(id)
);

create table cl_checklist_item (
    id bigint primary key,
    checklist_id bigint not null,
    form_id bigint not null,
    description varchar(500) not null,
    foreign key (checklist_id) references cl_checklist(id)
);

-- People
insert into cl_person values (1, 'Alice');
insert into cl_person values (2, 'Bob');
insert into cl_person values (3, 'Charlie');

-- Trips
insert into cl_trip values (1, 'Paris');
insert into cl_trip values (2, 'Tokyo');
insert into cl_trip values (3, 'London');

-- Trip owners
insert into cl_trip_owner values (1, 1);
insert into cl_trip_owner values (2, 2);
insert into cl_trip_owner values (3, 3);

-- Trip users
insert into cl_trip_user values (1, 2);
insert into cl_trip_user values (2, 3);

-- Checklists
insert into cl_checklist values (1, 1, 'Paris packing');
insert into cl_checklist values (2, 2, 'Tokyo packing');
insert into cl_checklist values (3, 3, 'London packing');

-- Checklist items (form_id groups them)
insert into cl_checklist_item values (1, 1, 100, 'Pack passport');
insert into cl_checklist_item values (2, 1, 100, 'Pack sunscreen');
insert into cl_checklist_item values (3, 2, 100, 'Pack umbrella');
insert into cl_checklist_item values (4, 3, 200, 'Pack jacket');
insert into cl_checklist_item values (5, 3, 100, 'Pack tea');
