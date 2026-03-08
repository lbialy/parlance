drop table if exists via_contact;
drop table if exists via_post;
drop table if exists via_book;
drop table if exists via_author;

create table via_author (
    id bigint primary key,
    name varchar(100) not null
);

create table via_post (
    id bigint primary key,
    author_id bigint not null,
    title varchar(200) not null,
    foreign key (author_id) references via_author(id)
);

create table via_book (
    id bigint primary key,
    author_id bigint not null,
    title varchar(200) not null,
    foreign key (author_id) references via_author(id)
);

create table via_contact (
    id bigint primary key,
    author_id bigint not null,
    email varchar(200) not null,
    active boolean not null default true,
    foreign key (author_id) references via_author(id)
);

-- Alice: 2 posts, 2 contacts (1 active, 1 inactive)
insert into via_author values (1, 'Alice');
insert into via_author values (2, 'Bob');
insert into via_author values (3, 'Carol');

insert into via_post values (1, 1, 'Alice Post 1');
insert into via_post values (2, 1, 'Alice Post 2');
insert into via_post values (3, 2, 'Bob Post 1');

insert into via_book values (1, 2, 'Bob Book 1');
insert into via_book values (2, 2, 'Bob Book 2');

insert into via_contact values (1, 1, 'alice@example.com', true);
insert into via_contact values (2, 1, 'alice-old@example.com', false);
insert into via_contact values (3, 2, 'bob@example.com', true);
