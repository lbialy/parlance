drop table if exists qb_product;

create table qb_product (
    id bigint primary key,
    name varchar(100) not null,
    price int not null
);

insert into qb_product values (1, 'Apple', 10);
insert into qb_product values (2, 'Banana', 20);
insert into qb_product values (3, 'apricot', 15);
insert into qb_product values (4, 'Cherry', 25);
