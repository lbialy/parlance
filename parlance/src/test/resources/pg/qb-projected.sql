drop table if exists qb_order;

create table qb_order (
    id bigint primary key,
    customer varchar(100) not null,
    status varchar(20) not null,
    amount int not null
);

insert into qb_order values (1, 'Alice', 'paid', 100);
insert into qb_order values (2, 'Alice', 'paid', 200);
insert into qb_order values (3, 'Bob', 'paid', 150);
insert into qb_order values (4, 'Bob', 'pending', 50);
insert into qb_order values (5, 'Charlie', 'pending', 75);
insert into qb_order values (6, 'Charlie', 'paid', 300);
insert into qb_order values (7, 'Charlie', 'paid', 100);
