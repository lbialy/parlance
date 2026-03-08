drop table if exists qb_user;

create table qb_user (
    id bigint primary key,
    first_name varchar(100),
    age int not null
);

insert into qb_user values (1, 'Alice', 25);
insert into qb_user values (2, 'Bob', 30);
insert into qb_user values (3, 'Charlie', 17);
insert into qb_user values (4, null, 22);
