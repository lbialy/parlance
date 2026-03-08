drop table if exists qb_counter;
create table qb_counter (
    id bigint primary key,
    name varchar(100) not null,
    status varchar(20) not null,
    view_count bigint not null default 0,
    score int not null default 0
);
insert into qb_counter values (1, 'Alpha', 'active', 10, 100);
insert into qb_counter values (2, 'Beta', 'active', 20, 200);
insert into qb_counter values (3, 'Gamma', 'archived', 30, 300);
insert into qb_counter values (4, 'Delta', 'draft', 0, 0);
