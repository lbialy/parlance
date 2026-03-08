drop table if exists upsert_item;
create table upsert_item (
    id bigint primary key,
    name varchar(100) not null,
    amount int not null default 0,
    unique(name)
);
insert into upsert_item values (1, 'Alpha', 10);
insert into upsert_item values (2, 'Beta', 20);
