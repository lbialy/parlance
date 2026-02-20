drop table if exists repo_item;

create table repo_item (
    id bigint primary key,
    name varchar(100) not null,
    status varchar(20) not null,
    amount int not null
);

insert into repo_item values (1, 'Alpha', 'active', 10);
insert into repo_item values (2, 'Beta', 'active', 20);
insert into repo_item values (3, 'Gamma', 'inactive', 30);
insert into repo_item values (4, 'Delta', 'active', 40);
insert into repo_item values (5, 'Epsilon', 'inactive', 50);
