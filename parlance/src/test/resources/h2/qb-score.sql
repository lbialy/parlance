drop table if exists qb_score;

create table qb_score (
    id bigint primary key,
    score_a int not null,
    score_b int not null
);

insert into qb_score values (1, 10, 20);
insert into qb_score values (2, 30, 30);
insert into qb_score values (3, 50, 40);
insert into qb_score values (4, 15, 15);
