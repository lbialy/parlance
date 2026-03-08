drop table if exists qb_paginated;

create table qb_paginated (
    id bigint primary key,
    name varchar(100) not null,
    score int not null,
    created_at timestamp not null
);

insert into qb_paginated values (1, 'Alice', 90, '2024-01-01 10:00:00');
insert into qb_paginated values (2, 'Bob', 85, '2024-01-02 11:00:00');
insert into qb_paginated values (3, 'Charlie', 70, '2024-01-03 12:00:00');
insert into qb_paginated values (4, 'Diana', 95, '2024-01-04 13:00:00');
insert into qb_paginated values (5, 'Eve', 60, '2024-01-05 14:00:00');
insert into qb_paginated values (6, 'Frank', 80, '2024-01-06 15:00:00');
insert into qb_paginated values (7, 'Grace', 75, '2024-01-07 16:00:00');
insert into qb_paginated values (8, 'Hank', 55, '2024-01-08 17:00:00');
insert into qb_paginated values (9, 'Ivy', 88, '2024-01-09 18:00:00');
insert into qb_paginated values (10, 'Jack', 65, '2024-01-10 19:00:00');
