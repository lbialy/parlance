drop table if exists pv_user_role;
drop table if exists pv_role;
drop table if exists pv_user;
drop table if exists student_course;
drop table if exists course;
drop table if exists student;

create table pv_user (id bigint primary key, name varchar(100) not null);
create table pv_role (id bigint primary key, name varchar(100) not null);
create table pv_user_role (
    user_id bigint not null,
    role_id bigint not null,
    primary key (user_id, role_id),
    foreign key (user_id) references pv_user(id),
    foreign key (role_id) references pv_role(id)
);

-- Alice: admin+editor, Bob: editor, Charlie: 0 roles, Dave: all 3 roles
insert into pv_user values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'Dave');
insert into pv_role values (1, 'admin'), (2, 'editor'), (3, 'viewer');
insert into pv_user_role values (1, 1), (1, 2);
insert into pv_user_role values (2, 2);
insert into pv_user_role values (4, 1), (4, 2), (4, 3);

-- Convention-based tables (entity names: Student, Course -> pivot: student_course)
create table student (id bigint primary key, name varchar(100) not null);
create table course (id bigint primary key, title varchar(200) not null);
create table student_course (
    student_id bigint not null,
    course_id bigint not null,
    primary key (student_id, course_id),
    foreign key (student_id) references student(id),
    foreign key (course_id) references course(id)
);

insert into student values (1, 'Alice'), (2, 'Bob');
insert into course values (1, 'Math'), (2, 'Physics'), (3, 'History');
insert into student_course values (1, 1), (1, 2);
insert into student_course values (2, 2), (2, 3);
