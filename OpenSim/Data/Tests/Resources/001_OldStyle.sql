drop table if exists version_test;

create table version_test
(
	id char(36) not null primary key,
	magic int,
	v1 int,
	v2 int,
	v3 int,
	v4 int,
	v5 int,
	v6 int
);

insert into version_test values('test', 1, 1, 0, 0, 0, 0, 0); 
