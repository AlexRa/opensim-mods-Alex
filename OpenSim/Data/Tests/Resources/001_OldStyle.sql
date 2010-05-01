
create table if not exists version_test
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

if not exists(select * from version_test) then
  insert into version_test(id, magic) values('test', 1); 

update table version_test set v1 = magic;
