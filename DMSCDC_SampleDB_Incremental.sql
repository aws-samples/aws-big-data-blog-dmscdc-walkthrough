use sampledb;

update product set name = 'Sample Product', dept = 'Sample Dept', category = 'Sample Category' where id = 1001;
delete from product where id = 1002;
call loadorders(1345, current_date);
insert into store values(1009, '125 Technology Dr.', 'Irvine', 'CA', 'US', '92618');
