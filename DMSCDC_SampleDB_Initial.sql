create schema if not exists sampledb;

drop table if exists sampledb.store;
create table sampledb.store(
id int,
address1 varchar(1024),
city varchar(255),
state varchar(2),
countrycode varchar(2),
postcode varchar(10));

insert into sampledb.store (id, address1, city, state, countrycode, postcode) values
(1001, '320 W. 100th Ave, 100, Southgate Shopping Ctr - Anchorage','Anchorage','AK','US','99515'),
(1002, '1005 E Dimond Blvd','Anchorage','AK','US','99515'),
(1003, '1771 East Parks Hwy, Unit #4','Wasilla','AK','US','99654'),
(1004, '345 South Colonial Dr','Alabaster','AL','US','35007'),
(1005, '700 Montgomery Hwy, Suite 100','Vestavia Hills','AL','US','35216'),
(1006, '20701 I-30','Benton','AR','US','72015'),
(1007, '2034 Fayetteville Rd','Van Buren','AR','US','72956'),
(1008, '3640 W. Anthem Way','Anthem','AZ','US','85086');

drop table if exists sampledb.product;
create table sampledb.product(
id int ,
name varchar(255),
dept varchar(100),
category varchar(100),
price decimal(10,2));

insert into sampledb.product (id, name, dept, category, price) values
(1001,'Fire 7','Amazon Devices','Fire Tablets',39),
(1002,'Fire HD 8','Amazon Devices','Fire Tablets',89),
(1003,'Fire HD 10','Amazon Devices','Fire Tablets',119),
(1004,'Fire 7 Kids Edition','Amazon Devices','Fire Tablets',79),
(1005,'Fire 8 Kids Edition','Amazon Devices','Fire Tablets',99),
(1006,'Fire HD 10 Kids Edition','Amazon Devices','Fire Tablets',159),
(1007,'Fire TV Stick','Amazon Devices','Fire TV',49),
(1008,'Fire TV Stick 4K','Amazon Devices','Fire TV',49),
(1009,'Fire TV Cube','Amazon Devices','Fire TV',119),
(1010,'Kindle','Amazon Devices','Kindle E-readers',79),
(1011,'Kindle Paperwhite','Amazon Devices','Kindle E-readers',129),
(1012,'Kindle Oasis','Amazon Devices','Kindle E-readers',279),
(1013,'Echo Dot (3rd Gen)','Amazon Devices','Echo and Alexa',49),
(1014,'Echo Auto','Amazon Devices','Echo and Alexa',24),
(1015,'Echo Show (2nd Gen)','Amazon Devices','Echo and Alexa',229),
(1016,'Echo Plus (2nd Gen)','Amazon Devices','Echo and Alexa',149),
(1017,'Fire TV Recast','Amazon Devices','Echo and Alexa',229),
(1018,'EchoSub Bundle with 2 Echo (2nd Gen)','Amazon Devices','Echo and Alexa',279),
(1019,'Mini Projector','Electronics','Projectors',288),
(1020,'TOUMEI Mini Projector','Electronics','Projectors',300),
(1021,'InFocus IN114XA Projector, DLP XGA 3600 Lumens 3D Ready 2HDMI Speakers','Electronics','Projectors',350),
(1022,'Optoma X343 3600 Lumens XGA DLP Projector with 15,000-hour Lamp Life','Electronics','Projectors',339),
(1023,'TCL 55S517 55-Inch 4K Ultra HD Roku Smart LED TV (2018 Model)','Electronics','TV',379),
(1024,'Samsung 55NU7100 Flat 55” 4K UHD 7 Series Smart TV 2018','Electronics','TV',547),
(1025,'LG Electronics 55UK6300PUE 55-Inch 4K Ultra HD Smart LED TV (2018 Model)','Electronics','TV',496);


drop table if exists sampledb.productorder;
create table sampledb.productorder (
id int NOT NULL,
productid int,
storeid int,
qty int,
soldprice decimal(10,2),
create_dt date);

DROP PROCEDURE IF EXISTS sampledb.loadorders;
CREATE PROCEDURE sampledb.loadorders (
  IN OrderCnt int,
  IN create_dt date
)
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE maxid INT Default 0;
  SELECT coalesce(max(id),0) INTO maxid from sampledb.productorder;
  helper: LOOP
    IF i<OrderCnt THEN
      SET i = i+1;
      INSERT INTO sampledb.productorder(id, productid, storeid, qty, soldprice, create_dt)
         values (maxid+i,1000+ceil(rand()*25), 1000+ceil(rand()*8), ceil(rand()*10), rand()*100, create_dt) ;
      ITERATE helper;
    ELSE
      LEAVE  helper;
    END IF;
  END LOOP;
END;

truncate table sampledb.productorder;
call sampledb.loadorders(1010, '2018-11-27');
call sampledb.loadorders(1196, '2018-12-03');
call sampledb.loadorders(4250, '2018-12-10');
call sampledb.loadorders(1246, '2018-12-13');
call sampledb.loadorders(723, '2018-11-28');
