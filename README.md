# Load ongoing data lake changes with AWS DMS and AWS Glue
**AWS Big Data Blog Post Code Walkthrough**

The AWS Big Data blog post [Load ongoing data lake changes with AWS DMS and AWS Glue](https://aws.amazon.com/blogs/big-data/loading-ongoing-data-lake-changes-with-aws-dms-and-aws-glue/) demonstrates how to deploy a solution that loads ongoing changes from popular database sources into your data lake. The solution streams new and changed data into Amazon S3. It also creates and updates appropriate data lake objects, providing a source-similar view of the data based on a schedule you configure.

This Github project describes in detail the Glue code components used in the blog post.   In addition, it includes detailed steps to setup the RDS DB mentioned in the blog post to demonstrate the solution.

[Glue Jobs](#glue-jobs)
 * [Controller](#controller) - [DMSCDC_Controller.py](./DMSCDC_Controller.py)
 * [LoadInitial](#loadinitial) - [DMSCDC_LoadInitial.py](./DMSCDC_LoadInitial.py)
 * [LoadIncremental](#loadincremental) - [DMSCDC_LoadIncremental.py](./DMSCDC_LoadIncremental.py)

[Sample Database](#sample-database)
 * [MySQL RDS Settings](#mysql-rds-settings)
 * [Initial Load Script](#initial-load-script) - [DMSCDC_SampleDB_Initial.sql](./DMSCDC_SampleDB_Initial.sql)
 * [Incremental Load Script](#incremental-load-script) - [DMSCDC_SampleDB_Incremental.sql](./DMSCDC_SampleDB_Incremental.sql)

## Glue Jobs
This solution uses a Python shell job for the Controller. The controller will run each time the workflow is triggered but the LoadInitial and LoadIncremental are only executed as needed.

### Controller
The controller job will leverage the state table stored in DynamoDB and compare the state with the files in S3.  It will determine which tables/files need to be processed.

The first step in the processing is to determine the *folders* which are located in DMS raw location and construct and item object representing the default values for the folder.  Each *folder* would represent a table in your source database which will have changes streamed into S3.  
```python
#get the list of table folders
s3_input = 's3://'+bucket+'/'+prefix
url = urlparse.urlparse(s3_input)
folders = s3conn.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/').get('CommonPrefixes')

index = 0
max = len(folders)

for folder in folders:
    full_folder = folder['Prefix']
    folder = full_folder[len(prefix):]
    path = bucket + '/' + full_folder
    item = {
        'path': {'S':path},
        'bucket': {'S':bucket},
        'prefix': {'S':prefix},
        'folder': {'S':folder},
        'PrimaryKey': {'S':'null'},
        'PartitionKey': {'S':'null'},
        'LastFullLoadDate': {'S':'1900-01-01 00:00:00'},
        'LastIncrementalFile': {'S':path + '0.parquet'},
        'ActiveFlag': {'S':'false'}}
```

The next step is to pull the state of each table from the DMSCDC_Controller DynamoDB table.  If the table is not present the table will be created.  If the row is not present the row will be created with the default values set earlier.
```python
    try:
        response1 = ddbconn.describe_table(TableName='DMSCDC_Controller')
    except Exception as e:
        ddbconn.create_table(
            TableName='DMSCDC_Controller',
            KeySchema=[{'AttributeName': 'path','KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'path','AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 1,'WriteCapacityUnits': 1})

    try:
        response = ddbconn.get_item(
          TableName='DMSCDC_Controller',
          Key={'path': {'S':path}})
        if 'Item' in response:
            item = response['Item']
        else:
            ddbconn.put_item(
                TableName='DMSCDC_Controller',
                Item=item)
    except:
        ddbconn.put_item(
            TableName='DMSCDC_Controller',
            Item=item)

    partitionKey = item['PartitionKey']['S']
    lastFullLoadDate = item['LastFullLoadDate']['S']
    lastIncrementalFile = item['LastIncrementalFile']['S']
    activeFlag = item['ActiveFlag']['S']
    primaryKey = item['PrimaryKey']['S']
```

In the next step, the controller determines if the initial file needs to be processed by comparing the timestamp of the initial load file stored in the DynamoDB table with the timestamp retrieved from S3.  If it is determined that the initial file should be processed the [LoadInitial](#LoadInitial) glue job is triggered.  The controller will wait for its completion using the *testGlueJob* function. If the job completes successfully, the DynamoDB table will be updated with the timestamp of the file which was processed.

Note: if the DynamoDB table has the ActiveFlag != 'true', this table will be skipped.  This is to ensure that a user has reviewed the table and set appropriate keys. If the PartitionKey has been set, the Spark job will partition the data before it is written.  If the PrimaryKey has been set, the incremental load will conduct change detection logic.
```python
    loadInitial = False
    #determine if need to run initial --> Run Initial --> Update DDB
    initialfiles = s3conn.list_objects(Bucket=bucket, Prefix=full_folder+'LOAD').get('Contents')
    if activeFlag == 'true' :
      if initialfiles is not None :
          s3FileTS = initialfiles[0]['LastModified'].replace(tzinfo=None)
          ddbFileTS = datetime.datetime.strptime(lastFullLoadDate, '%Y-%m-%d %H:%M:%S')
          if s3FileTS > ddbFileTS:
              message='Starting to process Initial file.'
              loadInitial = True
              lastFullLoadDate = datetime.datetime.strftime(s3FileTS,'%Y-%m-%d %H:%M:%S')
          else:
              message='Intial files already processed.'
      else:
          message='No initial files to process.'
    else:
      message='Load is not active.  Update DynamoDB.'
    print(message)

    #Call Initial Glue Job for this source
    if loadInitial:
        response = glue.start_job_run(
            JobName='DMSCDC_LoadInitial',
            Arguments={
                '--bucket':bucket,
                '--prefix':prefix,
                '--folder':folder,
                '--out_path':out_path,
                '--partitionKey':partitionKey})

        #Wait for execution complete, timeout in 20*30=900 secs, if successful, update ddb
        if testGlueJob(response['JobRunId'], 30, 30, 'DMSCDC_LoadInitial') != 1:
            message = 'Error during Controller execution'
            raise ValueError(message)
        else:
            ddbconn.update_item(
                TableName='DMSCDC_Controller',
                Key={"path": {"S":path}},
                AttributeUpdates={"LastFullLoadDate": {"Value": {"S": lastFullLoadDate}}})
```
In the final step, the controller determines if any incremental files need to be processed by comparing the *lastIncrementalFile* stored in the DynamoDB table with the *newIncrementalFile* retrieved from S3.  If those two values are different it is determined that there are incremental files and the [LoadIncremental](#LoadIncremental) glue job is triggered.  The controller will wait for its completion using the *testGlueJob* function. If the job completes successfully, the DynamoDB table will be updated with the *newIncrementalFile* of the file which was processed.
```python
    loadIncremental = False
    newIncrementalFile = path + '0.csv'

    if activeFlag == 'true':
      incrementalFiles = s3conn.list_objects(Bucket=bucket, Prefix=full_folder+'2').get('Contents')
      if incrementalFiles is not None:
          filecount = len(incrementalFiles)
          newIncrementalFile = bucket + '/' + incrementalFiles[filecount-1]['Key']
          if newIncrementalFile != lastIncrementalFile:
              loadIncremental = True
              message = "Starting to process incremental files"
          else:
              message = "Incremental files already processed."
      else:
          message = "No incremental files to process."
    else:
      message = "Load is not active.  Update DynamoDB."
    print(message)

    if loadIncremental:
        response = glue.start_job_run(
            JobName='DMSCDC_LoadIncremental',
            Arguments={
                '--bucket':bucket,
                '--prefix':prefix,
                '--folder':folder,
                '--out_path':out_path,
                '--partitionKey':partitionKey,
                '--lastIncrementalFile' : lastIncrementalFile,
                '--newIncrementalFile' : newIncrementalFile,
                '--primaryKey' : primaryKey
                })

        if testGlueJob(response['JobRunId'], 30, 30, 'DMSCDC_LoadIncremental') != 1:
            message = 'Error during Controller execution'
            raise ValueError(message)
        else:
            ddbconn.update_item(
                TableName='DMSCDC_Controller',
                Key={"path": {"S":path}},
                AttributeUpdates={"LastIncrementalFile": {"Value": {"S": newIncrementalFile}}})
```

### LoadInitial
The initial load will first read the initial load file into a Spark DataFrame. An additional column will be added to the dataframe *"Op"* with a default value of *I* representing *inserts*.  This field is added to provide parity between the initial load and the incremental load.     
```python
input = spark.read.parquet(s3_inputpath+"/LOAD*.parquet").withColumn("Op", lit("I"))
```
If a partition key has been set for this table, the data will be written to the Data Lake in the appropriate partitions.  

Note: When the initial job runs, any previously loaded data is overwritten.
```python
partition_keys = args['partitionKey']
if partition_keys != "null" :
    partitionKeys = partition_keys.split(",")
    input.repartition(partitionKeys[0]).write.mode('overwrite').partitionBy(partitionKeys).parquet(s3_outputpath)
else:
    input.write.mode('overwrite').parquet(s3_outputpath)
```

### LoadIncremental
The incremental load will first read only the new incremental files into a Spark DataFrame.  
```python
last_file = args['lastIncrementalFile']
curr_file = args['newIncrementalFile']
primary_keys = args['primaryKey']
partition_keys = args['partitionKey']

inputfile = spark.read.parquet(s3_inputpath+"/2*.parquet")

inputfile.filter(input_file_name() > last_file)
inputfile.filter(input_file_name() <= curr_file)
```
Next, the process will determine if a primary key has been set in the DynamoDB table.  If it has been set, the change detection process will need to be implemented.  Otherwise, all records marked for insert (where the *Op* = 'I') will be prepped to be written.
```python
if primary_keys == "null":
    output = inputfile.filter(inputfile.Op=='I')
    filelist = [["null"]]
  else:
      primaryKeys = primary_keys.split(",")
```
To process the changed records the process will de-dup the input records as well as determine the impacted data lake files.
First, a few metadata fields are added to the input data and the already loaded data:
* sortpath - For already loaded data, this is set to "0" to ensure that newly loaded data with the same primary key will override it.  For new data this is the filename generated by DMS.
* filepath - For already loaded data, this is the file name of the data lake file.  For new data this is the filename generated by DMS.
* rownum - This is the relative rownumber of each primary key. For already loaded data, this will always be '1' as there should always be only 1 record for each primary key.  For new data data, this will be a value from 1-to-N for each primary key.  In the case when there are multiple operations on the same primary key, the latest operation will have the greatest rownum value.

```python    
    windowRow = Window.partitionBy(primaryKeys).orderBy(desc("sortpath"))

    target = spark.read.parquet(s3_outputpath).withColumn("sortpath", lit("0")).withColumn("filepath",input_file_name()).withColumn("rownum", lit(1))
    input = inputfile.withColumn("sortpath", input_file_name()).withColumn("filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))
```
To determine which data lake files will be impacted, the input data is joined to the target data and  a distinct list of files is captured.  
```python
    files = target.join(inputfile, primaryKeys, 'inner').select(col("filepath").alias("filepath1")).distinct()
```
Since S3 data is immutable, not only the records which have changed will need to be written, but also all the unchanged records from the impacted data lake files.  To accomplish this task, a new dataframe will be created which is union of the source and target data.
```python
    uniondata = input.select(target.columns).union(target.join(files,files.filepath1==target.filepath).select(target.columns))
```
To determine which rows to write from the union dataframe, a *rnk* column is added indicating the last change for a given primary leveraging the *sortpath* and *rownum* fields which were added.  In addition, only records where the last operation is not a delete (*Op != D*) will be filtered.
```python
    window = Window.partitionBy(primaryKeys).orderBy(desc("sortpath"), desc("rownum"))
    output = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile.columns)
```
If a partition key has been set for this table, the data will be written to the Data Lake in the appropriate partitions.  
```python
if partition_keys != "null" :
    partitionKeys = partition_keys.split(",")
    output.repartition(partitionKeys[0]).write.mode('append').partitionBy(partitionKeys).parquet(s3_outputpath)
else:
    output.write.mode('append').parquet(s3_outputpath)
```
Finally, the impacted data lake files containing the changed records need to be deleted.  

Note: Because S3 does not guarantee consistency on delete operations, until the file is actually removed from S3, there is a possibility of users seeing duplicate data temporarily.
```python
filelist = files.collect()
for row in filelist:
    if row[0] != "null":
        o = urlparse.urlparse(row[0])
        s3conn.delete_object(Bucket=o.netloc, Key=urllib.unquote(o.path)[1:])
```

## Sample Database
Below is a sample MySQL database you can build and use in conjunction with this project.  It can be used to demonstrate the end-to-end flow of data into your Data Lake.

### MySQL RDS Settings
In order for a DB to be a candidate for DMS as a source it needs to meet certain [requirements](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.html) whether it's managed through Amazon via RDS or if it's an EC2 or on-premise installation.  

In this example, assume we have launched an *RDS* database of type *MySQL*.  After the database has been launched, perform the following actions:
1. Create a new [Parameter Group](https://console.aws.amazon.com/rds/home?#parameter-groups:) based on the version of MySQL which you are using.
1. Modify the parameter group parameters setting the following
 * binlog_format: "ROW"
 * binlog_checksum: "NONE"
1. Modify the database to use the newly created parameter group.

### Initial Load Script
#### Create Table Script
```sql
create schema sampledb;
use sampledb;

drop table store;
create table store(
id int,
address1 varchar(1024),
city varchar(255),
state varchar(2),
countrycode varchar(2),
postcode varchar(10));

drop table product;
create table product(
id int ,
name varchar(255),
dept varchar(100),
category varchar(100),
price decimal(10,2));

drop table productorder;
create table productorder (
id int NOT NULL AUTO_INCREMENT,
productid int,
storeid int,
qty int,
soldprice decimal(10,2),
create_dt date,
PRIMARY KEY (id));
```
#### Reference Data Load Script
```sql
insert into store (id, address1, city, state, countrycode, postcode) values
(1001, '320 W. 100th Ave, 100, Southgate Shopping Ctr - Anchorage','Anchorage','AK','US','99515'),
(1002, '1005 E Dimond Blvd','Anchorage','AK','US','99515'),
(1003, '1771 East Parks Hwy, Unit #4','Wasilla','AK','US','99654'),
(1004, '345 South Colonial Dr','Alabaster','AL','US','35007'),
(1005, '700 Montgomery Hwy, Suite 100','Vestavia Hills','AL','US','35216'),
(1006, '20701 I-30','Benton','AR','US','72015'),
(1007, '2034 Fayetteville Rd','Van Buren','AR','US','72956'),
(1008, '3640 W. Anthem Way','Anthem','AZ','US','85086');

insert into product (id, name, dept, category, price) values
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

```
#### Transaction Data Load Script
To populate the transaction data, use the procedure to insert *OrderCnt* rows each with a random value for *Store* and *Product*.
```sql
CREATE PROCEDURE loadorders (
  IN OrderCnt int,
  IN create_dt date
)
BEGIN
  DECLARE i INT DEFAULT 0;
  helper: LOOP
    IF i<OrderCnt THEN
      INSERT INTO productorder(productid, storeid, qty, soldprice, create_dt)
         values (1000+ceil(rand()*25), 1000+ceil(rand()*8), ceil(rand()*10), rand()*100, create_dt) ;
      SET i = i+1;
      ITERATE helper;
    ELSE
      LEAVE  helper;
    END IF;     
  END LOOP;
END
```
```sql
call loadorders(1010, '2018-11-27');
call loadorders(1196, '2018-12-03');
call loadorders(4250, '2018-12-10');
call loadorders(1246, '2018-12-13');
call loadorders(723, '2018-11-28');
```
### Incremental Load Script
Before proceeding with the incremental load, ensure that your initial data has arrived in the Data Lake.  By default the solution will run on an hourly basis on the 00 minute.  To change the load frequency or minute, modify the **[Glue Trigger](https://console.aws.amazon.com/glue/home?#etl:tab=triggers)**.  Once the seed data has been loaded, execute the following script in the MySQL database to simulate changed data.
```sql
update product set name = 'Sample Product', dept = 'Sample Dept', category = 'Sample Category' where id = 1001;
delete from product where id = 1002;
call loadorders(1345, current_date);
insert into store values(1009, '125 Technology Dr.', 'Irvine', 'CA', 'US', '92618');
```

Once DMS has processed these changes, you will see new incremental files for store, product, and productorder.  On the next execution of the Glue job, you will see new and updated files in your data lake.
