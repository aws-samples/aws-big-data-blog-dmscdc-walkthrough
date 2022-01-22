# Load ongoing data lake changes with AWS DMS and AWS Glue
**AWS Big Data Blog Post Code Walkthrough**

The AWS Big Data blog post [Load ongoing data lake changes with AWS DMS and AWS Glue](https://aws.amazon.com/blogs/big-data/loading-ongoing-data-lake-changes-with-aws-dms-and-aws-glue/) demonstrates how to deploy a solution that loads ongoing changes from popular database sources into your data lake. The solution streams new and changed data into Amazon S3. It also creates and updates appropriate data lake objects, providing a source-similar view of the data based on a schedule you configure.

This Github project describes in detail the Glue code components used in the blog post.   In addition, it includes detailed steps to setup the RDS DB mentioned in the blog post to demonstrate the solution.

> 01/22/2022: This solution was updated to resolve some key bugs and implement certain performance related enhancements. See the [change log](CHANGELOG.md) for more details.

[Glue Jobs](#glue-jobs)
 * [Controller](#controller) - [DMSCDC_Controller.py](./DMSCDC_Controller.py)
 * [ProcessTable](#processtable) - [DMSCDC_ProcessTable.py](./DMSCDC_ProcessTable.py)
 * [LoadInitial](#loadinitial) - [DMSCDC_LoadInitial.py](./DMSCDC_LoadInitial.py)
 * [LoadIncremental](#loadincremental) - [DMSCDC_LoadIncremental.py](./DMSCDC_LoadIncremental.py)

[Sample Database](#sample-database)
 * [Source DB Setup](#source-db-setup) - [DMSCDC_SampleDB.yaml](./DMSCDC_SampleDB.yaml)
 * [Incremental Load Script](#incremental-load-script) - [DMSCDC_SampleDB_Incremental.sql](./DMSCDC_SampleDB_Incremental.sql)

## Glue Jobs
This solution uses a Python shell job for the Controller. The controller will run each time the workflow is triggered but the LoadInitial and LoadIncremental are only executed as needed.

### Controller
The controller job will leverage the state table stored in DynamoDB and compare the state with the files in S3.  It will determine which tables/files need to be processed.

The first step in the processing is to determine the *schemas* and *tables* which are located in DMS raw location and construct an *item* object representing the default values for the *table*.  
```python
#get the list of table folders
s3_input = 's3://'+bucket+'/'+prefix
url = urlparse(s3_input)
schemas = s3conn.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/').get('CommonPrefixes')
if schemas is None:
    logger.error('DMSBucket: '+bucket+' DMSFolder: ' + prefix + ' is empty.  Confirm the source DB has data and the DMS replication task is replicating to this S3 location. Review the documention https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.html to ensure the DB is setup for CDC.')
    sys.exit()

index = 0
#get folder metadata
for schema in schemas:
    tables = s3conn.list_objects(Bucket=bucket, Prefix=schema['Prefix'], Delimiter='/').get('CommonPrefixes')
    for table in tables:
        full_folder = table['Prefix']
        foldername = full_folder[len(schema['Prefix']):]
        path = bucket + '/' + full_folder
        print('Processing table: ' + path)
        item = {
            'path': {'S':path},
            'bucket': {'S':bucket},
            'prefix': {'S':schema['Prefix']},
            'folder': {'S':foldername},
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
            BillingMode='PAY_PER_REQUEST')
        time.sleep(10)

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
```

Next, the controller will start a new python shell job `DMSCDC_ProcessTable` for each table in parallel.  The job will determine if an initial and/or incremental load is needed.

> Note: if the DynamoDB table has the ActiveFlag != 'true', this table will be skipped.  This is to ensure that a user has reviewed the table and set appropriate keys. If the PrimaryKey has been set, the incremental load will conduct change detection logic.  If the PrimaryKey has NOT been set, incremental load will only load rows marked for insert. If the PartitionKey has been set, the Spark job will partition the data before it is written.  

```python
#determine if need to run incremental --> Run incremental --> Update DDB
if item['ActiveFlag']['S'] == 'true':
    logger.warn('starting processTable, args: ' + json.dumps(item))
    response = glue.start_job_run(JobName='DMSCDC_ProcessTable',Arguments={'--item':json.dumps(item)})
    logger.debug(json.dumps(response))
else:
  logger.error('Load is not active.  Update dynamoDB: ' + full_folder)
```        

### ProcessTable
The first step of the `DMSCDCD_ProcessTable` job, is to determine if the initial file needs to be processed by comparing the timestamp of the initial load file stored in the DynamoDB table with the timestamp retrieved from S3.  If it is determined that the initial file should be processed the [LoadInitial](#LoadInitial) glue job is triggered.  The controller will wait for its completion using the `runGlueJob` function. If the job completes successfully, the DynamoDB table will be updated with the timestamp of the file which was processed.

```python
logger.warn('starting processTable: ' + path)
loadInitial = False
#determine if need to run initial --> Run Initial --> Update DDB
initialfiles = s3conn.list_objects(Bucket=bucket, Prefix=full_folder+'LOAD').get('Contents')
if initialfiles is not None :
    s3FileTS = initialfiles[0]['LastModified'].replace(tzinfo=None)
    ddbFileTS = datetime.datetime.strptime(lastFullLoadDate, '%Y-%m-%d %H:%M:%S')
    if s3FileTS > ddbFileTS:
        message='Starting to process Initial file: ' + full_folder
        loadInitial = True
        lastFullLoadDate = datetime.datetime.strftime(s3FileTS,'%Y-%m-%d %H:%M:%S')
    else:
      message='Intial files already processed: ' + full_folder
else:
  message='No initial files to process: ' + full_folder
logger.warn(message)

#Call Initial Glue Job for this source
if loadInitial:
    args={
        '--bucket':bucket,
        '--prefix':full_folder,
        '--folder':folder,
        '--out_path':out_path,
        '--partitionKey':partitionKey}
    if runGlueJob('DMSCDC_LoadInitial',args) != 1:
        ddbconn.update_item(
            TableName='DMSCDC_Controller',
            Key={"path": {"S":path}},
            AttributeUpdates={"LastFullLoadDate": {"Value": {"S": lastFullLoadDate}}})
```
The second step of the `DMSCDCD_ProcessTable` job, is to determine if any incremental files need to be processed by comparing the *lastIncrementalFile* stored in the DynamoDB table with the *newIncrementalFile* retrieved from S3.  If those two values are different it is determined that there are incremental files and the [LoadIncremental](#LoadIncremental) glue job is triggered.  The controller will wait for its completion using the *testGlueJob* function. If the job completes successfully, the DynamoDB table will be updated with the *newIncrementalFile*.

```python
loadIncremental = False
#Get the latest incremental file
incrementalFiles = s3conn.list_objects_v2(Bucket=bucket, Prefix=full_folder+'2', StartAfter=lastIncrementalFile).get('Contents')
if incrementalFiles is not None:
    filecount = len(incrementalFiles)
    newIncrementalFile = bucket + '/' + incrementalFiles[filecount-1]['Key']
    if newIncrementalFile != lastIncrementalFile:
        loadIncremental = True
        message = 'Starting to process incremental files: ' + full_folder
    else:
        message = 'Incremental files already processed: ' + full_folder
else:
    message = 'No incremental files to process: ' + full_folder
logger.warn(message)

#Call Incremental Glue Job for this source
if loadIncremental:
    args = {
        '--bucket':bucket,
        '--prefix':full_folder,
        '--folder':foldername,
        '--out_path':out_path,
        '--partitionKey':partitionKey,
        '--lastIncrementalFile' : lastIncrementalFile,
        '--newIncrementalFile' : newIncrementalFile,
        '--primaryKey' : primaryKey
        }
    if runGlueJob('DMSCDC_LoadIncremental',args) != 1:
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
    partitionCount = input.select(partitionKeys).distinct().count()
    input.repartition(partitionCount,partitionKeys).write.mode('overwrite').partitionBy(partitionKeys).parquet(s3_outputpath)
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

# gets list of files to process
inputFileList = []
results = s3conn.list_objects_v2(Bucket=args['bucket'], Prefix=args['prefix'] +'2', StartAfter=args['lastIncrementalFile']).get('Contents')
for result in results:
    if (args['bucket'] + '/' + result['Key'] != last_file):
        inputFileList.append('s3://' + args['bucket'] + '/' + result['Key'])

inputfile = spark.read.parquet(*inputFileList)
```

Next, the process will determine if a primary key has been set in the DynamoDB table.  If it has been set, the change detection process will be executed.  If it has not, or this is the first time this table is being loaded, only records marked for insert (where the *Op* = 'I') will be prepped to be written.

```python
tgtExists = True
try:
    test = spark.read.parquet(s3_outputpath)
except:
    tgtExists = False

if primary_keys == "null" or not(tgtExists):
    output = inputfile.filter(inputfile.Op=='I')
    filelist = [["null"]]
```

Otherwise, to process the changed records the process will de-dup the input records as well as determine the impacted data lake files.

First, a few metadata fields are added to the input data and the already loaded data:
* sortpath - For already loaded data, this is set to "0" to ensure that newly loaded data with the same primary key will override it.  For new data this is the filename generated by DMS which will have a higher sort order than "0".
* filepath - For already loaded data, this is the file name of the data lake file.  For new data this is the filename generated by DMS.
* rownum - This is the relative rownumber of each primary key. For already loaded data, this will always be '1' as there should always be only 1 record for each primary key.  For new data data, this will be a value from 1-to-N for each operation against a primary key.  In the case when there are multiple operations on the same primary key, the latest operation will have the greatest rownum value.

```python    
primaryKeys = primary_keys.split(",")
windowRow = Window.partitionBy(primaryKeys).orderBy("sortpath")

#Loads the target data adding columns for processing
target = spark.read.parquet(s3_outputpath).withColumn("sortpath", lit("0")).withColumn("tgt_filepath",input_file_name()).withColumn("rownum", lit(1))
input = inputfile.withColumn("sortpath", input_file_name()).withColumn("src_filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))

```

To determine which data lake files will be impacted, the input data is joined to the target data and a distinct list of files is captured.  

```python
#determine impacted files
files = target.join(input, primaryKeys, 'inner').select(col("tgt_filepath").alias("list_filepath")).distinct()
filelist = files.collect()
```

Since S3 data is immutable, not only the records which have changed will need to be written, but also all the unchanged records from the impacted data lake files.  To accomplish this task, a new dataframe will be created which is union of the source and target data.

```python
uniondata = input.unionByName(target.join(files,files.list_filepath==target.tgt_filepath), allowMissingColumns=True)
```

To determine which rows to write from the union dataframe, a *rnk* column is added indicating the last change for a given primary leveraging the *sortpath* and *rownum* fields which were added.  In addition, only records where the last operation is not a delete (*Op != D*) will be filtered.

```python
window = Window.partitionBy(primaryKeys).orderBy(desc("sortpath"), desc("rownum"))
output = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile.columns)
```

If a partition key has been set for this table, the data will be written to the Data Lake in the appropriate partitions.  

```python
# write data by partitions
if partition_keys != "null" :
    partitionKeys = partition_keys.split(",")
    partitionCount = output.select(partitionKeys).distinct().count()
    output.repartition(partitionCount,partitionKeys).write.mode('append').partitionBy(partitionKeys).parquet(s3_outputpath)
else:
    output.write.mode('append').parquet(s3_outputpath)
```

Finally, the impacted data lake files containing the changed records need to be deleted.  

Note: Because S3 does not guarantee consistency on delete operations, until the file is actually removed from S3, there is a possibility of users seeing duplicate data temporarily.

```python
#delete old files
for row in filelist:
    if row[0] != "null":
        o = parse.urlparse(row[0])
        s3conn.delete_object(Bucket=o.netloc, Key=parse.unquote(o.path)[1:])
```

## Sample Database
Below is a sample MySQL database you can build and use in conjunction with this project.  It can be used to demonstrate the end-to-end flow of data into your Data Lake.

### Source DB Setup
In order for a DB to be a candidate for DMS as a source it needs to meet certain [requirements](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.html) whether it's managed through Amazon via RDS or if it's an EC2 or on-premise installation.  Launch the following cloud formation stack to deploy a MySQL DB that has been pre-configured to meet these requirements and which is *pre-loaded* with the [initial load script](DMSCDC_SampleDB_Initial.sql).  Be sure to choose the *same VPC/Subnet/Security Group* as your DMS Replication Instance to ensure they have network connectivity.

[![](https://console.aws.amazon.com/cloudformation/home?#/stacks/new?stackName=DMSCDCSampleDB&templateURL=https://s3-us-west-2.amazonaws.com/dmscdc-files/DMSCDC_SampleDB.yaml)](launch-stack.svg)

### Incremental Load Script
Before proceeding with the incremental load, ensure that
1. The initial data has arrived in the DMS bucket.
1. You have configured your DynamoDB table to set the appropriate ActiveFlag, PrimaryKey and PartitionKey values.
1. The initial data has arrived in the LAKE bucket.  Note: By default the solution will run on an hourly basis on the 00 minute.  To change the load frequency or minute, modify the **[Glue Trigger](https://console.aws.amazon.com/glue/home?#etl:tab=triggers)**.  

Once the seed data has been loaded, execute the following script in the MySQL database to simulate changed data.
```sql
update product set name = 'Sample Product', dept = 'Sample Dept', category = 'Sample Category' where id = 1001;
delete from product where id = 1002;
call loadorders(1345, current_date);
insert into store values(1009, '125 Technology Dr.', 'Irvine', 'CA', 'US', '92618');
```

Once DMS has processed these changes, you will see new incremental files for `store`, `product`, and `productorder` in the DMS bucket.  On the next execution of the Glue job, you will see new and updated files in the LAKE bucket. Test.
