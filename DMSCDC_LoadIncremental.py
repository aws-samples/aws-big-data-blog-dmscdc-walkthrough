import sys
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
import boto3
import urllib
from urllib import parse

s3conn = boto3.client('s3')

sparkContext = SparkContext.getOrCreate()
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'bucket',
        'prefix',
        'folder',
        'out_path',
        'lastIncrementalFile',
        'newIncrementalFile',
        'primaryKey',
        'partitionKey'])

job.init(args['JOB_NAME'], args)
s3_outputpath = 's3://' + args['out_path'] + args['folder']

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

tgtExists = True
try:
    test = spark.read.parquet(s3_outputpath)
except:
    tgtExists = False

#No Primary_Keys implies insert only
if primary_keys == "null" or not(tgtExists):
    output = inputfile.filter(inputfile.Op=='I')
    filelist = [["null"]]
else:
    primaryKeys = primary_keys.split(",")
    windowRow = Window.partitionBy(primaryKeys).orderBy("sortpath")

    #Loads the target data adding columns for processing
    target = spark.read.parquet(s3_outputpath).withColumn("sortpath", lit("0")).withColumn("tgt_filepath",input_file_name()).withColumn("rownum", lit(1))
    input = inputfile.withColumn("sortpath", input_file_name()).withColumn("src_filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))

    #determine impacted files
    files = target.join(input, primaryKeys, 'inner').select(col("tgt_filepath").alias("list_filepath")).distinct()
    filelist = files.collect()

    uniondata = input.unionByName(target.join(files,files.list_filepath==target.tgt_filepath), allowMissingColumns=True)
    window = Window.partitionBy(primaryKeys).orderBy(desc("sortpath"), desc("rownum"))
    output = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile.columns)

# write data by partitions
if partition_keys != "null" :
    partitionKeys = partition_keys.split(",")
    partitionCount = output.select(partitionKeys).distinct().count()
    output.repartition(partitionCount,partitionKeys).write.mode('append').partitionBy(partitionKeys).parquet(s3_outputpath)
else:
    output.write.mode('append').parquet(s3_outputpath)

#delete old files
for row in filelist:
    if row[0] != "null":
        o = parse.urlparse(row[0])
        s3conn.delete_object(Bucket=o.netloc, Key=parse.unquote(o.path)[1:])
