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
import urlparse
import urllib

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
s3_inputpath = 's3://' + args['bucket'] + '/' + args['prefix'] + args['folder']
s3_outputpath = 's3://' + args['out_path'] + args['folder']

last_file = args['lastIncrementalFile']
curr_file = args['newIncrementalFile']
primary_keys = args['primaryKey']
partition_keys = args['partitionKey']

inputfile = spark.read.parquet(s3_inputpath+"/2*.parquet")

inputfile.filter(input_file_name() > last_file)
inputfile.filter(input_file_name() <= curr_file)

#No Primary_Keys implies insert only
if primary_keys == "null":
    output = inputfile.filter(inputfile.Op=='I')
    filelist = [["null"]]
else:
    primaryKeys = primary_keys.split(",")
    windowRow = Window.partitionBy(primaryKeys).orderBy("sortpath")

    #Loads the targetdata adding columns for processing
    target = spark.read.parquet(s3_outputpath).withColumn("sortpath", lit("0")).withColumn("filepath",input_file_name()).withColumn("rownum", lit(1))
    input = inputfile.withColumn("sortpath", input_file_name()).withColumn("filepath",input_file_name()).withColumn("rownum", row_number().over(windowRow))

    #determine impacted files
    files = target.join(inputfile, primaryKeys, 'inner').select(col("filepath").alias("filepath1")).distinct()

    #union new and existing data of impacted files
    uniondata = input.select(target.columns).union(target.join(files,files.filepath1==target.filepath).select(target.columns))
    window = Window.partitionBy(primaryKeys).orderBy(desc("sortpath"), desc("rownum"))
    output = uniondata.withColumn('rnk', rank().over(window)).where(col("rnk")==1).where(col("Op")!="D").coalesce(1).select(inputfile.columns)

# write data by partitions
if partition_keys != "null" :
    partitionKeys = partition_keys.split(",")
    output.repartition(partitionKeys[0]).write.mode('append').partitionBy(partitionKeys).parquet(s3_outputpath)
else:
    output.write.mode('append').parquet(s3_outputpath)

#delete old files
filelist = files.collect()
for row in filelist:
    if row[0] != "null":
        o = urlparse.urlparse(row[0])
        s3conn.delete_object(Bucket=o.netloc, Key=urllib.unquote(o.path)[1:])
