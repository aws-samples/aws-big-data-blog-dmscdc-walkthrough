import sys
import json
from urllib.parse import urlparse
import urllib
import datetime
import boto3
import time
from awsglue.utils import getResolvedOptions
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ddbconn = boto3.client('dynamodb')
glue = boto3.client('glue')
s3conn = boto3.client('s3')


def testGlueJob(jobId, count, sec, jobName):
    i = 0
    while i < count:
        response = glue.get_job_run(JobName=jobName, RunId=jobId)
        status = response['JobRun']['JobRunState']
        if status == 'SUCCEEDED':
            return 1
        elif (status == 'RUNNING' or status == 'STARTING' or status == 'STOPPING'):
            time.sleep(sec)
            i+=1
        else:
            return 0
        if i == count:
            return 0

def recursiveTraverseFolders(bucket, prefix):
  print('Checking prefix: '+prefix)
  folders = s3conn.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/').get('CommonPrefixes')
  if isinstance(folders, list):
    for folder in folders:
      childPrefix = folder['Prefix']
      hasChildFolders = recursiveTraverseFolders(bucket, childPrefix)
      if not hasChildFolders:
        print('Processing folder: '+folder['Prefix'])
        processFolder(folder, prefix)
    return True
  return False

def processFolder(folder, prefix):
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

    #CreateTable if not already present
    try:
        response1 = ddbconn.describe_table(TableName='DMSCDC_Controller')
    except Exception as e:
        ddbconn.create_table(
            TableName='DMSCDC_Controller',
            KeySchema=[{'AttributeName': 'path','KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'path','AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 1,'WriteCapacityUnits': 1})

    #Put Item if not already present
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

    logger.info(json.dumps(item))
    logger.info('Bucket: '+bucket+' Path: ' + full_folder)

    if activeFlag == 'true' :
      #determine if need to run initial --> Run Initial --> Update DDB
      loadInitial = False

      initialfiles = s3conn.list_objects(Bucket=bucket, Prefix=full_folder+'LOAD').get('Contents')
      if initialfiles is not None :
          s3FileTS = initialfiles[0]['LastModified'].replace(tzinfo=None)
          ddbFileTS = datetime.datetime.strptime(lastFullLoadDate, '%Y-%m-%d %H:%M:%S')
          if s3FileTS > ddbFileTS:
            print('Starting to process Initial file.')
            loadInitial = True
            lastFullLoadDate = datetime.datetime.strftime(s3FileTS,'%Y-%m-%d %H:%M:%S')
          else:
            print('Intial files already processed.')
      else:
          print('No initial files to process.')

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

      #determine if need to run incremental --> Run incremental --> Update DDB
      loadIncremental = False
      newIncrementalFile = path + '0.parquet'

      #Get the latest incremental file
      incrementalFiles = s3conn.list_objects(Bucket=bucket, Prefix=full_folder+'2').get('Contents')
      if incrementalFiles is not None:
        filecount = len(incrementalFiles)
        newIncrementalFile = bucket + '/' + incrementalFiles[filecount-1]['Key']
        if newIncrementalFile != lastIncrementalFile:
          loadIncremental = True
          print("Starting to process incremental files")
        else:
            print("Incremental files already processed.")
      else:
          print("No incremental files to process.")

      #Call Incremental Glue Job for this source
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

        #Wait for execution complete, timeout in 20*30=900 secs, if successful, update ddb
        if testGlueJob(response['JobRunId'], 30, 30, 'DMSCDC_LoadIncremental') != 1:
          message = 'Error during Controller execution'
          raise ValueError(message)
        else:
          ddbconn.update_item(
            TableName='DMSCDC_Controller',
            Key={"path": {"S":path}},
            AttributeUpdates={"LastIncrementalFile": {"Value": {"S": newIncrementalFile}}})
    else:
    	print("Load is not active--file processing skipped. Update dynamoDB.")

#Required Parameters
args = getResolvedOptions(sys.argv, [
  'prefix',
  'out_prefix',
  'bucket',
  'out_bucket'])

prefix = args['prefix']
out_prefix = args['out_prefix']
bucket = args['bucket']
out_bucket = args['out_bucket']
out_path = out_bucket + '/' + out_prefix

recursiveTraverseFolders(bucket, prefix)
