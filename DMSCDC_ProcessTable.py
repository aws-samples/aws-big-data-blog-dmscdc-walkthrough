import sys
import json
import datetime
import boto3
import time
from awsglue.utils import getResolvedOptions
import logging

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.addHandler(handler)

ddbconn = boto3.client('dynamodb')
glue = boto3.client('glue')
s3conn = boto3.client('s3')

#Required Parameters
args = getResolvedOptions(sys.argv, ['item'])
item = json.loads(args['item'])

partitionKey = item['PartitionKey']['S']
lastFullLoadDate = item['LastFullLoadDate']['S']
lastIncrementalFile = item['LastIncrementalFile']['S']
activeFlag = item['ActiveFlag']['S']
primaryKey = item['PrimaryKey']['S']
folder = item['folder']['S']
prefix = item['prefix']['S']
full_folder =  prefix + folder
path = item['path']['S']
bucket = item['bucket']['S']
out_path = item['out_path']['S']

def runGlueJob(job, args):
    logger.info('starting runGlueJob: ' + job + ' args: ' + json.dumps(args))
    response = glue.start_job_run(JobName=job,Arguments=args)
    count=90
    sec=10
    jobId=response['JobRunId']
    i = 0
    while i < count:
        response = glue.get_job_run(JobName=job, RunId=jobId)
        status = response['JobRun']['JobRunState']
        if status == 'SUCCEEDED':
            return 0
        elif (status == 'RUNNING' or status == 'STARTING' or status == 'STOPPING'):
            time.sleep(sec)
            i+=1
        else:
            logger.error('Error during loadIncremental execution: ' + status)
            return 1
        if i == count:
            logger.error('Execution timeout: ' + count*sec + ' seconds')
            return 1

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
        '--folder':folder,
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
