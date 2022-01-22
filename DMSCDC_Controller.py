import sys
import json
from urllib.parse import urlparse
import urllib
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

prefix = ""
out_prefix = ""

#Optional Parameters
if ('--prefix' in sys.argv):
    nextarg = sys.argv[sys.argv.index('--prefix')+1]
    if (nextarg is not None and not(nextarg.startswith('--'))):
        prefix = nextarg
if ('--out_prefix' in sys.argv):
    nextarg = sys.argv[sys.argv.index('--out_prefix')+1]
    if (nextarg is not None and not(nextarg.startswith('--'))):
        out_prefix = nextarg

#Required Parameters
args = getResolvedOptions(sys.argv, [
        'bucket',
        'out_bucket'])

bucket = args['bucket']
out_bucket = args['out_bucket']
out_path = out_bucket + '/' + out_prefix

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
        logger.warn('Processing table: ' + path)
        item = {
            'path': {'S':path},
            'bucket': {'S':bucket},
            'prefix': {'S':schema['Prefix']},
            'folder': {'S':foldername},
            'PrimaryKey': {'S':'null'},
            'PartitionKey': {'S':'null'},
            'LastFullLoadDate': {'S':'1900-01-01 00:00:00'},
            'LastIncrementalFile': {'S':path + '0.parquet'},
            'ActiveFlag': {'S':'false'},
            'out_path': {'S': out_path}}

        #CreateTable if not already present
        try:
            response1 = ddbconn.describe_table(TableName='DMSCDC_Controller')
        except Exception as e:
            ddbconn.create_table(
                TableName='DMSCDC_Controller',
                KeySchema=[{'AttributeName': 'path','KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'path','AttributeType': 'S'}],
                BillingMode='PAY_PER_REQUEST')
            time.sleep(10)

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

        logger.debug(json.dumps(item))
        logger.warn('Bucket: '+bucket+' Path: ' + full_folder)


        #determine if need to run incremental --> Run incremental --> Update DDB
        if item['ActiveFlag']['S'] == 'true':
            logger.warn('starting processTable, args: ' + json.dumps(item))
            response = glue.start_job_run(JobName='DMSCDC_ProcessTable',Arguments={'--item':json.dumps(item)})
            logger.debug(json.dumps(response))
        else:
          logger.error('Load is not active.  Update dynamoDB: ' + full_folder)
