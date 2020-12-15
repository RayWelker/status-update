import boto3
from boto3.dynamodb.conditions import Key

import sqs_operations
import s3_operations

def populate_job_details(execution_id, snakemake_table):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(snakemake_table)
  table.update_item(
    Key={
      "executionId": execution_id,
    },
    UpdateExpression="SET #s = :newStatus",
    ExpressionAttributeValues={
      ":newStatus": "READY"
    },
    ExpressionAttributeNames={
      "#s": "status"
    },
    ReturnValues="UPDATED_NEW"
  )
  return True
  
def populate_job_details_complete(execution_id, status_table):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(status_table)
  table.update_item(
    Key={
      "executionId": execution_id,
    },
    UpdateExpression="SET #s = :newStatus",
    ExpressionAttributeValues={
      ":newStatus": "COMPLETE"
    },
    ExpressionAttributeNames={
      "#s": "status",
    },
    ReturnValues="UPDATED_NEW"
  )
  return True
  
def get_job_details(execution_id, status_table, snakemake_table, sqs_queue_url, receipt_handle):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(status_table)
  item = table.get_item(
    Key={
      "executionId": execution_id,
    })
  print(item)
  s3_objects = item['Item']['s3_object']
  s3_bucket = item['Item']['s3_bucket']
  status = item['Item']['status']
  if status == 'RESTORING':
    print('Checking if restoration has completed: %s' % execution_id)
    s3_operations.get_s3_restore_status(s3_objects, s3_bucket, execution_id, status_table, sqs_queue_url, receipt_handle)
  elif status == 'COMPLETE':
    print('Restoration is complete for execution ID: %s' % execution_id)
    print('Updating item status to READY for execution ID: %s in table: %s' % (execution_id, snakemake_table))
    populate_job_details(execution_id, snakemake_table)
    print('Removing message from SQS queue URL: %s' % sqs_queue_url)
    sqs_operations.delete_message(sqs_queue_url, receipt_handle)
  return True