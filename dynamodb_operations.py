import boto3
import sqs_operations
import s3_operations
from boto3.dynamodb.conditions import Key

PRIMARY_KEY = 'executionId'

def populate_job_details(execution_id, TABLE):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(TABLE)
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
  
def populate_job_details_complete(execution_id, GLACIER_RESTORE_TABLE):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(GLACIER_RESTORE_TABLE)
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
  
def get_job_details(execution_id, GLACIER_RESTORE_TABLE, TABLE, SQSQueueURL, receipt_handle):
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(GLACIER_RESTORE_TABLE)
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
    s3_operations.get_s3_restore_status(s3_objects, s3_bucket, execution_id, GLACIER_RESTORE_TABLE, TABLE, SQSQueueURL)
  elif status == 'COMPLETE':
    print('Restoration is complete for execution ID: %s' % execution_id)
    print('Updating item status to READY for execution ID: %s in table: %s' % (execution_id, TABLE))
    populate_job_details(execution_id, TABLE)
    print('Removing message from SQS queue URL: %s' % SQSQueueURL)
    sqs_operations.delete_message(SQSQueueURL, receipt_handle)
  return True