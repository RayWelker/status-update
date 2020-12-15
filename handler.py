import json
import urllib.parse
import boto3
import os

import dynamodb_operations

sqs = boto3.client('sqs')
TABLE = os.environ['TABLE']
GLACIER_RESTORE_TABLE = os.environ['GLACIER_RESTORE_TABLE']
SQSQueueURL = os.environ['SQS_QUEUE_URL']

def handler(event, context):
    try:
      response = sqs.receive_message(
        QueueUrl = SQSQueueURL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
      )
      receipt_handle = response['Messages'][0]['ReceiptHandle']
      execution_id = response['Messages'][0]['Body']
      dynamodb_operations.get_job_details(execution_id, GLACIER_RESTORE_TABLE, TABLE, SQSQueueURL, receipt_handle)
    except Exception as e:
        print(e)
        raise e
