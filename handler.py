import json
import urllib.parse
import boto3
import os

import dynamodb_operations

sqs = boto3.client('sqs')
snakemake_table = os.environ['SNAKEMAKE_TABLE']
status_table = os.environ['STATUS_TABLE']
sqs_queue_url = os.environ['SQS_QUEUE_URL']

def handler(event, context):
  try:
    response = sqs.receive_message(
      QueueUrl = sqs_queue_url,
      MaxNumberOfMessages=1,
      WaitTimeSeconds=5
    )
    receipt_handle = response['Messages'][0]['ReceiptHandle']
    execution_id = response['Messages'][0]['Body']
    dynamodb_operations.get_job_details(execution_id, status_table, snakemake_table, sqs_queue_url, receipt_handle)
  except Exception as e:
      print(e)
      raise e
