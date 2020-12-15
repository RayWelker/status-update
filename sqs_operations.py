import boto3
import json

def delete_message(sqs_queue_url, receipt_handle):
  sqs = boto3.client('sqs')
  sqs.delete_message(
    QueueUrl=sqs_queue_url,
    ReceiptHandle=receipt_handle
  )