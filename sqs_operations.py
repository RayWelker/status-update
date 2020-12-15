import boto3
import json

def delete_message(SQSQueueURL, receipt_handle):
    sqs = boto3.client('sqs')
    response = sqs.delete_message(
      QueueUrl=SQSQueueURL,
      ReceiptHandle=receipt_handle
    )