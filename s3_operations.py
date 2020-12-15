import boto3
import json

import dynamodb_operations
import sqs_operations

def get_s3_restore_status(s3_objects, s3_bucket, execution_id, status_table, snakemake_table, sqs_queue_url, receipt_handle):
    s3 = boto3.resource('s3')
    completed_list = []
    for x in s3_objects:
        bcl_bucket = s3.Bucket(s3_bucket)
        obj = s3.Object(bcl_bucket, x)
        if obj.storage_class == 'GLACIER':
            completed_list.append(True)
        else:
            completed_list.append(False)
    if all([ele == True for ele in completed_list]):
        print('Restoration is complete for execution ID: %s' % execution_id)
        print('Updating item: %s table: %s' % (execution_id, status_table))
        dynamodb_operations.populate_job_details_complete(execution_id, status_table)
        print('Removing message from SQS queue URL: %s' % sqs_queue_url)
        sqs_operations.delete_message(sqs_queue_url, receipt_handle)
    else:
        completed_list.append('False')
        print('Restoration has not completed')