import boto3
import dynamodb_operations
import json

def get_s3_restore_status(s3_objects, s3_bucket, execution_id, GLACIER_RESTORE_TABLE, TABLE, SQSQueueURL):
    client = boto3.client('s3')
    s3 = boto3.resource('s3')
    completed_list = []
    for x in s3_objects:
        bcl_prefix = x.rsplit('/', 1)[0]
        bcl_bucket = s3_bucket
        obj = s3.Object(bcl_bucket, x)
        if obj.storage_class == 'GLACIER':
            completed_list.append(True)
        else:
            completed_list.append(False)
    if all([ele == True for ele in completed_list]):
        print('Restoration is complete for execution ID: %s' % execution_id)
        print('Updating item: %s table: %s' % (execution_id, GLACIER_RESTORE_TABLE))
        dynamodb_operations.populate_job_details_complete(execution_id, GLACIER_RESTORE_TABLE)
        print('Removing message from SQS queue URL: %s' % SQSQueueURL)
        sqs_operations.delete_message(SQSQueueURL, receipt_handle)
    else:
        completed_list.append('False')
        print('Restoration has not completed')