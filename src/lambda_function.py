from process_files import process_event_data
from dynamo_event import manage_dynamo_event
import uuid
import json
import os


def lambda_handler(event, context):
    stage_bucket = os.environ['STAGE_BUCKET']
    counter, count_error = 0, 0
    
    for record in event['Records']:
        event_bridge_data = json.loads(record['body'])
        event_bridge_data_detail = event_bridge_data['detail']
        STATUS = "CHECK" # or PENDING, COMPLETED, FAIL

        check_record = manage_dynamo_event(event_bridge_data, STATUS)
        
        if check_record:
            status_record = check_record['status']
            key_record = check_record['file_key']
            print(f"ERROR: Record exist in database. Status: {status_record}. Key: {key_record}")
            count_error += 1
        
        else:
            STATUS = "PENDING"
            manage_dynamo_event(event_bridge_data, STATUS)
            
            # Process event in process_files module
            try:
                response = process_event_data(stage_bucket, event_bridge_data_detail)
            
            except Exception as e:
                print(f"[ERROR] {e}")
                response = {
                    'errorCount': 1,
                    'errorCode': 'PROCESS_EVENT_ERROR',
                    'success': False
                }
            
            if response['success']:
                STATUS = "COMPLETED"
                manage_dynamo_event(event_bridge_data, STATUS)
            else:
                STATUS = "FAIL"
                manage_dynamo_event(event_bridge_data, STATUS)
                
            count_error += response['errorCount']
            
        counter += 1
    
    text_message = f"Total errors: {count_error} of {counter} records"
    print(text_message)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": text_message
        }),
    }
