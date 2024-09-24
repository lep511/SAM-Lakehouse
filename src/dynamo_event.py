import boto3
from botocore.exceptions import ClientError
import os

def manage_dynamo_event(event_body, status):
    dynamodb = boto3.client("dynamodb")
    dynamo_table_name = os.getenv("DYNAMO_TABLE_NAME")
    
    if not dynamo_table_name:
        print("[ERROR] Dynamo table name not found in environment variables")
        return
    
    etag = event_body['detail']['object']['etag']
    file_size = event_body['detail']['object']['size']
    f_time = event_body['time']
    event_id = event_body['id']
    file_key = event_body['detail']['object']['key']
    database = file_key.split("/")[-4]
    table_name = file_key.split("/")[-3]
    file_name = file_key.split("/")[-1]      
    
    if status == "CHECK":
        # Check if a element exist in DynamoDB
        key = {
            "id": {"S": etag}
        }
        try:
            response = dynamodb.get_item(
                TableName=dynamo_table_name,
                Key=key,
                ConsistentRead=True
            )
        except ClientError as e:
            print(f"[ERROR] Error checking element in DynamoDB: {e}")
            return False
        else:
            item = response.get('Item', {})
            if item:
                return item
            else:
                return False
    
    elif status == "PENDING":
        # Create new item in DynamoDB
        item = {
            'id': {"S": etag},
            'status': {"S": status},
            'file_key': {"S": file_key},
            'file_size': {"N": str(file_size)},
            'event_time': {"S": f_time},
            'event_id': {"S": event_id},
            'database': {"S": database},
            'table': {"S": table_name},
            'file_name': {"S": file_name},
            'GSI-PK': {"S": status},
            'GSI-SK': {"S": f"{database}#{table_name}#{etag}"},
        }
        
        try:
            response = dynamodb.put_item(
                TableName=dynamo_table_name,
                Item=item
            )
        except ClientError as e:
            print(f"[ERROR] Error put element in DynamoDB: {e}. Status: {status}")
            return False
        else:
            return True
    
    elif status == "COMPLETED" or status == "FAIL":
        
        key = {
            "id": {"S": etag}
        }
        
        expression_attribute_values = {
            ":s": {"S": status},
            ":gpk": {"S": status},
            ":gsk": {"S": f"{database}#{table_name}#{file_name}"}
        }
        
        expression_attribute_names = {
            "#STATUS": "status",
            "#GPK": "GSI-PK",
            "#GSK": "GSI-SK"
        }
        
        update_expression = "SET #STATUS = :s, #GPK = :gpk, #GSK = :gsk"
        
        try:
            response = dynamodb.update_item(
                TableName=dynamo_table_name,
                ExpressionAttributeNames=expression_attribute_names,
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )
            return response
        except ClientError as e:
            print(f"[ERROR] Error update element in DynamoDB: {e}. Status: {status}")
            return False
        else:
            return True