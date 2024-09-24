from pyiceberg.partitioning import PartitionSpec, PartitionField
from utils_lib import read_metadata, check_glue_database, check_table
from utils_lib import load_file, check_partition_columns, append_data_table
import json
import re

def process_event_data(stage_bucket, event_bridge_data):
    """
    Process a file based on event data from AWS EventBridge.

    Parameters:
    stage_bucket (str): The S3 bucket where the file is staged.
    event_bridge_data (dict): The event data from AWS EventBridge containing details about the file.

    Returns:
    dict: A dictionary containing the error count, error code, and success status.
    """
    
    raw_bucket = event_bridge_data['bucket']['name']
    file_key = event_bridge_data['object']['key']
    file_size = event_bridge_data['object']['size']
    etag_file = event_bridge_data['object']['etag']
    
    print(f"Processing file {file_key} in bucket {raw_bucket}")
    print(f"ETag: {etag_file}")
    print(f"File size: {file_size}")
    
    
    # 1.1 Read metadata file
    metadata = read_metadata(raw_bucket, file_key)
    
    if metadata:   
        try:
            table_name = metadata["tableName"]
            glue_database = metadata["database"]
            schema_json = metadata["schemas"][0]
            properties = metadata["properties"]
            partition_json = json.dumps(metadata["partition-specs"][0])
            partition_data = PartitionSpec.model_validate_json(partition_json)

        except Exception as e:
            print(f"[ERROR] Error in metadata.json file: {e}")
            return {
                'errorCount': 1,
                'errorCode': 'METADATA_FILE_ERROR',
                'success': False
            }
    else:
        return {
            'errorCount': 1,
            'errorCode': 'METADATA_FILE_NOT_FOUND',
            'success': False
        }
    
    # 1.2 Check table_name
    pattern = r"^[a-z]+\.[a-z][a-z_]+$"
    if not re.search(pattern, table_name):
        print(f"[ERROR] Table name {table_name} is not valid")
        return {
            'errorCount': 1,
            'errorCode': 'TABLE_NAME_ERROR',
            'success': False
        }

    
    # 2. Chech Glue database
    if not check_glue_database(glue_database):
        return {
            'errorCount': 1,
            'errorCode': 'GLUE_CONNECTION_ERROR',
            'success': False
        }
    
    
    # 3. Check table
    tbl = check_table(stage_bucket, glue_database, table_name, schema_json, partition_data, properties)
    
    if not tbl:
        return {
            'errorCount': 1,
            'errorCode': 'CHECK_TABLE_ERROR',
            'success': False
        }
        
    file_path = f"s3://{raw_bucket}/{file_key}"
    schema = tbl.schema()
    
    
    # 4. Load file data
    file_data = load_file(file_path, schema, partition_data)
    
    if not file_data:
        return {
            'errorCount': 1,
            'errorCode': 'FILE_NOT_FOUND',
            'success': False
        }


    # 5. Check partition data
    if partition_data:
        if not check_partition_columns(file_data, partition_data):
            return {
                'errorCount': 1,
                'errorCode': 'CHECK_PARTITION_DATA_ERROR',
                'success': False
            }


    # 6. Append data to table
    write_data = append_data_table(tbl, file_data)
    
    if not write_data:
        return {
            'errorCount': 1,
            'errorCode': 'WRITE_DATA_ERROR',
            'success': False
        }
    else:
        return {
            'errorCount': 0,
            'errorCode': '',
            'success': True
        }
