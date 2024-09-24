import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import pyarrow.parquet as pq
import glob
import argparse
import threading
import json
import os
import sys

sys.path.insert(0,'../src')
from process_files import process_event_data
from utils_lib import check_glue_database


sys.path.insert(0,'../notebook')
from generate_files import generate_metadata_file


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def upload_file(file_name, bucket, object_name=None, metadata={}, max_concurrency=10):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    
    s3 = boto3.client('s3')
    
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Set the max_concurrency attribute to increase or decrease bandwidth usage
    config = TransferConfig(max_concurrency=max_concurrency)
    
    # Set the desired multipart threshold value (1GB)
    GB = 1024 ** 3
    config = TransferConfig(multipart_threshold=1*GB)

    # Enabled thread use/transfer concurrency
    config = TransferConfig(use_threads=True)
    
    # Upload the file
    try:
        response = s3.upload_file(
            file_name, bucket, object_name,
            ExtraArgs={'Metadata': metadata},
            Callback=ProgressPercentage(file_name),
            Config=config
        )
    except ClientError as e:
        print(f"Error uploading file: {e}")
        return False
    return True

########################################################################################
# Main Function
########################################################################################

def main_test(file_name, json_file):

    try:
        with open("event_bridge_data.json", "r") as f:
            event_data = json.load(f)
    except FileNotFoundError:
        print("File event_bridge_data.json not found.")
        sys.exit(1)
    else:
        raw_bucket = event_data["event_bridge_data"]["bucket"]["name"]
        stage_bucket = event_data["stage_bucket"]
        event_bridge_data = event_data["event_bridge_data"]

    try:
        with open(json_file, 'r') as f:
            param_json = json.load(f)
    except FileNotFoundError:
        print(f"File {json_file} not found.")
    else:
        database = "test"
        table_name = param_json["table_name"]
        description = param_json["description"]
        partition_cols = param_json.get("partition_cols")
        required_col = param_json.get("required_col")
        doc_string = param_json.get("doc_string")
        metadata_s3 = param_json.get("metadata_s3")
        
    if check_glue_database(database):
        resp = input(f"Do you want to delete the {database} database? (y/N): ")
        if resp.lower() == "y":
            print(f"\nDeleting database '{database}'...")
            client = boto3.client('glue')
            client.delete_database(Name=database)
            print(f"Database '{database}' deleted.")
        else:
            print("\nContinuing without deleting the database.")

    directory_data = "data"
    # Check if the directory exists
    if not os.path.exists(directory_data):
        # Create the directory if it doesn't exist
        os.makedirs(directory_data)
        print(f"\nCreated folder: {directory_data}\n")

    ppa = pq.read_table(file_name)
    pq.write_table(ppa, "data/filetest001.parquet", compression="SNAPPY")

    metadata = generate_metadata_file(
        ppa=ppa,
        table_name=table_name,
        database=database,
        required_col=required_col,
        partition_cols=partition_cols,
        doc_string=doc_string,
        description=description
    )

    with open("data/metadata.json", 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)

    print("\nUpload metadata file...")
    upload_file(
        file_name='data/metadata.json', 
        bucket=raw_bucket, 
        object_name=f'{database}/{table_name}/metadata.json',
        metadata={'datalake': 'iceberg-test'}
    )
    
    print("\n\nUpload parquet files...")
    # Define the directory path
    directory_path = './data'

    # Use glob to find all .parquet files in the directory
    parquet_files = glob.glob(os.path.join(directory_path, '*.parquet'))
    
    metadata_s3 = metadata_s3 or {}

    for file in parquet_files:
        _, file_obj = os.path.split(file)
        upload_file(
           file_name=file, 
           bucket=raw_bucket, 
           object_name=f'{database}/{table_name}/files/{file_obj}',
           metadata=metadata_s3,
           max_concurrency=20
        )
        
    print("Process event data...\n")

    response = process_event_data(stage_bucket, event_bridge_data)
    
    print(response)

if __name__ == "__main__":
    print("Select file to test:")
    print("===============================================")
    print("   1. yellow_trip_data_part1.parquet")
    print("   2. yellow_trip_data_part2.parquet")
    print("   3. file001.parquet")
    print("   4. file002.parquet")
    print("   0. Exit")

    run_test = input("\nSelect test: ")
        
    if run_test == "1":
        file_name = "test_data/yellow_trip_data_part1.parquet"
        json_file = "ingest_parameters_yellow_trip.json"
    
    elif run_test == "2":
        file_name = "test_data/yellow_trip_data_part2.parquet"
        json_file = "ingest_parameters_yellow_trip.json"
    
    elif run_test == "3":
        file_name = "test_data/file001.parquet"
        json_file = "ingest_parameters_file00x.json"
    
    elif run_test == "4":
        file_name = "test_data/file002.parquet"
        json_file = "ingest_parameters_file00x.json"
    
    else:
        if run_test == "0":
            print("Exiting...")
        else:
            print("Invalid option")
        exit(1)

    main_test(file_name, json_file)