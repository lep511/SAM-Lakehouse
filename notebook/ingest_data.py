import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import argparse
import os
import glob
import sys
import threading
import re
import json
import uuid
import sys

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


def delete_parquet_files(directory):
    """Deletes all .parquet files from the specified directory.

    Args:
        directory: The path to the directory containing the .parquet files.
    """

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                os.remove(os.path.join(root, file))
                

def ingest_data_files(metadata_s3):
    
    try:
        with open("data/metadata.json", 'r') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        print("File << data/metadata.json >> not found.")
        sys.exit(1)
    else:
        database = metadata["database"]
        table_name = metadata["tableName"].split(".")[-1]

    # Create an S3 client
    s3 = boto3.client('s3')
    
    # List all buckets
    response = s3.list_buckets()

    # Filter buckets that start with 'ingest-data'
    buckets = [bucket['Name'] for bucket in response['Buckets'] if bucket['Name'].startswith('raw-datalake-iceberg-')]

    if len(buckets) == 0:
        print("No RAW bucket found.")
        delete_parquet_files("./data")
        sys.exit(1)
    elif len(buckets) == 1:
        print(f"Found bucket: {buckets[0]}")
        bucket_name = buckets[0]
    else:
        print("Multiple buckets found:")
        bucket_dict = {i+1: bucket for i, bucket in enumerate(buckets)}
        for key, value in bucket_dict.items():
            print(f"{key}: {value}")
        print("-------------------------------------")
        choice = input("Enter the number of the bucket you want to use: ")
        if choice.isdigit() and int(choice) in bucket_dict:
            bucket_name = bucket_dict[int(choice)]
        else:
            print("Invalid choice. Exiting.")
            delete_parquet_files("./data")
            sys.exit(1)
        print(f"Using bucket: {bucket_name}")
        
    print("\nUpload metadata file...")
    upload_file(
        file_name='data/metadata.json', 
        bucket=bucket_name, 
        object_name=f'icebergdatalake/{database}/{table_name}/metadata.json',
        metadata={'datalake': 'iceberg-datalake'}
    )
    
    print("\n\nUpload data files...")
    # Define the directory path
    directory_path = './data'
    
    files = []
    extensions = ['.parquet', '.csv', '.gz']

    for root, dirs, filenames in os.walk(directory_path):
        for filename in filenames:
            file_extension = os.path.splitext(filename)[1].lower()
            if file_extension in extensions:
                files.append(os.path.join(root, filename))
    
    metadata_s3 = metadata_s3 or {}

    if not files:
        print("No files data found in the specified directory.")
        sys.exit(1)
    
    # Print the list of .parquet files
    for file in files:
        _, file_obj = os.path.split(file)
        upload_file(
            file_name=file, 
            bucket=bucket_name, 
            object_name=f'icebergdatalake/{database}/{table_name}/files/{file_obj}',
            metadata=metadata_s3,
            max_concurrency=20
        )

if __name__ == "__main__":
    """
    Application example:
    
    >> python ingest_data.py --metadata ingest_parameters.json
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata_s3", type=str, required=True)
    args = parser.parse_args()
    json_metadata_s3 = args.metadata_s3
    
    try:
        with open(json_metadata_s3, 'r') as f:
            data_file = json.load(f)
    except FileNotFoundError:
        print(f"File << data/metadata.json >> not found.")
        sys.exit(1)
    else:
        metadata_s3 = data_file["metadata_s3"]

    ingest_data_files(metadata_s3)

