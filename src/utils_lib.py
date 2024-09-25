from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.exceptions import NoSuchTableError
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.csv as csv
import s3fs
import boto3
from botocore.exceptions import ClientError
import uuid
import re
import json


#####################################################################
# 1. Read Metadata
#####################################################################

def read_metadata(raw_bucket, file_key):
    """
    Read metadata from a JSON file stored in an S3 bucket.

    Parameters:
    raw_bucket (str): The name of the S3 bucket.
    file_key (str): The key (path) to the file in the S3 bucket.

    Returns:
    dict or bool: The metadata as a dict if successful, False if an error occurs.
    """
    
    s3 = boto3.client('s3')
    folder_metadata = "/".join(file_key.split("/")[:-2])
    key_metadata = folder_metadata + "/metadata.json"
    
    try:
        response = s3.get_object(Bucket=raw_bucket, Key=key_metadata)
        metadata = json.loads(response['Body'].read().decode('utf-8'))
    
    except ClientError as e:
        print(f"[ERROR] Can't find metadata file in this path: {folder_metadata} because {e}")
        return False
    
    except Exception as e:
        print(f"[ERROR] {e}")
        return False
    
    else:
        return metadata


#####################################################################
# 2. Check Glue database
#####################################################################
    
def check_glue_database(glue_database):
    """
    Check if an AWS Glue database exists, and create it if it does not.

    Parameters:
    glue_database (str): The name of the AWS Glue database to check or create.

    Returns:
    bool: True if the database exists or is successfully created, False if an error occurs.
    """
    
    glue_client = boto3.client("glue")
    try:
        glue_client.get_database(Name=glue_database)
        print(f"Database {glue_database} already exists")
        return True
    
    except ClientError:
        glue_client.create_database(DatabaseInput={'Name': glue_database})
        print(f"Database {glue_database} created")
        return True
    
    except Exception as e:
        print(f"[ERROR] {e}")
        return False


#####################################################################
# 3. Check table
#####################################################################

def check_table(stage_bucket, glue_database, table_name, schema_json, partition_data, properties):
    """
    Check if an Iceberg table exists in the AWS Glue Catalog, and create it if it does not.

    Parameters:
    stage_bucket (str): The S3 bucket where the table data is stored.
    glue_database (str): The AWS Glue database name.
    table_name (str): The name of the table to check or create.
    schema_json (dict): The schema to apply to the table.
    partition_data (str): The partitioning scheme to use.
    properties (dict): Additional properties for the table.

    Returns:
    Table or None: The loaded or newly created table if successful, otherwise None if an error occurs.
    """
    schema_json_dump = json.dumps(schema_json)
    schema_data = Schema.model_validate_json(schema_json_dump)
    table_name_formatted = table_name.split(".")[-1]
            
    # Instantiate glue catalog
    catalog = load_catalog("glue", **{"type": "glue"})
    
    try:
        tbl = catalog.load_table(table_name)
    
    except NoSuchTableError:
        print(f"Table {table_name_formatted} does not exist")
        table_exist = False
    
    except Exception as e:
        print(f"[ERROR] Error loading table {table_name_formatted}: {e}")
        return None
    
    else:
        table_exist = True
    
    
    if table_exist:
        # Compare schema
        json_glue_database = tbl.schema().model_dump_json()
        glue_fields = json.loads(json_glue_database)["fields"]
        metadata_fields = schema_json["fields"]
        
        if len(metadata_fields) > len(glue_fields):
            response = update_schema(tbl, glue_fields, metadata_fields)
            if response:
                print(f"Schema updated for table {table_name}")
    
    else:
        # If the table doesn't exist, create it
        try:
            tbl = catalog.create_table(
                    identifier=table_name,
                    location=f"s3://{stage_bucket}/{glue_database}/{table_name_formatted}",
                    schema=schema_data,
                    partition_spec=partition_data,
                    properties=properties
            )

        except Exception as e:
            print(f"[ERROR] Error creating table {table_name_formatted}: {e}")
            return None
            
        print(f"Iceberg table: {table_name_formatted} created using AWS Glue Catalog")
    
    return tbl    

#####################################################################
# 4. Load file data
#####################################################################

def load_file(file_path, schema, partition):
    """
    Load a Parquet or CSV file from an S3 bucket using a specified schema and partitioning.

    Parameters:
    file_path (str): The S3 path to the Parquet or CSV file.
    schema (pyiceberg.Schema): The schema to apply to the data file.
    partition (str): The partitioning scheme to use.

    Returns:
    pyarrow.Table or None: The loaded Parquet file as a PyArrow Table if successful, 
                           otherwise None if an error occurs.
    """
    s3_fs = s3fs.S3FileSystem()
    file_system = s3_fs
    csv_extensions = ["csv", "csv.gz"]
    
    if check_file_extension(file_path, csv_extensions):
        try:
            with s3_fs.open(file_path, 'rb') as source:
                table = csv.read_csv(source)

        except Exception as e:
            print(f"[ERROR] Error loading file: {file_path} - {e}")
            return None

        else:
            # Save the CSV data to parquet locally
            _id = str(uuid.uuid4())
            temp_dir = f"/tmp/{_id}"
            pq.write_to_dataset(
                table, 
                root_path=temp_dir
            )
            file_path = temp_dir
            file_system = None
            
    try:
        df = pq.read_table(
            source=file_path,
            coerce_int96_timestamp_unit="us",
            schema=schema.as_arrow(),
            partitioning=partition,
            filesystem=file_system
        )
    
    except Exception as e:
        print(f"[ERROR] Error loading file: {file_path} - {e}")
        return None
    
    else:
        return df
    
    
#####################################################################
# 5. Check if partition colums don't have null values
#####################################################################

def check_partition_columns(df, partition):
    """
    Check if the partition columns in a DataFrame contain any null values.

    Parameters:
    df (pyarrow.DataFrame): The DataFrame to check.
    partition (Partition): The partition object containing the source_id_to_fields_map attribute.

    Returns:
    bool or None: True if no null values are found in the partition columns, 
                  None if null values are present and an error message is printed.
    """

    field_map = partition.source_id_to_fields_map
    col_names = [field.name for fields in field_map.values() for field in fields]
    
    try:
        total_nan = check_nan_count(df, col_names)

    except Exception as e:
        print(f"[ERROR] Error checking partition columns. {e}")
        return None
    
    else:
        if total_nan == 0:
            return True
        
        else:
            print(f"[ERROR] Partition column(s) {col_names} have null values")
            return None


######################################################################
# 6. Append data to table
#####################################################################

def append_data_table(tbl, file_data):
    """
    Append data to an existing table.

    Parameters:
    tbl (Table): The table to which data will be appended.
    file_data (pyarrow.DataFrame): The data to append to the table.

    Returns:
    bool: True if the data is successfully appended, False if an error occurs.
    """
    
    try:
        tbl.append(file_data)
        return True
    
    except Exception as e:
        print(f"[ERROR] Error writing data to table: {e}")
        return False
    
    else:
        print("Successful writing of the data to database")
        

######################################################################
# Check NAN count
#####################################################################

def check_nan_count(df, col_names):
    """
    Check the number of NaN values in each column of a DataFrame.

    Parameters:
    - df: PyArrow Table or DataFrame

    Returns:
    - Count of NaN values in each column
    """
    
    sum_n = 0
    for column in col_names:
        try:
            array = df[column]
            null_mask = pc.is_null(array)
            nan_count = pc.sum(null_mask).as_py()
        
        except Exception as e:
            print(f"[ERROR] {e}")
            nan_count = 1
        
        sum_n += nan_count
    
    return sum_n


######################################################################
# Update schema
######################################################################

def update_schema(tbl, glue_schema, metadata_schema):
    
    diff_count = len(metadata_schema) - len(glue_schema)
    new_glue_fields = metadata_schema[-diff_count:]
    
    new_schema = {
        'type': 'struct',
        'fields': new_glue_fields,
        'schema-id': 0,
        'identifier-field-ids': []
    }
    
    try:
        
        new_schema_data = Schema.model_validate_json(json.dumps(new_schema))    
        
        for col in new_schema_data.fields:
            with tbl.update_schema() as update:
                update.add_column(
                            col.name, 
                            field_type=col.field_type, 
                            doc= col.doc, 
                            required= col.required
                )
    
    except Exception as e:
        print(f"[ERROR] Error updating schema: {e}")
        return False
    
    else:
        return True
    
######################################################################
# Check file extension
######################################################################
    
def check_file_extension(path, extensions):
    """
    Checks if a single file path has the specified extension using regular expressions.

    Args:
        path: The file path to check.
        extensions: A list of file extensions to look for (e.g., ["csv", "csv.gz"]).

    Returns:
        True if the file path matches one of the specified extensions, False otherwise.
    """

    pattern = r"(\.(" + "|".join(extensions) + "))$"
    return re.search(pattern, path, re.IGNORECASE) is not None
