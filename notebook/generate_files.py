import awswrangler as wr
import pyarrow.parquet as pq
import argparse
import os
import glob
import sys
import threading
import re
import json
import uuid
import sys

def map_types():
    return {
        "timestamp[ns]": "timestamp",
        "timestampt": "timestamp",
        "double": "double",
        "string": "string",
        "int": "int",
        "int8": "int",
        "int16": "int",
        "int32": "int",
        "int64": "long",
        "float": "float",
        "float16": "float",
        "halffloat": "float",
        "float32": "float",
        "float64": "double",
        "bool": "boolean",
        "binary": "binary",
        "date32[day]": "date",
        "date64[ms]": "date",
        "date": "date",
        "date32": "date",
        "date64": "date",
        "decimal128": "decimal",  # Assuming you handle decimal precision and scale separately
        "list": "list",
        "list<item: int32>": "list",
        "list<item: int64>": "list",
        "list<item: string>": "list",
        "list<element: int32 not null>": "list",
        "struct<b_c_int: int32>": "struct",
        "struct": "struct",
        "map": "map",
        "time": "time",
        "time16": "time",
        "time32": "time",
        "uuid" : "string", 
    }
    

def delete_parquet_files(directory):
    """Deletes all .parquet files from the specified directory.

    Args:
        directory: The path to the directory containing the .parquet files.
    """

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".parquet"):
                os.remove(os.path.join(root, file))


def generate_metadata_file(ppa, table_name, database, required_col=None, partition_cols=None, doc_string=None, description=None):
    
    table_name_for = f"{database}.{table_name}"
    # Check table_name
    pattern = r"^[a-z]+\.[a-z][a-z_]+$"
    if not re.search(pattern, table_name_for):
        raise ValueError("Database and table_name must be lower case and must be a UTF-8 string")
    
    required_col = required_col or {}
    partition_cols = partition_cols or []
    doc_string = doc_string or {}
    description = description or " "
    
    col_types = {}
    list_map_types = map_types()

    for col in ppa.column_names:
        col_types[col] = list_map_types[str(ppa.schema.field(col).type)]
    
    fields = []
    id_data = {}
    n = 1
    
    for col in ppa.column_names:
        item = {}
        item["id"] = n
        item["name"] = col
        item["type"] = col_types[col]
        item["required"] = required_col.get(col, False)
        if col not in partition_cols:
            item["doc"] = doc_string.get(col, "")
        id_data[col] = n
        fields.append(item)
        n += 1
        
    field_par_spec = []
    field_id = 1000

    for col in partition_cols:
        field_partition = {}
        field_partition["source-id"] = id_data[col]
        field_partition["field-id"] = field_id
        field_partition["transform"] = "identity"
        field_partition["name"] = col
        field_id += 1
        field_par_spec.append(field_partition)
    
    metadata = {}
    metadata["tableName"] = table_name_for
    metadata["database"] = database
    metadata["properties"] = {"Description": description}
    schemas = []
    schemas.append({
        "type": "struct",
        "fields": fields,
        "schema-id": 0,
        "identifier-field-ids": []
    })
    metadata["schemas"] = schemas
    
    partition_specs_data = []
    partition_specs_data.append({
        "spec-id": 0,
        "fields": field_par_spec
        }
    )
    metadata["partition-specs"] = partition_specs_data
    return metadata


def main(data_file, json_file):
    
    try:
        with open(json_file, 'r') as f:
            param_json = json.load(f)
    except FileNotFoundError:
        print(f"File {json_file} not found.")
        delete_parquet_files("./data")
        sys.exit(1)
    else:
        database = param_json["database"]
        table_name = param_json["table_name"]
        description = param_json["description"]
        partition_cols = param_json.get("partition_cols")
        required_col = param_json.get("required_col")
        doc_string = param_json.get("doc_string")
    
    print("Read data file...")
    ppa = pq.read_table(data_file)  
    df_raw = ppa.to_pandas()
    
    # Normalize all columns names to be compatible with Amazon Athena.
    df = wr.catalog.sanitize_dataframe_columns_names(df_raw, handle_duplicate_columns="rename")
    col_names = df.columns.to_list()
    
    ppa = ppa.rename_columns(col_names)
    
    _id = str(uuid.uuid4())
    file_data_name = f"data/file-{_id}.parquet"
    print(f"Generating parquet file: {file_data_name}")
    pq.write_table(ppa, file_data_name, compression="SNAPPY")
    
    print("Generating metadata file: data/metadata.json")
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



if __name__ == "__main__":
    """
    Application example:
    
    >> python generate_files.py --datafile yellow_tripdata_2019-12.parquet --json_file ingest_parameters.json
    
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--datafile", type=str, required=True)
    parser.add_argument("--json_file", type=str, required=True)

    args = parser.parse_args()
    data_file = args.datafile
    json_file = args.json_file
    
    main(data_file, json_file)
