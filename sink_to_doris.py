import csv
import pymysql
import json
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
import os

# Source Configuration
SOURCE_DIR = os.getenv("SOURCE_DIR", "")

# Doris Configuration
DORIS_HOST = os.getenv("DORIS_HOST", "")
DORIS_PORT = int(os.getenv("DORIS_PORT", 9030))
DORIS_USER = os.getenv("DORIS_USER", "")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD", "")
DORIS_DB = "dev"

# Timezone Configuration
TIMEZONE = ZoneInfo("Asia/Shanghai")

# Data source configuration dictionary with updated logic for author_id handling
DATA_SOURCE_CONFIG = {
    "dim_users.csv": [
        {
            "target_table": "dim_users_info",
            "field_mappings": {
                "user_id": "id",
                "user_name": "username",
                "email": "email",
                "user_status": "state",
                "user_tags": lambda row: ["Admin"] if row["is_admin"] == "true" else ["User"],
                "user_attributes": lambda row: {
                    "create_time": row["created_at"],
                    "latest_login_time": row["last_sign_in_at"],
                    "last_activity_date": row["last_activity_on"]
                }
            }
        }
    ],
    "dim_projects.csv": [
        {
            "target_table": "dim_projects_info",
            "field_mappings": {
                "project_id": "id",
                "project_name": "name",
                "project_desc": "description",
                "project_tags": "tag_list",
                "project_metadata": "metadata"
            }
        }
    ],
    "dim_groups.csv": [
        {
            "target_table": "dim_groups_info",
            "field_mappings": {
                "group_id": "id",
                "group_name": "name",
                "group_desc": "description",
                "group_members": "members",
                "group_attributes": lambda row: {
                    "visibility": row["visibility"],
                    "create_time": row["created_at"],
                    "path": row["path"]
                }
            }
        }
    ],
    "code_changes.csv": [
        {
            "target_table": "fact_code_changes_records",
            "field_mappings": {
                "operation_type": "operation",
                "author_id": "author_id",
                "time_stamp": "time",
                "content": "message",
                "project_id": "project_id",
                "hash_value": "sha",
                "code_change_metadata": lambda row: {
                    "author": row["author"],
                    "email": row["email"],
                    "mr_state": row["mr_state"]
                }
            }
        }
    ]
}

def convert_value(value):
    if isinstance(value, list):
        return [convert_value(item) for item in value]
    elif isinstance(value, str):
        if value.isdigit():
            return int(value)
        elif value.startswith("[") and value.endswith("]"):
            return [v.strip() for v in value[1:-1].split(',')]
        elif value.startswith("{") and value.endswith("}"):
            try:
                return json.loads(value)
            except:
                return value
        else:
            return value
    return value

# Step 1: Fetch total count from source
def get_total_count(file_path) -> int:
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        return sum(1 for row in reader) - 1  # Exclude header row

# Step 2: Fetch data from in batches
def fetch_from_source(file_path: str, skip: int, batch_size: int) -> List[Dict[str, Any]]:
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        if skip > 0:
            for _ in range(skip):
                next(reader)
        for _ in range(batch_size):
            try:
                row = next(reader)
                data.append(row)
            except StopIteration:
                break
    return data

# Step 3: Transform data
def transform_data(record: Dict[str, Any], target_table: str, file_path: str) -> Dict[str, Any]:
    mapping_info = next((mapping for mapping in DATA_SOURCE_CONFIG[os.path.basename(file_path)] if mapping["target_table"] == target_table), None)
    if not mapping_info:
        raise ValueError(f"Cannot find field mappings for table {target_table}, source file: {file_path}")
    
    transformed_record = {}
    for target_field, source_expression in mapping_info["field_mappings"].items():
        if callable(source_expression):
            value = source_expression(record)
        else:
            value = record.get(source_expression, "")
        
        if target_field == "author_id":
            transformed_record[target_field] = value if value else None
        else:
            transformed_record[target_field] = convert_value(value)

    return transformed_record

# Step 4: Sink data to Doris
def sink_to_doris(rows: List[Dict[str, Any]], target_table: str) -> None:
    connection = pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        database=DORIS_DB
    )
    try:
        with connection.cursor() as cursor:
            placeholders = ', '.join(['%s'] * len(rows[0]))
            columns = ', '.join(rows[0].keys())
            insert_query = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"
            data = []
            for row in rows:
                transformed_row = []
                for value in row.values():
                    if isinstance(value, (list, dict)):
                        transformed_row.append(json.dumps(value, ensure_ascii=False))
                    else:
                        transformed_row.append(value)
                data.append(tuple(transformed_row))
            cursor.executemany(insert_query, data)
            connection.commit()
            print(f"Successfully inserted {len(rows)} records into table {target_table}")
    except Exception as e:
        print(f"Error inserting data into table {target_table}: {e}")
        connection.rollback()
    finally:
        connection.close()

# Main Pipeline
def main():
    for file_path, table_mappings in DATA_SOURCE_CONFIG.items():
        for mapping in table_mappings:
            target_table = mapping["target_table"]
            total_count = get_total_count(os.path.join(SOURCE_DIR, file_path))
            batch_size = 100  # Batch size definition
            for skip in range(0, total_count, batch_size):
                records = fetch_from_source(os.path.join(SOURCE_DIR, file_path), skip, batch_size)
                transformed_records = [transform_data(record, target_table, os.path.join(SOURCE_DIR, file_path)) for record in records]
                sink_to_doris(transformed_records, target_table)

if __name__ == "__main__":
    main()