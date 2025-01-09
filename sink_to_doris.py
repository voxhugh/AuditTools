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
# Data Source Mapping Configuration
DATA_SOURCE_CONFIG = {
    "dim_users.csv": [
        {
            "target_table": "dim_users_info",
            "field_mappings": {
                "user_id": "id",
                "user_name": "username",
                "email": "email",
                "user_status": "state",
                "user_tags": lambda row: ["Admin"] if safe_get(row,'is_admin') == 'True' else ["User"],
                "user_attributes": {
                    "create_time": "created_at",
                    "latest_login_time": "last_sign_in_at",
                    "last_activity_date": "last_activity_on"
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
                "project_metadata": lambda row: extract_from_json_str(safe_get(row, 'metadata'))
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
                "group_attributes": {
                    "visibility": "visibility",
                    "create_time": "created_at",
                    "path": "path"
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
                "code_change_metadata": {
                    "author": "author",
                    "email": "email",
                    "mr_state": "mr_state"
                }
            }
        }
    ],
    "audit_records.csv": [
        {
            "target_table": "fact_user_operations_records",
            "field_mappings": {
                "user_id": "author_id",
                "operation_type": lambda row: row['operation'] if row['operation']!= 'others' else None,
                "operation_time": "time",
                "related_project_id": lambda row: int(safe_get(row, 'entity_id')) if safe_get(row, 'target_type') == 'Project' else None,
                "related_group_id": lambda row: int(safe_get(row, 'entity_id')) if safe_get(row, 'target_type') == 'Group' else None,
                "operation_details": lambda row: json.dumps({
                    "target_type": safe_get(row, 'target_type'),
                    "target_id": safe_get(row, 'target_id'),
                    "pre_post": safe_get(row, 'pre_post')
                }),
                "operation_metadata": lambda row: json.dumps({
                    "last_role": safe_get(row, 'last_role'),
                    "event": safe_get(row, 'event'),
                    "add_info_": safe_get(row, 'add_info_'),
                    "ip": safe_get(row, 'ip')
                })
            },
            "condition": lambda row: safe_get(row, 'entity_type') == 'User'
        },
        {
            "target_table": "fact_project_group_changes_records",
            "field_mappings": {
                "object_type": "entity_type",
                "object_id": "entity_id",
                "change_type": "operation",
                "change_time": "time",
                "operator_id": "author_id",
                "change_details": lambda row: json.dumps({
                    "target_type": safe_get(row, 'target_type'),
                    "target_id": safe_get(row, 'target_id'),
                    "pre_post": safe_get(row, 'pre_post'),
                    "last_role": safe_get(row, 'last_role')
                }),
                "change_metadata": lambda row: json.dumps({
                    "event": safe_get(row, 'event'),
                    "add_info_": safe_get(row, 'add_info_'),
                    "ip": safe_get(row, 'ip')
                })
            },
            "condition": lambda row: safe_get(row, 'entity_type') in ['Project', 'Group']
        }
    ],
    "mr_reviews.csv": [
        {
            "target_table": "fact_audit_records_info",
            "field_mappings": {
                "related_object_id": "mr_id",
                "object_type": lambda _: "Merge Request",
                "reviewers_ids": "reviewers_ids",
                "audit_time": "time",
                "audit_result": "approval_status",
                "comment": "comments",
                "project_id": "project_id",
                "audit_metadata": lambda row: json.dumps({
                    "author_id": safe_get(row, 'author_id'),
                    "assignee_id": safe_get(row, 'assignee_id'),
                    "mr_title": safe_get(row, 'mr_title'),
                    "mr_description": safe_get(row, 'mr_description'),
                    "source_branch": safe_get(row, 'source_branch'),
                    "target_branch": safe_get(row, 'target_branch')
                })
            }
        }
    ],
    "cicd_pipelines.csv": [
        {
            "target_table": "fact_cicd_pipeline_activities_records",
            "field_mappings": {
                "pipeline_id": "pipeline_id",
                "stage_name": "stage",
                "job_name": "job_name",
                "start_time": "time",
                "end_time": "end_time",
                "job_status": "job_status",
                "project_id": "project_id",
                "job_metadata": lambda row: json.dumps({
                    "triggered_by_user": safe_get(row, 'triggered_by'),
                    "environment": safe_get(row, 'environment'),
                    "commit_sha": safe_get(row, 'commit_sha')
                })
            }
        }
    ],
    "cicd_changes.csv": [
        {
            "target_table": "fact_cicd_config_changes_records",
            "field_mappings": {
                "change_type": "change_type",
                "change_detail": "change_content",
                "change_time": "time",
                "project_id": "project_id",
                "config_metadata": lambda row: json.dumps({
                    "author_name": safe_get(row, 'author'),
                    "message": safe_get(row, 'message'),
                    "commit_hash_value": safe_get(row, 'commit_sha')
                })
            }
        }
    ],
    "all_system_changes.csv": [
        {
            "target_table": "fact_system_config_changes_records",
            "field_mappings": {
                "change_object": "entity_type",
                "change_type": "event",
                "change_detail": lambda row: json.dumps({
                    "name": safe_get(row, 'entity_name'),
                    "description": safe_get(row, 'entity_description'),
                    "details": safe_get(row, 'entity_details'),
                    "hook_events": safe_get(row, 'hook_events'),
                    "flag_state": safe_get(row, 'flag_state')
                }),
                "change_time": "time"
            }
        }
    ]
}

METADATA_MAP = {
    'path': 'path_with_namespace',
    'create_time': 'created_at',
    'main_branch': 'default_branch',
    'last_activity_time': 'last_activity_at',
    'forks': 'forks_count',
    'stars':'star_count'
}

def safe_get(dictionary, key, default=""):
    return dictionary.get(key, default)

def fix_json_str(metadata_str: str) -> str:
    fixed_str = metadata_str.replace("'", '"').replace('None', 'null')
    return fixed_str

def extract_from_json_str(metadata_str: str) -> Dict[str, Any]:
    if not isinstance(metadata_str, str) or not metadata_str.strip():
        print("Warning: Metadata field is empty or not a valid string.")
        return {}
    try:
        metadata_str_fixed = fix_json_str(metadata_str)
        metadata_dict = json.loads(metadata_str_fixed)
        filtered_metadata = {key: metadata_dict.get(METADATA_MAP[key]) if METADATA_MAP[key] in metadata_dict else None
                             for key in METADATA_MAP}
        return {k: v for k, v in filtered_metadata.items() if v is not None}
    except json.JSONDecodeError as e:
        print(f"Warning: Invalid JSON format in metadata field - {e}")
        print(f"Problematic metadata string: {metadata_str[:50]}...")
        return {}
    except Exception as e:
        print(f"Unexpected error processing metadata field: {e}")
        return {}

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
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            return sum(1 for row in reader) - 1  # Exclude header row
    except Exception as e:
        print(f"Error fetching total count from {file_path}: {e}")
        return 0

# Step 2: Fetch data from in batches
def fetch_from_source(
    file_path,
    skip: int,
    batch_size: int
) -> List[Dict[str, Any]]:
    data = []
    try:
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
    except Exception as e:
        print(f"Error fetching data from {file_path}: {e}")
    return data

# Step 3: Transform data
def transform_data(record: Dict[str, Any], target_table: str, file_path: str) -> Optional[Dict[str, Any]]:
    try:
        mapping_info = next((mapping for mapping in DATA_SOURCE_CONFIG[os.path.basename(file_path)] if safe_get(mapping,'target_table') == target_table), None)
        if not mapping_info:
            return None
        condition = safe_get(mapping_info,'condition')
        if condition and not condition(record):
            return None

        transformed_record = {}
        for target_field, source_expression in mapping_info["field_mappings"].items():
            if callable(source_expression):
                value = source_expression(record)
                if target_field == 'operation_type' and value is None:
                    return None
            elif isinstance(source_expression, dict):
                value = {sub_key: safe_get(record, sub_value) for sub_key, sub_value in source_expression.items()}
            else:
                value = safe_get(record, source_expression)
            transformed_record[target_field] = convert_value(value)
        return transformed_record if any(transformed_record.values()) else None
    except Exception as e:
        print(f"Error transforming data from {file_path}: {e}")
        return None

# Step 4: Sink data to Doris
def sink_to_doris(rows: List[Dict[str, Any]], target_table: str) -> None:
    try:
        connection = pymysql.connect(
            host=DORIS_HOST,
            port=DORIS_PORT,
            user=DORIS_USER,
            password=DORIS_PASSWORD,
            database=DORIS_DB
        )
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
            print(f"Total records to process for {file_path}: {total_count}")
            batch_size = 100  # Batch size definition
            for skip in range(0, total_count, batch_size):
                print(f"Processing batch for {file_path}: {skip} to {skip + batch_size}")
                records = fetch_from_source(os.path.join(SOURCE_DIR, file_path), skip, batch_size)
                transformed_records = [transform_data(record, target_table, os.path.join(SOURCE_DIR, file_path)) for record in records]
                valid_records = [rec for rec in transformed_records if rec is not None]
                if valid_records:
                    sink_to_doris(valid_records, target_table)

if __name__ == "__main__":
    main()