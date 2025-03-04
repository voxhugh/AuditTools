import asyncio
import logging
import os
import re
import json
import uuid
from datetime import datetime
from difflib import unified_diff
from typing import List, Dict, Any, Tuple, Optional, Union
from concurrent.futures import ThreadPoolExecutor
import httpx
import pymysql
import base64

# ----------------------- Configuration and constants -----------------------
DORIS_HOST = os.getenv("DORIS_HOST", "")
DORIS_PORT = int(os.getenv("DORIS_PORT", 9030))
DORIS_USER = os.getenv("DORIS_USER", "")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD", "")
DORIS_DB = "gitlab"
BATCH_SIZE = 1000
GITLAB_URL = os.getenv("GITLAB_URL", "")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "")
HEADERS = {"PRIVATE-TOKEN": ACCESS_TOKEN}
PER_PAGE = 100
TIMEZONE = os.getenv("TIMEZONE", "Asia/Shanghai")
SINCE_ = None  # Start time: ${START}
UNTIL_ = None  # End time: ${END}

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ----------------------- Data Source Configuration -----------------------
DATA_SOURCE_CONFIG: Dict[str, List[str]] = {
    'dim_users_info': ["user_id", "user_name", "nickname", "email", "user_status", "user_tags", "user_attributes"],
    'dim_projects_info': ["project_id", "project_name", "project_desc", "project_tags", "project_metadata"],
    'dim_groups_info': ["group_id", "group_name", "group_desc", "group_members", "group_attributes"],
    'fact_code_changes_records': ["code_change_id", "operation_type", "author_id", "time_stamp", "content", "project_id", "hash_value", "code_change_metadata"],
    'fact_audit_records_info': ["audit_id", "related_object_id", "object_type", "reviewers_ids", "audit_time", "audit_result", "comment", "project_id", "audit_metadata"],
    'fact_cicd_pipeline_activities_records': ["activity_id", "pipeline_id", "stage_name", "job_name", "start_time", "end_time", "job_status", "project_id", "job_metadata"],
    'fact_cicd_config_changes_records': ["config_change_id", "change_type", "change_detail", "change_time", "project_id", "config_metadata"],
    'fact_user_operations_records': ["operation_id", "user_id", "operation_type", "operation_time", "related_project_id", "related_group_id", "operation_details", "operation_metadata"],
    'fact_project_group_changes_records': ["change_id", "object_type", "object_id", "change_type", "change_time", "operator_id", "change_details", "change_metadata"],
    'fact_system_config_changes_records': ["config_change_id", "change_object", "change_type", "change_detail", "change_time"]
}

operation_patterns = {
    "create": [re.compile(r"\b(add|created|added|new|inserted|registered|generate)\b", re.IGNORECASE),
               re.compile(r"\b(create|generation|generated)\b", re.IGNORECASE)],
    "update": [re.compile(r"\b(change|updated|modify|modified|edit|edited|alter|altered)\b", re.IGNORECASE),
               re.compile(r"\b(update|revision|revised)\b", re.IGNORECASE)],
    "delete": [re.compile(r"\b(remove|destroyed|deleted|eliminated|dropped|unregister)\b", re.IGNORECASE),
               re.compile(r"\b(delete|deletion|deregister)\b", re.IGNORECASE)]
}
since_dt = datetime.fromisoformat(SINCE_.replace('Z', '+00:00')) if SINCE_ else None
until_dt = datetime.fromisoformat(UNTIL_.replace('Z', '+00:00')) if UNTIL_ else None

# ----------------------- Helper functions -----------------------
def safe_get(d: dict, key: str, default=None) -> Any:
    return d.get(key, default) if isinstance(d, dict) else default

def safe_chain_get(d: dict, *keys, default=None) -> Any:
    result = d
    for key in keys:
        if result is None:
            return default
        result = result.get(key)
    return result if result is not None else default

def time_filters(url: str, since__param: str = 'updated_after', until__param: str = 'updated_before') -> str:
    query_params = []
    if SINCE_:
        query_params.append(f"{since__param}={SINCE_}")
    if UNTIL_:
        query_params.append(f"{until__param}={UNTIL_}")
    if query_params:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}{'&'.join(query_params)}"
    return url

def is_within_time(record: dict) -> bool:
    event_time_str = safe_get(record, "updated_at") or safe_get(record, "created_at")
    if not event_time_str:
        return True
    event_datetime = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
    return not since_dt or since_dt <= event_datetime <= until_dt if until_dt else True

def classify_event_operation(event: dict, keys: List[str], nested_keys: Optional[List[str]] = None) -> str:
    nested_keys = nested_keys or []
    for key_path in [keys, nested_keys]:
        try:
            current_dict = event
            for key in key_path:
                current_dict = current_dict[key]
            if isinstance(current_dict, dict):
                for k in current_dict.keys():
                    if (found_op := check_keywords(k)):
                        return found_op
            elif isinstance(current_dict, list):
                for item in current_dict:
                    if (found_op := check_keywords(item)):
                        return found_op
            elif (found_op := check_keywords(current_dict)):
                return found_op
        except (KeyError, TypeError):
            continue
    return "others"

def check_keywords(value: str, patterns=operation_patterns) -> Optional[str]:
    if not value:
        return None
    value_str = str(value).lower()
    for op_type, compiled_patterns in patterns.items():
        if any(pattern.search(value_str) for pattern in compiled_patterns):
            return op_type
    return None

def format_change_details(event: dict) -> str:
    change = safe_chain_get(event, "details", "change", default=None)
    if change is not None:
        from_ = safe_chain_get(event, "details", "from", default="")
        to_ = safe_chain_get(event, "details", "to", default="")
        return f"{from_}:{to_}"
    else:
        return ""

def convert_to_json(value: Any) -> Union[str, Any]:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    elif isinstance(value, datetime):
        return value.isoformat()
    return value

def fix_json_str(metadata_str: str) -> str:
    return metadata_str.replace("'", '"').replace('None', 'null')

def extract_from_json_str(metadata_str: str) -> Dict[str, Any]:
    if not isinstance(metadata_str, str) or not metadata_str.strip():
        logging.warning("Metadata field is empty or not a valid string.")
        return {}
    try:
        # Attempt to fix common issues in the JSON string
        fixed_metadata_str = fix_json_str(metadata_str)
        # Try to parse the fixed JSON string into a Python dictionary
        metadata_dict = json.loads(fixed_metadata_str)
        return metadata_dict
    except json.JSONDecodeError as e:
        logging.warning(f"Invalid JSON format in metadata field - {e}")
        logging.warning(f"Problematic metadata string: {metadata_str[:50]}...")
    except Exception as e:
        logging.error(f"Unexpected error processing metadata field: {e}")
    return {}

def compare_yml_files(old_content: Optional[str], new_content: Optional[str]) -> Tuple[str, str]:
    if old_content is None:
        old_content = ''
    if new_content is None:
        new_content = ''
    if not old_content and new_content:
        return "added", ''
    elif old_content and not new_content:
        return "deleted", ''
    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)
    diff = list(unified_diff(old_lines, new_lines, fromfile='old', tofile='new'))
    return ("modified", ''.join(diff)) if diff else ('', '')

async def file_content(client: httpx.AsyncClient, project_id: int, commit_sha: str, file_path: str) -> str:
    endpoint = f"projects/{project_id}/repository/files/{file_path}"
    content_url = f"{GITLAB_URL}/{endpoint}?ref={commit_sha}"
    response = await make_api_request(client, content_url, HEADERS)
    if 'content' in response:
        return base64.b64decode(response['content']).decode('utf-8')
    return ""

async def mr_notes(client: httpx.AsyncClient, project_id: int, mr_iid: int) -> List[Dict[str, Any]]:
    endpoint = f"projects/{project_id}/merge_requests/{mr_iid}/notes"
    notes_url = f"{GITLAB_URL}/{endpoint}"
    notes = await make_api_request(client, notes_url, HEADERS)
    return [
        {
            "commenter":  safe_chain_get(note,"author","username",default=""),
            "content": safe_get(note,"body"),
            "time": safe_get(note,"created_at")
        } for note in notes
    ]

async def make_api_request(client: httpx.AsyncClient, url: str, headers: Dict[str, str]) -> Any:
    try:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error occurred: {e.response.status_code} - {e.request.url}")
    except (httpx.RequestError, json.JSONDecodeError) as e:
        logging.error(f"API request failed: {e}")
    return []

def process_system_change_records(records: List[Dict], event_type: str, entity_type: str) -> List[Dict[str, Any]]:
    processed_records = []
    for record in records:
        event_time = safe_get(record, "updated_at", default=None) or safe_get(record, "created_at", default="1970-01-01T00:00:00Z")
        entity_name = safe_get(record, "name", default="")
        entity_description = safe_get(record, "description", default="")
        entity_details = safe_get(record, "url", default=None) or safe_get(record, "version", default="")
        hook_events = ', '.join([k for k, v in record.items() if k.endswith('_events') and v])
        flag_state = "active" if safe_get(record, "active") else "inactive"
        
        system_change_record = {
            "config_change_id": str(uuid.uuid4()),
            "change_object": entity_type,
            "change_type": event_type,
            "change_detail": json.dumps({
                "name": entity_name,
                "description": entity_description,
                "details": entity_details,
                "hook_events": hook_events,
                "flag_state": flag_state
            }),
            "change_time": event_time
        }
        processed_records.append(system_change_record)
    return processed_records

# ----------------------- Fetch Functions -----------------------
async def fetch_users(client: httpx.AsyncClient) -> None:
    page = 1
    batch = []
    logging.info("Acquiring Users Info, page 1...")
    while True:
        url = f"{GITLAB_URL}/users?order_by=updated_at&page={page}&per_page={PER_PAGE}"
        try:
            response = await make_api_request(client, url, HEADERS)
            if not response or isinstance(response, dict):
                break
            for user in response:
                user_info = {
                    "user_id": safe_get(user, "id", default=-1),
                    "user_name": safe_get(user, "username"),
                    "nickname": safe_get(user, "name"),
                    "email": safe_get(user, "email"),
                    "user_status": safe_get(user, "state"),
                    "user_tags": ["Admin"] if safe_get(user, "is_admin") else ["User"],
                    "user_attributes": json.dumps({
                        "create_time": safe_get(user, "created_at"),
                        "latest_login_time": safe_get(user, "last_sign_in_at"),
                        "last_activity_date": safe_get(user, "last_activity_on")
                    })
                }
                batch.append(user_info)
                if len(batch) >= BATCH_SIZE:
                    await process_and_insert_data({"dim_users_info": batch})
                    batch = []
            page += 1
            logging.info(f"Acquiring Users Info, page {page}...")
        except Exception as e:
            logging.error(f"Error fetching users from {url}: {e}")
    if batch:
        await process_and_insert_data({"dim_users_info": batch})

async def fetch_projects(client: httpx.AsyncClient) -> None:
    batch = []
    endpoint = "projects"
    proj_url = f"{GITLAB_URL}/{endpoint}?simple=true"
    logging.info("Acquiring Projects Info...")
    response = await make_api_request(client, proj_url, HEADERS)
    if response:
        for proj in response:
            proj_id = safe_get(proj, "id")
            if proj_id:
                project_info = {
                    "project_id": proj_id,
                    "project_name": safe_get(proj, "name"),
                    "project_desc": safe_get(proj, "description"),
                    "project_tags": safe_get(proj, "tag_list"),
                    "project_metadata": json.dumps(extract_from_json_str(safe_get(proj,'metadata')))
                }
                batch.append(project_info)
                if len(batch) >= BATCH_SIZE:
                    await process_and_insert_data({"dim_projects_info": batch})
                    batch = []
    if batch:
        await process_and_insert_data({"dim_projects_info": batch})

async def fetch_groups(client: httpx.AsyncClient) -> None:
    page = 1
    batch = []
    logging.info("Acquiring Groups Info, page 1...")
    while True:
        url = f"{GITLAB_URL}/groups?page={page}&per_page={PER_PAGE}"
        try:
            response = await make_api_request(client, url, HEADERS)
            if not response or isinstance(response, dict):
                break
            for group in response:
                group_id = safe_get(group, "id", default=-1)
                members_info = []
                member_endpoint = f"groups/{group_id}/members"
                member_url = f"{GITLAB_URL}/{member_endpoint}"
                member_response = await make_api_request(client, member_url, HEADERS)
                if member_response:
                    members_info.extend(member_response)
                group_members = [member["id"] for member in members_info if safe_get(member, "id")]
                group_info = {
                    "group_id": group_id,
                    "group_name": safe_get(group, "name"),
                    "group_desc": safe_get(group, "description"),
                    "group_members": json.dumps(group_members),
                    "group_attributes": json.dumps({
                        "visibility": safe_get(group, "visibility"),
                        "create_time": safe_get(group, "created_at"),
                        "path": safe_get(group, "path")
                    })
                }
                batch.append(group_info)
                if len(batch) >= BATCH_SIZE:
                    await process_and_insert_data({"dim_groups_info": batch})
                    batch = []
            page += 1
            logging.info(f"Acquiring Groups Info, page {page}...")
        except Exception as e:
            logging.error(f"Error fetching groups: {e}")
    if batch:
        await process_and_insert_data({"dim_groups_info": batch})

async def fetch_code_changes(client: httpx.AsyncClient, project_id: int) -> List[Dict[str, Any]]:
    all_code_changes = []
    # commit
    commits_endpoint = f"projects/{project_id}/repository/commits"
    commits_url = f"{GITLAB_URL}/{commits_endpoint}"
    commits_url = time_filters(commits_url,'since', 'until')
    try:
        commits = await make_api_request(client, commits_url, HEADERS)
        if commits:
            for commit in commits:
                commit_record = {
                    "code_change_id": str(uuid.uuid4()),
                    "operation_type": "commit",
                    "author_id": "",
                    "time_stamp": safe_get(commit, "committed_date"),
                    "content": safe_get(commit, "message"),
                    "project_id": project_id,
                    "hash_value": safe_get(commit, "id"),
                    "code_change_metadata": json.dumps({
                        "author": safe_get(commit, "committer_name"),
                        "email": safe_get(commit, "committer_email"),
                    })
                }
                all_code_changes.append(commit_record)
    except Exception as e:
        logging.error(f"Error fetching commits for project {project_id}: {e}")

    # merge request
    mr_endpoint = f"projects/{project_id}/merge_requests"
    merge_requests_url = f"{GITLAB_URL}/{mr_endpoint}"
    merge_requests_url = time_filters(merge_requests_url)
    try:
        merge_requests = await make_api_request(client, merge_requests_url, HEADERS)
        if merge_requests:
            for merge_request in merge_requests:
                author_id = safe_chain_get(merge_request, "author", "id", default=-1)
                author = safe_chain_get(merge_request, "author", "name", default="")
                merge_record = {
                    "code_change_id": str(uuid.uuid4()),
                    "operation_type": "merge_request",
                    "author_id": author_id,
                    "time_stamp": safe_get(merge_request, "updated_at"),
                    "content": safe_get(merge_request, "title"),
                    "project_id": project_id,
                    "hash_value": "",
                    "code_change_metadata": json.dumps({
                        "author": author,
                        "mr_state": safe_get(merge_request, "state")
                    })
                }
                all_code_changes.append(merge_record)
    except Exception as e:
        logging.error(f"Error fetching merge requests for project {project_id}: {e}")

    # push
    push_endpoint = f"projects/{project_id}/events"
    pulls_url = f"{GITLAB_URL}/{push_endpoint}?action=pushed"
    pulls_url = time_filters(pulls_url, 'after', 'before')
    try:
        events = await make_api_request(client, pulls_url, HEADERS)
        if events:
            for event in events:
                pull_record = {
                    "code_change_id": str(uuid.uuid4()),
                    "operation_type": "push",
                    "author_id": safe_get(event, "author_id"),
                    "time_stamp": safe_get(event, "created_at"),
                    "content": f"commit from:{safe_chain_get(event, 'push_data', 'commit_from', default='')}",
                    "project_id": project_id,
                    "hash_value": safe_chain_get(event, 'push_data', 'commit_to', default=''),
                    "code_change_metadata": json.dumps({
                        "author": safe_chain_get(event, "author", "name", default=""),
                    })
                }
                all_code_changes.append(pull_record)
    except Exception as e:
        logging.error(f"Error fetching push events for project {project_id}: {e}")

    return all_code_changes

async def fetch_mr_records(client: httpx.AsyncClient, project_id: int) -> List[Dict[str, Any]]:
    all_mr_records = []
    merge_requests_endpoint = f"projects/{project_id}/merge_requests"
    merge_requests_url = f"{GITLAB_URL}/{merge_requests_endpoint}"
    merge_requests_url = time_filters(merge_requests_url)
    try:
        merge_requests = await make_api_request(client, merge_requests_url, HEADERS)
        for merge_request in merge_requests:
            mr_id = safe_get(merge_request, "iid")
            reviewers_ids = [r["id"] for r in safe_get(merge_request, "reviewers", [])]
            audit_id = str(uuid.uuid4())
            audit_metadata = json.dumps({
                key: safe_chain_get(merge_request, *path, default=-1 if "id" in key else None)
                for key, path in {
                    "author_id": ["author", "id"],
                    "assignee_id": ["assignee", "id"],
                    "mr_title": ["title"],
                    "mr_description": ["description"],
                    "source_branch": ["source_branch"],
                    "target_branch": ["target_branch"]
                }.items()
            })
            status_updates = [
                ("created_at", "opened"),
                ("updated_at", "approved"),
                ("merged_at", "merged"),
                ("closed_at", "closed")
            ]
            for timestamp_key, status in status_updates:
                timestamp = safe_get(merge_request, timestamp_key)
                if timestamp:
                    record = {
                        "audit_id": audit_id,
                        "related_object_id": mr_id,
                        "object_type": "Merge Request",
                        "reviewers_ids": reviewers_ids,
                        "audit_time": timestamp,
                        "audit_result": status,
                        "comment": [],
                        "project_id": project_id,
                        "audit_metadata": audit_metadata
                    }
                    all_mr_records.append(record)
            notes = await mr_notes(client, project_id, mr_id)
            for note in notes:
                if all_mr_records:
                    all_mr_records[-1]["comment"].append({
                        "commenter": safe_get(note, "commenter"),
                        "content": safe_get(note, "content"),
                        "time": safe_get(note, "time")
                    })
    except Exception as e:
        logging.error(f"Error fetching merge request records for project {project_id}: {e}")
    return all_mr_records

async def fetch_cicd_pipelines(client: httpx.AsyncClient, project_id: int) -> List[Dict[str, Any]]:
    all_pipeline_records = []
    pipelines_endpoint = f"projects/{project_id}/pipelines"
    pipelines_url = f"{GITLAB_URL}/{pipelines_endpoint}"
    pipelines_url = time_filters(pipelines_url)
    try:
        pipelines = await make_api_request(client, pipelines_url, HEADERS)
        if pipelines:
            for pipeline in pipelines:
                pipeline_id = safe_get(pipeline, "id")
                time = safe_get(pipeline, "created_at")
                end_time = safe_get(pipeline, "updated_at")
                commit_sha = safe_get(pipeline, "sha")
                jobs_endpoint = f"projects/{project_id}/pipelines/{pipeline_id}/jobs"
                jobs_url = f"{GITLAB_URL}/{jobs_endpoint}"
                jobs = await make_api_request(client, jobs_url, HEADERS)
                if jobs:
                    for job in jobs:
                        pipeline_record = {
                            "activity_id": str(uuid.uuid4()),
                            "pipeline_id": pipeline_id,
                            "stage_name": safe_get(job, "stage"),
                            "job_name": safe_get(job, "name"),
                            "start_time": safe_get(job, "started_at", default=None) or time,
                            "end_time": safe_get(job, "finished_at") or end_time,
                            "job_status": safe_get(job, "status"),
                            "project_id": project_id,
                            "job_metadata": json.dumps({
                                "triggered_by_user": safe_chain_get(job, "user", "username", default="system"),
                                "environment": safe_chain_get(job, "environment", "name", default=""),
                                "commit_sha": commit_sha
                            })
                        }
                        all_pipeline_records.append(pipeline_record)
    except Exception as e:
        logging.error(f"Error fetching CICD pipeline activities for project {project_id}: {e}")
    return all_pipeline_records

async def track_cicd_config_changes(client: httpx.AsyncClient, project_id: int) -> List[Dict[str, Any]]:
    config_changes = []
    commits_endpoint = f"projects/{project_id}/repository/commits"
    commits_url = f"{GITLAB_URL}/{commits_endpoint}"
    commits_url = time_filters(commits_url,'since', 'until')
    try:
        commits = await make_api_request(client, commits_url, HEADERS)
        if commits:
            for commit in commits:
                commit_sha = safe_get(commit, "id")
                changes_endpoint = f"projects/{project_id}/repository/commits/{commit_sha}/diff"
                changes_url = f"{GITLAB_URL}/{changes_endpoint}"
                changes = await make_api_request(client, changes_url, HEADERS)
                if changes:
                    for change in changes:
                        if change['new_path'] == '.gitlab-ci.yml':
                            old_content = (await file_content(client, project_id, commit["parent_ids"][0],
                                                             safe_get(change, "old_path"))) if "parent_ids" in commit and commit["parent_ids"] else ""
                            new_content = await file_content(client, project_id, commit_sha, safe_get(change, "new_path"))
                            try:
                                change_type, diff = compare_yml_files(old_content, new_content)
                                if change_type:
                                    change_record = {
                                        "config_change_id": str(uuid.uuid4()),
                                        "change_type": change_type,
                                        "change_detail": diff,
                                        "change_time": safe_get(commit, "committed_date"),
                                        "project_id": project_id,
                                        "config_metadata": json.dumps({
                                            "author_name": safe_get(commit, "author_name"),
                                            "message": safe_get(commit, "message"),
                                            "commit_sha": commit_sha
                                        })
                                    }
                                    config_changes.append(change_record)
                            except Exception as e:
                                logging.error(f"Error comparing YML files for project {project_id}: {e}")
    except Exception as e:
        logging.error(f"Error tracking CICD config changes for project {project_id}: {e}")
    return config_changes

async def fetch_audit_records(client: httpx.AsyncClient) -> None:
    audit_events_endpoint = "audit_events"
    base_url = time_filters(f"{GITLAB_URL}/{audit_events_endpoint}", 'created_after', 'created_before')
    page = 1
    user_batch = []
    project_group_batch = []
    logging.info("Fetching audit records, page 1...")
    while True:
        events_url = f"{base_url}{'&' if '?' in base_url else '?'}page={page}&per_page={PER_PAGE}"
        try:
            audit_events = await make_api_request(client, events_url, HEADERS)
            if not audit_events or isinstance(audit_events, dict):
                break
            for event in audit_events:
                author_id = safe_get(event, "author_id", default=-1)
                entity_type = safe_get(event, "entity_type")
                created_at = safe_get(event, "created_at", default="1970-01-01T00:00:00Z")
                operation = classify_event_operation(event, ['event_name'], ['details'])
                common_fields = {
                    "author_id": author_id,
                    "operation_time": created_at,
                    "target_type": safe_chain_get(event, "details", "target_type", default=""),
                    "target_id": safe_chain_get(event, "details", "target_id", default=-1),
                    "pre_post": format_change_details(event),
                    "last_role": safe_chain_get(event, "details", "as", default=""),
                    "add_info_": safe_chain_get(event, "details", "custom_message", default=""),
                    "ip": safe_chain_get(event, "details", "ip_address", default="")
                }
                if entity_type == 'User':
                    user_record = {
                        "operation_id": str(uuid.uuid4()),
                        "user_id": author_id,
                        "operation_type": operation if operation != 'others' else None,
                        "related_project_id": int(safe_get(event, 'entity_id')) if safe_get(event, 'target_type') == 'Project' else None,
                        "related_group_id": int(safe_get(event, 'entity_id')) if safe_get(event, 'target_type') == 'Group' else None,
                        "operation_details": json.dumps({
                            "target_type": common_fields['target_type'],
                            "target_id": common_fields['target_id'],
                            "pre_post": common_fields['pre_post']
                        }),
                        "operation_metadata": json.dumps({
                            "last_role": common_fields['last_role'],
                            "event": safe_get(event, "event_name"),
                            "add_info_": common_fields['add_info_'],
                            "ip": common_fields['ip']
                        })
                    }
                    user_batch.append(user_record)
                elif entity_type in ['Project', 'Group']:
                    project_group_record = {
                        "change_id": str(uuid.uuid4()),
                        "object_type": entity_type,
                        "object_id": safe_get(event, 'entity_id'),
                        "change_type": operation,
                        "change_time": created_at,
                        "operator_id": author_id,
                        "change_details": json.dumps({
                            "target_type": common_fields['target_type'],
                            "target_id": common_fields['target_id'],
                            "pre_post": common_fields['pre_post'],
                            "last_role": common_fields['last_role']
                        }),
                        "change_metadata": json.dumps({
                            "event": safe_get(event, "event_name"),
                            "add_info_": common_fields['add_info_'],
                            "ip": common_fields['ip']
                        })
                    }
                    project_group_batch.append(project_group_record)
                if len(user_batch) >= BATCH_SIZE:
                    await process_and_insert_data({"fact_user_operations_records": user_batch})
                    user_batch = []
                if len(project_group_batch) >= BATCH_SIZE:
                    await process_and_insert_data({"fact_project_group_changes_records": project_group_batch})
                    project_group_batch = []
            page += 1
            logging.info(f"Fetching audit records, page {page}...")
        except Exception as e:
            logging.error(f"Error fetching audit records: {e}")
    if user_batch:
        await process_and_insert_data({"fact_user_operations_records": user_batch})
    if project_group_batch:
        await process_and_insert_data({"fact_project_group_changes_records": project_group_batch})

async def fetch_system_changes(
    client: httpx.AsyncClient,
    endpoint: str,
    event_type: str,
    entity_type: str,
    project_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    url = f"{GITLAB_URL}/{endpoint}"
    if project_id is not None:
        url = url.format(project_id=project_id)
    try:
        changes = await make_api_request(client, url, HEADERS)
        if isinstance(changes, dict):
            changes = [changes]
        filtered_changes = [change for change in changes if is_within_time(change)]
        return process_system_change_records(filtered_changes, event_type, entity_type)
    except Exception as e:
        logging.error(f"Error fetching system changes for {entity_type} (endpoint: {endpoint}): {e}")
    return []

async def fetch_webhooks(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    endpoint = "hooks"
    try:
        logging.info("Fetching webhooks...")
        return await fetch_system_changes(client, endpoint, "create", "SystemHook")
    except Exception as e:
        logging.error(f"Error fetching webhooks: {e}")
        return []

async def fetch_application_settings_changes(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    endpoint = "application/settings"
    try:
        logging.info("Fetching application settings changes...")
        return await fetch_system_changes(client, endpoint, "update", "ApplicationSetting")
    except Exception as e:
        logging.error(f"Error fetching application settings changes: {e}")
        return []

async def fetch_feature_flag_changes(client: httpx.AsyncClient, project_id: int) -> List[Dict[str, Any]]:
    endpoint = "projects/{project_id}/feature_flags"
    try:
        logging.info("Fetching feature flag changes...")
        return await fetch_system_changes(client, endpoint, "update", "FeatureFlag", project_id)
    except Exception as e:
        logging.error(f"Error fetching feature flag changes for project {project_id}: {e}")
        return []

# ----------------------- Step 1: Fetch total repos from source -----------------------
async def get_project_ids(client: httpx.AsyncClient) -> List[int]:
    project_ids = []
    page = 1
    endpoint = "projects"
    base_url = time_filters(f"{GITLAB_URL}/{endpoint}?order_by=updated_at")
    while True:
        projects_url = f"{base_url}&page={page}&per_page={PER_PAGE}"
        projects = await make_api_request(client, projects_url, HEADERS)
        if not projects or isinstance(projects, dict):
            break
        project_ids.extend([safe_get(project, "id", default=-1) for project in projects])
        page += 1
    return project_ids

# ----------------------- Step 2: Fetch data from datasources -----------------------
async def acquire_users(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    try:
        return await fetch_users(client)
    except Exception as e:
        logging.error(f"Error acquiring users: {e}")
        return []

async def acquire_projects(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    try:
        return await fetch_projects(client)
    except Exception as e:
        logging.error(f"Error acquiring projects: {e}")
        return []

async def acquire_groups(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    try:
        return await fetch_groups(client)
    except Exception as e:
        logging.error(f"Error acquiring groups: {e}")
        return []

async def acquire_code_changes(client: httpx.AsyncClient, project_ids: List[int]) -> None:
    batch = []
    logging.info("Fetching code changes...")
    for project_id in project_ids:
        try:
            sub_changes = await fetch_code_changes(client, project_id)
            batch.extend(sub_changes)
            if len(batch) >= BATCH_SIZE:
                await process_and_insert_data({"fact_code_changes_records": batch})
                batch = []
        except Exception as e:
            logging.error(f"Error acquiring code changes for project {project_id}: {e}")
    if batch:
        await process_and_insert_data({"fact_code_changes_records": batch})

async def acquire_mr_records(client: httpx.AsyncClient, project_ids: List[int]) -> None:
    batch = []
    logging.info("Fetching MR records...")
    for project_id in project_ids:
        try:
            sub_mr_records = await fetch_mr_records(client, project_id)
            batch.extend(sub_mr_records)
            if len(batch) >= BATCH_SIZE:
                await process_and_insert_data({"fact_audit_records_info": batch})
                batch = []
        except Exception as e:
            logging.error(f"Error acquiring MR records for project {project_id}: {e}")
    if batch:
        await process_and_insert_data({"fact_audit_records_info": batch})

async def acquire_cicd_pipeline_activities(client: httpx.AsyncClient, project_ids: List[int]) -> None:
    batch = []
    logging.info("Fetching cicd pipeline activities...")
    for project_id in project_ids:
        try:
            sub_pipeline_records = await fetch_cicd_pipelines(client, project_id)
            batch.extend(sub_pipeline_records)
            if len(batch) >= BATCH_SIZE:
                await process_and_insert_data({"fact_cicd_pipeline_activities_records": batch})
                batch = []
        except Exception as e:
            logging.error(f"Error acquiring CICD pipeline activities for project {project_id}: {e}")
    if batch:
        await process_and_insert_data({"fact_cicd_pipeline_activities_records": batch})

async def acquire_cicd_config_changes(client: httpx.AsyncClient, project_ids: List[int]) -> None:
    batch = []
    logging.info("Fetching cicd config changes...")
    for project_id in project_ids:
        try:
            sub_changes = await track_cicd_config_changes(client, project_id)
            batch.extend(sub_changes)
            if len(batch) >= BATCH_SIZE:
                await process_and_insert_data({"fact_cicd_config_changes_records": batch})
                batch = []
        except Exception as e:
            logging.error(f"Error acquiring CICD config changes for project {project_id}: {e}")
    if batch:
        await process_and_insert_data({"fact_cicd_config_changes_records": batch})

async def acquire_audit_records(client: httpx.AsyncClient) -> Dict[str, List[Dict[str, Any]]]:
    try:
        return await fetch_audit_records(client)
    except Exception as e:
        logging.error(f"Error acquiring audit records: {e}")
        return {}

async def acquire_system_config_changes(client: httpx.AsyncClient, project_ids: List[int]) -> None:
    settings_changes = await fetch_application_settings_changes(client)
    if settings_changes:
        await process_and_insert_data({"fact_system_config_changes_records": settings_changes})
    webhooks = await fetch_webhooks(client)
    if webhooks:
        await process_and_insert_data({"fact_system_config_changes_records": webhooks})
    for project_id in project_ids:
        try:
            sub_flag_changes = await fetch_feature_flag_changes(client, project_id)
            if sub_flag_changes:
                await process_and_insert_data({"fact_system_config_changes_records": sub_flag_changes})
        except Exception as e:
            logging.error(f"Error acquiring feature flag changes for project {project_id}: {e}")

# ----------------------- Step 3: Transform data -----------------------
async def process_and_insert_data(records_dict: Dict[str, List[Dict[str, Any]]]) -> None:
    loop = asyncio.get_running_loop()
    for target_table, records in records_dict.items():
        if target_table not in DATA_SOURCE_CONFIG:
            logging.warning(f"No configuration found for table {target_table}")
            continue
        batch = []
        for record in records:
            transformed_record = {field: convert_to_json(safe_get(record, field)) for field in DATA_SOURCE_CONFIG[target_table]}
            batch.append(transformed_record)
            if len(batch) >= BATCH_SIZE:
                await loop.run_in_executor(thread_pool, sink_to_doris, batch, target_table)
                batch = []
        if batch:
            await loop.run_in_executor(thread_pool, sink_to_doris, batch, target_table)

# ----------------------- Step 4: Sink data to Doris -----------------------
thread_pool = ThreadPoolExecutor()
def sink_to_doris(records: List[Dict[str, Any]], target_table: str) -> None:
    fields = DATA_SOURCE_CONFIG.get(target_table)
    if not fields:
        logging.warning(f"No configuration found for table {target_table}")
        return
    try:
        conn = pymysql.connect(
            host=DORIS_HOST,
            port=DORIS_PORT,
            user=DORIS_USER,
            password=DORIS_PASSWORD,
            database=DORIS_DB
        )
        cursor = conn.cursor()
        columns = ', '.join(fields)
        placeholders = ', '.join(['%s'] * len(fields))
        insert_query = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"
        data = []
        for record in records:
            row = [convert_to_json(safe_get(record, field)) for field in fields]
            data.append(tuple(row))

        cursor.executemany(insert_query, data)
        conn.commit()
        logging.info(f"Successfully inserted {len(records)} rows into {target_table}")
    except pymysql.Error as e:
        logging.error(f"Error inserting into Doris table {target_table}: {e}")
    finally:
        if conn:
            conn.close()

# ----------------------- Main Pipeline -----------------------
async def main():
    logging.info("Initiating the script execution...")
    try:
        async with httpx.AsyncClient() as client:
            logging.info("Fetching project IDs...")
            project_ids = await get_project_ids(client)
            logging.info(f"Total project IDs fetched: {len(project_ids)}")
            tasks = [
                acquire_users(client),
                acquire_projects(client),
                acquire_groups(client),
                acquire_audit_records(client),
                acquire_code_changes(client, project_ids),
                acquire_mr_records(client, project_ids),
                acquire_cicd_pipeline_activities(client, project_ids),
                acquire_cicd_config_changes(client, project_ids),
                acquire_system_config_changes(client, project_ids)
            ]
            logging.info("Initiating concurrent data fetching tasks...")
            await asyncio.gather(*tasks)
            logging.info("Data processing and insertion completed successfully.")
        logging.info("Script execution finished gracefully.")
    except httpx.HTTPStatusError as http_error:
        logging.error(f"HTTP error occurred: {http_error.response.status_code} - {http_error.request.url}")
    except asyncio.TimeoutError:
        logging.error("A task timed out during execution.")
    except Exception as general_error:
        logging.error(f"An unexpected error occurred: {general_error}")

if __name__ == "__main__":
    asyncio.run(main())