import aiohttp
import asyncio
import csv
from datetime import datetime
from difflib import unified_diff
import base64
import logging
import os
import pytz
import re

# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# 环境变量：GitLab URL, Token
GITLAB_URL = os.getenv('GITLAB_URL')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')

if not GITLAB_URL:
    logging.error("GITLAB_URL environment variable is not set. Please set it and try again.")
    exit(1)

if not ACCESS_TOKEN:
    logging.error("ACCESS_TOKEN environment variable is not set. Please set it and try again.")
    exit(1)

# 常量
HEADERS = {"PRIVATE-TOKEN": ACCESS_TOKEN}
PER_PAGE = 100
SINCE = None  # 开始时间（例如："2024-11-27T00:00:00Z"）
UNTIL = None  # 结束时间

since_dt = datetime.fromisoformat(SINCE.replace('Z', '+00:00')) if SINCE else None
until_dt = datetime.fromisoformat(UNTIL.replace('Z', '+00:00')) if UNTIL else None


FIELDNAMES = {
    'code_changes':
            ["operation", "time", "author_id", "author", "email", "message", "sha", "project_id", "mr_state"],
    'mr_reviews':
            ["author_id", "author", "mr_title", "mr_description", "assignee_id", "assignee", 
            "reviewers_ids", "reviewers", "time", "project_id", "source_branch", "target_branch", 
            "mr_id", "approval_status", "comments"],
    'cicd_pipelines':
            ["project_id", "branch", "pipeline_id", "stage", "job_name", "job_status", "time", 
            "end_time", "duration", "triggered_by", "environment", "commit_sha"],
    'cicd_changes':
            ["change_type", "change_content", "time", "author", "project_id", "message", "commit_sha"],
    'audit_records':
            ["author_id", "author", "entity_id", "entity_type", "time", "operation", "event", 
            "target_id", "target_type", "target_name", "pre_post", "last_role", "add_info_", "ip"],
    'all_system_changes':
            ["event_id", "event", "entity_type", "time", "entity_name", "entity_description", 
            "entity_details", "hook_events", "flag_state"]
}

DIMFIELDS = {
    'Users': ["id", "username", "email", "state", "is_admin", "created_at", "last_sign_in_at", "last_activity_on"],
    'Projects': ["id", "name", "description", "tag_list", "metadata"],
    'Groups': ["id", "name", "description", "members", "visibility", "created_at", "path"]
}

# 预编译关键词到操作性质的映射
operation_patterns = {
    "create": [re.compile(r"\b(add|created|added|new|inserted|registered|generate)\b", re.IGNORECASE),
               re.compile(r"\b(create|generation|generated)\b", re.IGNORECASE)],
    "update": [re.compile(r"\b(change|updated|modify|modified|edit|edited|alter|altered)\b", re.IGNORECASE),
               re.compile(r"\b(update|revision|revised)\b", re.IGNORECASE)],
    "delete": [re.compile(r"\b(remove|destroyed|deleted|eliminated|dropped|unregister)\b", re.IGNORECASE),
               re.compile(r"\b(delete|deletion|deregister)\b", re.IGNORECASE)]
}


# 发送GET请求并处理响应
async def make_api_request(session, url, headers=None):
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            try:
                return await response.json()
            except aiohttp.ContentTypeError:
                logging.error(f"Failed to parse JSON response from {url}: {await response.text()}")
                return []
    except aiohttp.ClientError as e:
        logging.error(f"请求失败: {e}")
        return []

# URL - 时间过滤器
def time_filters(url, since_param='updated_after', until_param='updated_before'):
    query_params = []
    if SINCE:
        query_params.append(f"{since_param}={SINCE}")
    if UNTIL:
        query_params.append(f"{until_param}={UNTIL}")

    if query_params:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}{'&'.join(query_params)}"
    return url

# 记录 - 时间过滤器
def is_within_time(record):
    event_time = safe_get(record,"updated_at") or safe_get(record,"created_at",default="")
    if not event_time:
        return True

    event_datetime = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
    if since_dt and event_datetime < since_dt:
        return False
    if until_dt and event_datetime > until_dt:
        return False
    return True

# 安全获取字典键值
def safe_get(dictionary, key, default=""):
    return dictionary.get(key, default)

# 安全获取链式字典键值
def safe_chain_get(dictionary, *keys, default=None):
    result = dictionary
    for key in keys:
        if result is None:
            return default
        result = result.get(key)
    return result if result is not None else default
    

# 获取所有项目的ID
async def get_project_ids(session):
    project_ids = []
    page = 1
    base_url = time_filters(f"{GITLAB_URL}/projects")   # 根据时间段初筛
    while True:
        projects_url = f"{base_url}{'&' if '?' in base_url else '?'}page={page}&per_page={PER_PAGE}&order_by=updated_at"
        projects = await make_api_request(session, projects_url, HEADERS)
        
        if not projects:
            break
        
        project_ids.extend([safe_get(project,"id",default=-1) for project in projects])
        page += 1
    return project_ids

# 获取文件的内容
async def get_file_content(session, project_id, commit_sha, file_path):
    content_url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{file_path}?ref={commit_sha}"
    response = await make_api_request(session, content_url, HEADERS)
    if 'content' in response:
        return base64.b64decode(response['content']).decode('utf-8')
    return ""

# 比较两个版本的yml文件并返回差异
def compare_yml_files(old_content, new_content):
    old_lines = old_content.splitlines(keepends=True) if old_content else []
    new_lines = new_content.splitlines(keepends=True) if new_content else []
    diff = list(unified_diff(old_lines, new_lines, fromfile='old', tofile='new'))
    change_type = "modified"
    if not old_content and new_content:
        change_type = "added"
    elif old_content and not new_content:
        change_type = "deleted"
    return change_type, ''.join(diff)

# 检查给定值中是否包含匹配的操作性质关键字
def check_keywords(value, patterns=operation_patterns):
    if not value:
        return None
    value_str = str(value).lower()
    for op_type, compiled_patterns in patterns.items():
        if any(pattern.search(value_str) for pattern in compiled_patterns):
            return op_type
    return None

# 返回记录的操作性质
def classify_event_operation(event, keys, nested_keys=None):
    nested_keys = nested_keys or []

    # 第一步：检查嵌套键中的键名
    if nested_keys:
        nested_dict = event
        try:
            for key in nested_keys:
                nested_dict = nested_dict[key]
            if isinstance(nested_dict, dict):
                for key in nested_dict.keys():
                    if (found_op := check_keywords(key)):
                        return found_op
        except (KeyError, TypeError):
            pass

    # 第二步：检查指定键的值
    for key in keys:
        if (found_op := check_keywords(event.get(key))):
            return found_op

    # 第三步：再次检查嵌套键中的值
    if nested_keys:
        try:
            nested_value = event
            for key in nested_keys:
                nested_value = nested_value[key]
            if isinstance(nested_value, dict):
                for sub_value in nested_value.values():
                    if (found_op := check_keywords(sub_value)):
                        return found_op
            elif isinstance(nested_value, list):
                for item in nested_value:
                    if (found_op := check_keywords(item)):
                        return found_op
            elif nested_value is not None and (found_op := check_keywords(nested_value)):
                return found_op
        except (KeyError, TypeError):
            pass

    return "others"

# 获取代码变更记录
async def get_code_changes(session, project_id):
    all_code_changes = []

    # 获取提交记录
    commits_url = f"{GITLAB_URL}/projects/{project_id}/repository/commits"
    commits_url = time_filters(commits_url, 'since', 'until')
    commits = await make_api_request(session, commits_url, HEADERS)
    if commits:
        for commit in commits:
            safe_get
            commit_record = {
                "operation": "commit",
                "time": safe_get(commit,"committed_date"),
                "author_id": "",
                "author": safe_get(commit,"author_name"),
                "email": safe_get(commit,"author_email"),
                "message": safe_get(commit,"message"),
                "sha": safe_get(commit,"id"),
                "project_id": project_id,
                "mr_state": ""
            }
            all_code_changes.append(commit_record)

    # 获取合并请求记录
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    if merge_requests:
        for merge_request in merge_requests:
            author_id = safe_chain_get(merge_request,"author","id",default=-1)
            author = safe_chain_get(merge_request,"author","username",default="")
            merge_record = {
                "operation": "merge_request",
                "time": safe_get(merge_request,"updated_at"),
                "author_id": author_id,
                "author": author,
                "email": "",
                "message": safe_get(merge_request,"title"),
                "sha": "",
                "project_id": project_id,
                "mr_state": safe_get(merge_request,"state")
            }
            all_code_changes.append(merge_record)

    # 获取推送记录
    pulls_url = f"{GITLAB_URL}/projects/{project_id}/events?action=pushed"
    pulls_url = time_filters(pulls_url, 'after', 'before')
    events = await make_api_request(session, pulls_url, HEADERS)
    if events:
        for event in events:
            author = safe_chain_get(event,"author","username",default="")
            p1 = safe_chain_get(event,"push_data","commit_from",default="")
            p2 = safe_chain_get(event,"push_data","commit_to",default="")
            pull_record = {
                "operation": "push",
                "time": safe_get(event,"created_at"),
                "author_id": safe_get(event,"author_id"),
                "author": author,
                "email": "",
                "message": f"commit from:{p1}",
                "sha": p2,
                "project_id": project_id,
                "mr_state": ""
            }
            all_code_changes.append(pull_record)

    return all_code_changes

# 获取指定项目ID和MR IID的所有注释
async def get_mr_notes(session, project_id, mr_iid):
    notes_url = f"{GITLAB_URL}/projects/{project_id}/merge_requests/{mr_iid}/notes"
    notes = await make_api_request(session, notes_url, HEADERS)
    return [
        {
            "commenter":  safe_chain_get(note,"author","username",default=""),
            "content": safe_get(note,"body"),
            "time": safe_get(note,"created_at")
        } for note in notes
    ]

# 获取审查和合规记录
async def get_mr_review(session, project_id):
    all_mr_records = []
    
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    
    if not merge_requests:
        return all_mr_records
    for merge_request in merge_requests:
        author_id = safe_chain_get(merge_request,"author","id",default=-1)
        author = safe_chain_get(merge_request,"author","username",default="")
        assignee_id = safe_chain_get(merge_request,"assignee","id",default=-1)
        assignee = safe_chain_get(merge_request,"assignee","username",default="")
        reviewers_ids = [r["id"] for r in safe_get(merge_request,"reviewers",default=[])]
        reviewers = ", ".join([r["username"] for r in safe_get(merge_request,"reviewers",default=[])])
        mr_id = safe_get(merge_request,"iid")
        base_mr_record = {
            "author_id": author_id,
            "author": author,
            "mr_title": safe_get(merge_request,"title"),
            "mr_description": safe_get(merge_request,"description"),
            "assignee_id": assignee_id,
            "assignee": assignee,
            "reviewers_ids": reviewers_ids,
            "reviewers": reviewers,
            "project_id": project_id,
            "source_branch": safe_get(merge_request,"source_branch"),
            "target_branch": safe_get(merge_request,"target_branch"),
            "mr_id": mr_id,
            "comments": []
        }

        # 根据不同的状态添加记录
        status_updates = [
            ("created_at", "opened"),
            ("updated_at", "approved"),
            ("merged_at", "merged"),
            ("closed_at", "closed")
        ]
        
        for key, value in status_updates:
            if safe_get(merge_request,key,default=None):
                record = base_mr_record.copy()
                record.update({
                    "time": merge_request[key],
                    "approval_status": value
                })
                all_mr_records.append(record)

        # 获取MR的注释
        notes = await get_mr_notes(session, project_id, mr_id)
        for note in notes:
            comment = {
                "commenter": safe_get(note,"commenter"),
                "content": safe_get(note,"content"),
                "time": safe_get(note,"time")
            }
            if all_mr_records:
                all_mr_records[-1]["comments"].append(comment)

    return all_mr_records

# 获取CI/CD管道记录
async def get_cicd_pipelines(session, project_id):
    pipelines_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/pipelines")
    pipelines = await make_api_request(session, pipelines_url, HEADERS)
    all_pipeline_records = []
    if not pipelines:
        return all_pipeline_records
    for pipeline in pipelines:
        pipeline_id = safe_get(pipeline,"id")
        branch = safe_get(pipeline,"ref")
        time = safe_get(pipeline,"created_at")
        end_time =  safe_get(pipeline,"updated_at")
        duration = safe_get(pipeline,"duration",default=0)
        commit_sha = safe_get(pipeline,"sha")
        
        # 获取流水线的所有作业
        jobs_url = f"{GITLAB_URL}/projects/{project_id}/pipelines/{pipeline_id}/jobs"
        jobs = await make_api_request(session, jobs_url, HEADERS)
        if not jobs:
            continue
        for job in jobs:
            job_start_time = safe_get(job,"started_at",default=None)
            job_end_time = safe_get(job,"finished_at")
            job_duration = safe_get(job,"duration",default=0)
            triggered_by = safe_chain_get(job,"user","username",default="system")
            environment = safe_chain_get(job,"environment","name",default="")
            
            if not job_start_time:
                job_start_time = time
            
            pipeline_record = {
                "project_id": project_id,
                "branch": branch,
                "pipeline_id": pipeline_id,
                "stage": safe_get(job,"stage"),
                "job_name": safe_get(job,"name"),
                "job_status": safe_get(job,"status"),
                "time": job_start_time,
                "end_time": job_end_time or end_time,
                "duration": job_duration or duration,
                "triggered_by": triggered_by,
                "environment": environment,
                "commit_sha": commit_sha
            }
            all_pipeline_records.append(pipeline_record)
    
    return all_pipeline_records

# 跟踪指定项目中的CI/CD配置变更
async def track_cicd_config_changes(session, project_id):
    commits_url = f"{GITLAB_URL}/projects/{project_id}/repository/commits"
    commits_url = time_filters(commits_url, 'since', 'until')
    commits = await make_api_request(session, commits_url, HEADERS)
    all_changes = []
    if not commits:
        return all_changes
    for commit in commits:
        commit_sha = safe_get(commit,"id")
        
        # 获取当前提交的更改文件列表
        changes_url = f"{GITLAB_URL}/projects/{project_id}/repository/commits/{commit_sha}/diff"
        changes = await make_api_request(session, changes_url, HEADERS)
        if not changes:
            continue
        for change in changes:
            if change['new_path'] == '.gitlab-ci.yml':

                # 获取新旧内容
                old_content = (await get_file_content(session, project_id, commit["parent_ids"][0], 
                                                     safe_get(change,"old_path"))) if "parent_ids" in commit and commit["parent_ids"] else ""
                new_content = await get_file_content(session, project_id, commit_sha, safe_get(change,"new_path"))
                
                # 比较文件内容
                change_type, diff = compare_yml_files(old_content, new_content)
                
                # 记录变更
                change_record = {
                    "change_type": change_type,
                    "change_content": diff,
                    "time": safe_get(commit,"committed_date"),
                    "author": safe_get(commit,"author_name"),
                    "project_id": project_id,
                    "message": safe_get(commit,"message"),
                    "commit_sha": commit_sha
                }
                all_changes.append(change_record)
    
    return all_changes

# 获取所有审计记录（用户/项目/组）
async def get_audit_records(session):
    all_audit_records = []
    base_url = f"{GITLAB_URL}/audit_events"
    base_url = time_filters(base_url, 'created_after', 'created_before')
    page = 1
    logging.info("Fetching audit records, page 1...")
    while True:
        events_url = f"{base_url}{'&' if '?' in base_url else '?'}page={page}&per_page={PER_PAGE}"
        audit_events = await make_api_request(session, events_url, HEADERS)
        if not audit_events or isinstance(audit_events, dict):
            break
        for event in audit_events:
            # 确定要检查的键和嵌套键
            keys = ['event_name']
            nested_keys = ['details']
            operation = classify_event_operation(event, keys, nested_keys)
            author = safe_chain_get(event,"details","author_name",default="")
            target_id = safe_chain_get(event,"details","target_id",default=-1)
            target_type = safe_chain_get(event,"details","target_type",default="")
            target_name = safe_chain_get(event,"details","target_details",default="")
            last_role = safe_chain_get(event,"details","as",default="")
            add_info_ = safe_chain_get(event,"details","custom_message",default="")
            ip = safe_chain_get(event,"details","ip_address",default="")
            change = safe_chain_get(event,"details","change",default=None)
            if change is not None:
                from_ = safe_chain_get(event,"details","from",default="")
                to_ = safe_chain_get(event,"details","to",default="")
                pre_post = f"{from_}:{to_}"
            else:
                pre_post = ""
            record = {
                "author_id": safe_get(event,"author_id",default=-1),
                "author": author,
                "entity_id": safe_get(event,"entity_id",default=-1),
                "entity_type": safe_get(event,"entity_type"),
                "time": safe_get(event,"created_at",default="1970-01-01T00:00:00Z"),
                "operation": operation,
                "event": safe_get(event,"event_name"),
                "target_id": target_id,
                "target_type": target_type,
                "target_name": target_name,
                "pre_post": pre_post,
                "last_role": last_role,
                "add_info_": add_info_,
                "ip": ip
            }
            all_audit_records.append(record)
        page += 1
        logging.info(f"Fetching audit records, page {page}...")
    return all_audit_records

# 处理和格式化系统记录
def process_records(records, event, entity_type):
    processed_records = []
    for record in records:
        event_id = safe_get(record,"id",default=None)
        if not event_id:
            try:
                usr_id = record["strategies"][0].get("id",-1)
                event_id = usr_id
            except (KeyError, IndexError):
                event_id = -1

        event_time = safe_get(record,"updated_at",default=None) or safe_get(record,"created_at",default="1970-01-01T00:00:00Z")
        entity_details = safe_get(record,"url",default=None) or safe_get(record,"version")
        hook_events = ', '.join([k for k, v in record.items() if k.endswith('_events') and v])
        safe_get(record,"active",default=None)
        flag_state = "active" if safe_get(record,"active") else "inactive" if safe_get(record,"active") else ""
        processed_records.append({
            "event_id": event_id,
            "event": event,
            "entity_type": entity_type,
            "time": event_time,
            "entity_name": safe_get(record,"name"),
            "entity_description": safe_get(record,"description"),
            "entity_details": entity_details,
            "hook_events": hook_events,
            "flag_state": flag_state
        })
    return processed_records

# 获取通用变更记录  (本地筛选)
async def get_changes(session, endpoint, event, entity_type):
    url = f"{GITLAB_URL}/{endpoint}"
    changes = await make_api_request(session, url, HEADERS)
    if isinstance(changes, dict):  # 如果返回的是单个对象而不是列表，则转换为列表
        changes = [changes]
    filtered_changes = [change for change in changes if is_within_time(change)]
    return process_records(filtered_changes, event, entity_type)

# 获取应用设置变更
async def get_application_settings_changes(session):
    return await get_changes(session, "application/settings", "update", "ApplicationSetting")

# 获取功能标志变更
async def get_feature_flag_changes(session, project_id):
    enpoint = f"projects/{project_id}/feature_flags"
    return await get_changes(session, enpoint, "valid_update", "FeatureFlag")

# 获取系统钩子创建
async def get_webhooks(session):
    return await get_changes(session, "hooks", "create", "SystemHook")

# 获取所有系统级别变更记录
async def get_system_level_changes(session, project_ids):
    tasks = []
    logging.info("Fetching application settings changes...")
    tasks.append(get_application_settings_changes(session))
    logging.info("Fetching webhooks...")
    tasks.append(get_webhooks(session))
    logging.info("Fetching feature flag changes...")
    for project_id in project_ids:
        tasks.append(get_feature_flag_changes(session, project_id))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_records = []
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Error fetching system level changes: {result}")
        else:
            all_records.extend(result)
    return all_records

# 用户维度
async def acquire_users(session):
    all_users_info = []
    base_url = f"{GITLAB_URL}/users?order_by=updated_at"
    page = 1
    logging.info("Acquiring Users Info, page 1...")
    while True:
        usr_url = f"{base_url}&page={page}&per_page={PER_PAGE}"
        users_info = await make_api_request(session, usr_url, HEADERS)
        if not users_info or isinstance(users_info, dict):
            break
        for usr in users_info:
            record = {
                "id": safe_get(usr,"id",default=-1),
                "username": safe_get(usr,"username"),
                "email": safe_get(usr,"email"),
                "state": safe_get(usr,"state"),
                "is_admin": safe_get(usr,"is_admin"),
                "created_at": safe_get(usr,"created_at"),
                "last_sign_in_at": safe_get(usr,"current_sign_in_at"),
                "last_activity_on": safe_get(usr,"last_activity_on")
            }
            all_users_info.append(record)
        page += 1
        logging.info(f"Acquiring Users Info, page {page}...")
    return all_users_info

# 项目维度
async def acquire_projects(session, project_ids):
    all_projects = []
    project_ids_set = set(project_ids)
    proj_url = f"{GITLAB_URL}/projects?simple=true"
    logging.info("Acquiring Projects Info...")
    projects = await make_api_request(session, proj_url, HEADERS)
    if projects:
        for proj in projects:
            proj_id = safe_get(proj, "id")
            if proj_id and proj_id in project_ids_set:
                record = {
                    "id": proj_id,
                    "name": safe_get(proj, "name"),
                    "description": safe_get(proj, "description"),
                    "tag_list": safe_get(proj, "tag_list"),
                    "metadata": proj
                }
                all_projects.append(record)
    return all_projects

# 组维度
async def acquire_groups(session):
    all_groups = []
    base_url = f"{GITLAB_URL}/groups"
    page = 1
    logging.info("Acquiring Groups Info, page 1...")
    while True:
        group_url = f"{base_url}?page={page}&per_page={PER_PAGE}"
        groups_info = await make_api_request(session, group_url, HEADERS)
        if not groups_info or isinstance(groups_info, dict):
            break
        for grp in groups_info:
            record = {
                "id": safe_get(grp, "id", default=-1),
                "name": safe_get(grp, "name"),
                "description": safe_get(grp, "description"),
                "members": [],
                "visibility": safe_get(grp, "visibility"),
                "created_at": safe_get(grp, "created_at"),
                "path": safe_get(grp, "path")
            }
            # 获取组内成员
            group_id = record["id"]
            if group_id!= -1:
                mem_url = f"{base_url}/{group_id}/members"
                members_info = await make_api_request(session, mem_url, HEADERS)
                if members_info:
                    for member in members_info:
                        member_id = safe_get(member, "id")
                        if member_id:
                            record["members"].append(member_id)
            all_groups.append(record)
        page += 1
        logging.info(f"Acquiring Groups Info, page {page}...")
    return all_groups

# 将记录按时间升序追加到CSV文件
def write_to_csv(records, filename, fieldnames, sort_key="time"):
    if sort_key:
        sorted_records = sorted(records, key=lambda x: x.get(sort_key, ""))
    else:
        sorted_records = records

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in sorted_records:
            # 处理 comments 字段
            if "comments" in record:
                comments = "\n".join([f"{c['commenter']}: {c['content']} ({c['time']})" for c in record["comments"]])
                record["comments"] = comments
            writer.writerow({k: record.get(k, "") for k in fieldnames})

# 并发获取记录并写入CSV文件
async def fetch_and_write_records(session, project_ids, fetch_func, filename, fieldnames_key, sort_key="time"):
    logging.info(f"Fetching {fieldnames_key} records...")
    tasks = [fetch_func(session, project_id) for project_id in project_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_records = []
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Error fetching {fieldnames_key} records: {result}")
        else:
            all_records.extend(result)
    write_to_csv(all_records, filename, FIELDNAMES[fieldnames_key], sort_key)

# 动态生成目标目录
def generate_audit_directory_name():
    # 北京时间
    global since_dt, until_dt
    start_time = since_dt
    end_time = until_dt
    if start_time:
        start_time = start_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Shanghai'))
    if end_time:
        end_time = end_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Shanghai'))
    else:
        beijing_tz = pytz.timezone('Asia/Shanghai')
        end_time = datetime.now(beijing_tz)
    # 判断时间精度
    if start_time and end_time:
        if (end_time - start_time).days <= 7:
            time_precision = 'D'
        elif (end_time - start_time).days <= 30:
            time_precision = 'W'
        elif (end_time - start_time).days <= 90:
            time_precision = 'M'
        else:
            time_precision = 'Q'
        return f"Audit_Output_{time_precision}_{start_time.strftime('%Y%m%d')}-{end_time.strftime('%Y%m%d')}"
    else:
        # 缺省则返回：19700101-当前时间
        current_time_str = end_time.strftime('%Y%m%d')
        return f'Audit_Output_All_19700101-{current_time_str}'

async def main():
    directory_name = generate_audit_directory_name()
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    async with aiohttp.ClientSession() as session:
        try:
            logging.info("Starting the script execution.")
            project_ids = await get_project_ids(session)
            logging.info(f"Retrieved {len(project_ids)} project IDs.")
            await fetch_and_write_records(session, project_ids, get_code_changes, f"{directory_name}/code_changes.csv", 'code_changes')
            await fetch_and_write_records(session, project_ids, get_mr_review, f"{directory_name}/mr_reviews.csv", 'mr_reviews')
            await fetch_and_write_records(session, project_ids, get_cicd_pipelines, f"{directory_name}/cicd_pipelines.csv", 'cicd_pipelines')
            await fetch_and_write_records(session, project_ids, track_cicd_config_changes, f"{directory_name}/cicd_changes.csv", 'cicd_changes')
            audit_records = await get_audit_records(session)
            write_to_csv(audit_records, f"{directory_name}/audit_records.csv", FIELDNAMES['audit_records'])
            system_level_changes = await get_system_level_changes(session, project_ids)
            write_to_csv(system_level_changes, f"{directory_name}/all_system_changes.csv", FIELDNAMES['all_system_changes'])
            dim_users = await acquire_users(session)
            write_to_csv(dim_users,f"{directory_name}/dim_users.csv",DIMFIELDS['Users'],sort_key='id')
            dim_projects = await acquire_projects(session,project_ids)
            write_to_csv(dim_projects,f"{directory_name}/dim_projects.csv",DIMFIELDS['Projects'],sort_key='id')
            dim_groups = await acquire_groups(session)
            write_to_csv(dim_groups,f"{directory_name}/dim_groups.csv",DIMFIELDS['Groups'],sort_key='id')
            logging.info("Script execution completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred during the execution of main: {e}")

if __name__ == "__main__":
    asyncio.run(main())