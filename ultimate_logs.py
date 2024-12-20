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
            return await response.json()
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
    event_time = record.get("updated_at") or record.get("created_at", "")
    if not event_time:
        return True

    event_datetime = datetime.fromisoformat(event_time.replace('Z', '+00:00'))
    if since_dt and event_datetime < since_dt:
        return False
    if until_dt and event_datetime > until_dt:
        return False
    return True

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
        
        project_ids.extend([project["id"] for project in projects])
        page += 1
    
    return project_ids

# 获取项目名称
async def get_project_name(session, project_id):
    project_url = f"{GITLAB_URL}/projects/{project_id}"
    project_info = await make_api_request(session, project_url, HEADERS)
    return project_info.get("name", "")

# 获取文件的内容
async def get_file_content(session, project_id, commit_sha, file_path):
    content_url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{file_path}?ref={commit_sha}"
    response = await make_api_request(session, content_url, HEADERS)
    if 'content' in response:
        return base64.b64decode(response['content']).decode('utf-8')
    return None

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
    for commit in commits:
        commit_record = {
            "operation": "commit",
            "time": commit.get("committed_date",""),
            "author_id": "",
            "author": commit.get("author_name",""),
            "email": commit.get("author_email",""),
            "message": commit.get("message"),
            "sha": commit.get("id"),
            "project_id": project_id,
            "mr_state": ""
        }
        all_code_changes.append(commit_record)

    # 获取合并请求记录
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    for merge_request in merge_requests:
        author = merge_request.get("author",{})
        merge_record = {
            "operation": "merge_request",
            "time": merge_request.get("updated_at",""),
            "author_id": author.get("id",""),
            "author": author.get("username",""),
            "email": "",
            "message": merge_request.get("title"),
            "sha": "",
            "project_id": project_id,
            "mr_state": merge_request.get("state","")
        }
        all_code_changes.append(merge_record)

    # 获取推送记录
    pulls_url = f"{GITLAB_URL}/projects/{project_id}/events?action=pushed"
    pulls_url = time_filters(pulls_url, 'after', 'before')
    events = await make_api_request(session, pulls_url, HEADERS)
    for event in events:
        author = event.get("author", {})
        p1 = event["push_data"].get("commit_from","")
        p2 = event["push_data"].get("commit_to","")
        pull_record = {
            "operation": "push",
            "time": event.get("created_at",""),
            "author_id": event.get("author_id",""),
            "author": author.get("username", ""),
            "email": "",
            "message": f"{p1}:{p2}",
            "sha": "",
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
            "commenter": note["author"]["username"],
            "content": note["body"],
            "time": note["created_at"]
        } for note in notes
    ]

# 获取审查和合规记录
async def get_mr_review(session, project_id):
    all_mr_records = []
    
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    
    for merge_request in merge_requests:
        author = merge_request.get("author",{})
        assignee = merge_request.get("assignee",{})
        reviewers_ids = [r["id"] for r in merge_request.get("reviewers", [])]
        reviewers = ", ".join([r["username"] for r in merge_request.get("reviewers", [])])
        base_mr_record = {
            "author_id": author.get("id",""),
            "author": author.get("username",""),
            "mr_title": merge_request.get("title",""),
            "mr_description": merge_request.get("description",""),
            "assignee_id": assignee.get("id",""),
            "assignee": assignee.get("username",""),
            "reviewers_ids": reviewers_ids,
            "reviewers": reviewers,
            "project_id": project_id,
            "source_branch": merge_request.get("source_branch",""),
            "target_branch": merge_request.get("target_branch",""),
            "mr_id": merge_request.get("iid"),
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
            if merge_request.get(key):
                record = base_mr_record.copy()
                record.update({
                    "time": merge_request[key],
                    "approval_status": value
                })
                all_mr_records.append(record)

        # 获取MR的注释
        notes = await get_mr_notes(session, project_id, merge_request["iid"])
        for note in notes:
            comment = {
                "commenter": note.get("commenter",""),
                "content": note.get("content",""),
                "time": note.get("time","")
            }
            if all_mr_records:
                all_mr_records[-1]["comments"].append(comment)

    return all_mr_records

# 获取CI/CD管道记录
async def get_cicd_pipelines(session, project_id):
    pipelines_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/pipelines")
    pipelines = await make_api_request(session, pipelines_url, HEADERS)
    all_pipeline_records = []
    for pipeline in pipelines:
        pipeline_id = pipeline.get("id", "")
        branch = pipeline.get("ref", "")
        time = pipeline.get("created_at", "")
        end_time = pipeline.get("updated_at", "")
        duration = pipeline.get("duration", 0)
        triggered_by = pipeline.get("user",{}).get("username","system")
        commit_sha = pipeline.get("sha", "")
        
        # 获取流水线的所有作业
        jobs_url = f"{GITLAB_URL}/projects/{project_id}/pipelines/{pipeline_id}/jobs"
        jobs = await make_api_request(session, jobs_url, HEADERS)
        for job in jobs:
            job_status = job.get("status", "")
            job_name = job.get("name", "")
            job_start_time = job.get("started_at", None)
            job_end_time = job.get("finished_at", "")
            job_duration = job.get("duration", 0)
            environment = job.get("environment", {}).get("name", "")
            
            if not job_start_time:
                job_start_time = time
                logging.debug(f"Job {job_name} (ID: {job['id']}) does not have a started_at field. Status: {job_status}")
            
            pipeline_record = {
                "project_id": project_id,
                "branch": branch,
                "pipeline_id": pipeline_id,
                "stage": job.get("stage", ""),
                "job_name": job.get("name", ""),
                "job_status": job.get("status", ""),
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
    for commit in commits:
        commit_sha = commit.get("id","")
        
        # 获取当前提交的更改文件列表
        changes_url = f"{GITLAB_URL}/projects/{project_id}/repository/commits/{commit_sha}/diff"
        changes = await make_api_request(session, changes_url, HEADERS)
        for change in changes:
            if change['new_path'] == '.gitlab-ci.yml':
                # 获取新旧内容
                old_content = await get_file_content(session, project_id, commit["parent_ids"][0], change['old_path']) if commit["parent_ids"] else None
                new_content = await get_file_content(session, project_id, commit_sha, change['new_path'])
                
                # 比较文件内容
                change_type, diff = compare_yml_files(old_content, new_content)
                
                # 记录变更
                change_record = {
                    "change_type": change_type,
                    "change_content": diff,
                    "time": commit.get("committed_date",""),
                    "author": commit.get("author_name",""),
                    "project_id": project_id,
                    "message": commit.get("message",""),
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

            details = event.get("details", {})
            pre_post = f"{details.get('from', '')}:{details.get('to', '')}" if "change" in details else ""

            record = {
                "author_id": event.get("author_id", "-1"),
                "author": details.get("author_name", ""),
                "entity_id": event.get("entity_id", "-1"),
                "entity_type": event.get("entity_type", ""),
                "time": event.get("created_at","1970-01-01T00:00:00Z"),
                "operation": operation,
                "event": event["event_name"],
                "target_id": details.get("target_id", "-1"),
                "target_type": details.get("target_type", ""),
                "target_name": details.get("target_details", ""),
                "pre_post": pre_post,
                "last_role": details.get("as", ""),
                "add_info_": details.get("custom_message", ""),
                "ip":details.get("ip_address","")
            }
            all_audit_records.append(record)
        
        page += 1
    
    return all_audit_records

# 处理和格式化系统记录
def process_records(records, event, entity_type):
    processed_records = []
    for record in records:
        event_id = record.get("id") or record.get("strategies",{}).get("id","-1")
        event_time = record.get("updated_at") or record.get("created_at", "1970-01-01T00:00:00Z")
        entity_details = record.get("url") or record.get("version", "")
        hook_events = ', '.join([k for k, v in record.items() if k.endswith('_events') and v])
        flag_state = "active" if record.get("active") else "inactive" if "active" in record else ""
        processed_records.append({
            "event_id": event_id,
            "event": event,
            "entity_type": entity_type,
            "time": event_time,
            "entity_name": record.get("name", ""),
            "entity_description": record.get("description", ""),
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
    # 创建并发任务列表
    tasks = [
        get_application_settings_changes(session),
        get_webhooks(session),
        *[get_feature_flag_changes(session, project_id) for project_id in project_ids]  # 为每个项目创建获取功能标志变更的任务
    ]
    # 收集所有结果，合并记录
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_records = []
    for result in results:
        if isinstance(result, Exception):
            logging.error(f"Error fetching system level changes: {result}")
        else:
            all_records.extend(result)
    
    return all_records

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
    tasks = [fetch_func(session, project_id) for project_id in project_ids]
    records_lists = await asyncio.gather(*tasks, return_exceptions=True)
    all_records = [item for sublist in records_lists for item in sublist]

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
    
    directory_name = generate_audit_directory_name()        # 生成目录
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    async with aiohttp.ClientSession() as session:
        try:
            logging.info("Starting the script execution.")

            project_ids = await get_project_ids(session)        # 获取项目id & 初筛
            logging.info(f"Retrieved {len(project_ids)} project IDs.")

            await fetch_and_write_records(session, project_ids, get_code_changes, f"{directory_name}/code_changes.csv", 'code_changes')
            await fetch_and_write_records(session, project_ids, get_mr_review, f"{directory_name}/mr_reviews.csv", 'mr_reviews')
            await fetch_and_write_records(session, project_ids, get_cicd_pipelines, f"{directory_name}/cicd_pipelines.csv", 'cicd_pipelines')
            await fetch_and_write_records(session, project_ids, track_cicd_config_changes, f"{directory_name}/cicd_changes.csv", 'cicd_changes')

            audit_records = await get_audit_records(session)
            write_to_csv(audit_records, f"{directory_name}/audit_records.csv", FIELDNAMES['audit_records'])

            system_level_changes = await get_system_level_changes(session, project_ids)
            write_to_csv(system_level_changes, f"{directory_name}/all_system_changes.csv", FIELDNAMES['all_system_changes'])

            logging.info("Script execution completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred during the execution of main: {e}")

if __name__ == "__main__":
    asyncio.run(main())