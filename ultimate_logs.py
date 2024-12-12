import aiohttp
import asyncio
import csv
from datetime import datetime
from difflib import unified_diff
import base64
import logging
import os

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

since_dt = datetime.fromisoformat(SINCE.replace('Z', '+00:00')) if SINCE else None
until_dt = datetime.fromisoformat(UNTIL.replace('Z', '+00:00')) if UNTIL else None

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
    return project_info.get("name", "Unknown Project")

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

# 获取代码变更记录
async def get_code_changes(session, project_id):
    all_code_changes = []
    project_name = await get_project_name(session, project_id)
    
    # 获取提交记录
    commits_url = f"{GITLAB_URL}/projects/{project_id}/repository/commits"
    commits_url = time_filters(commits_url, 'since', 'until')
    commits = await make_api_request(session, commits_url, HEADERS)
    for commit in commits:
        commit_record = {
            "operation": "commit",
            "time": commit["committed_date"],
            "author_id": "",
            "author": commit["author_name"],
            "email": commit["author_email"],
            "message": commit["message"],
            "sha": commit["id"],
            "project_id": project_id,
            "project_name": project_name,
            "state": ""
        }
        all_code_changes.append(commit_record)

    # 获取合并请求记录
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    for merge_request in merge_requests:
        state = merge_request["state"]
        merge_record = {
            "operation": "merge_request",
            "time": merge_request["updated_at"],
            "author_id": merge_request["author"]["id"],
            "author": merge_request["author"]["username"],
            "email": merge_request["author"].get("email", ""),
            "message": merge_request["title"],
            "sha": "",
            "project_id": project_id,
            "project_name": project_name,
            "state": state
        }
        all_code_changes.append(merge_record)

    # 获取拉取记录
    pulls_url = f"{GITLAB_URL}/projects/{project_id}/events?action=pushed"
    pulls_url = time_filters(pulls_url, 'after', 'before')
    events = await make_api_request(session, pulls_url, HEADERS)
    for event in events:
        author = event.get("author", {})
        pull_record = {
            "operation": "pull",
            "time": event["created_at"],
            "author_id": event["author_id"],
            "author": author.get("username", "Unknown"),
            "email": "",
            "message": f"拉取了仓库更新, 涉及的提交: {', '.join([commit['id'] for commit in commits])}",
            "sha": "",
            "project_id": project_id,
            "project_name": project_name,
            "state": ""
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
    project_name = await get_project_name(session, project_id)
    
        # 获取所有MR
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    
    for merge_request in merge_requests:
        base_mr_record = {
            "author_id": merge_request["author"]["id"],
            "author": merge_request["author"]["username"],
            "mr_title": merge_request["title"],
            "mr_description": merge_request["description"],
            "assignee_id": merge_request["assignee"]["id"] if merge_request["assignee"] else None,
            "assignee": merge_request["assignee"]["username"] if merge_request["assignee"] else None,
            "reviewers_ids": [r["id"] for r in merge_request.get("reviewers", [])],
            "reviewers": ", ".join([r["username"] for r in merge_request.get("reviewers", [])]),
            "project_id": project_id,
            "project_name": project_name,
            "source_branch": merge_request["source_branch"],
            "target_branch": merge_request["target_branch"],
            "mr_id": merge_request["iid"],
            "comments": []  # 初始化评论列表
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
                "commenter": note["commenter"],
                "content": note["content"],
                "time": note["time"]
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
        pipeline_id = pipeline.get("id", "Unknown ID")
        project_name = await get_project_name(session, project_id)
        branch = pipeline.get("ref", "Unknown Branch")
        time = pipeline.get("created_at", "Unknown Start Time")  # 使用 created_at 作为 time
        end_time = pipeline.get("updated_at", "Unknown End Time")
        duration = pipeline.get("duration", 0)  # 默认值为0
        triggered_by = pipeline["user"]["username"] if "user" in pipeline else "system"
        commit_sha = pipeline.get("sha", "Unknown SHA")
        
        # 获取流水线的所有作业
        jobs_url = f"{GITLAB_URL}/projects/{project_id}/pipelines/{pipeline_id}/jobs"
        jobs = await make_api_request(session, jobs_url, HEADERS)
        for job in jobs:
            stage_name = job.get("stage", "Unknown Stage")
            job_name = job.get("name", "Unknown Job Name")
            job_status = job.get("status", "Unknown Status")
            job_start_time = job.get("started_at", None)
            job_end_time = job.get("finished_at", "Unknown End Time")
            job_duration = job.get("duration", 0)  # 默认值为0
            environment = job.get("environment", {}).get("name", "Unknown Environment")
            
            # 如果 job_start_time 不存在，使用 pipeline 的 created_at 或者提供默认值
            if not job_start_time:
                job_start_time = time
                logging.debug(f"Job {job_name} (ID: {job['id']}) does not have a started_at field. Status: {job_status}")
            
            pipeline_record = {
                "project_id": project_id,
                "project_name": project_name,
                "branch": branch,
                "pipeline_id": pipeline_id,
                "stage_name": stage_name,
                "job_name": job_name,
                "job_status": job_status,
                "time": job_start_time,  # 使用 job_start_time 或者 pipeline 的 created_at
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
    project_name = await get_project_name(session, project_id)
    commits_url = f"{GITLAB_URL}/projects/{project_id}/repository/commits"
    commits_url = time_filters(commits_url, 'since', 'until')
    commits = await make_api_request(session, commits_url, HEADERS)
    all_changes = []
    for commit in commits:
        commit_sha = commit["id"]
        
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
                    "time": commit["committed_date"],
                    "author": commit["author_name"],
                    "project_id": project_id,
                    "project_name": project_name,
                    "message": commit["message"],
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
            details = event.get("details", {})
            operation = None

            # 根据存在的键确定operation，并设置event字段
            if "change" in details:
                operation = "change"
            elif "add" in details:
                operation = "add"
            elif "remove" in details:
                operation = "remove"
            else:
                operation = ""

            record = {
                "author_id": event.get("author_id", ""),
                "author": details.get("author_name", ""),
                "entity_id": event.get("entity_id", ""),
                "entity_type": event.get("entity_type", ""),
                "time": event["created_at"],
                "operation": operation,
                "event": event["event_name"],
                "target_id": details.get("target_id", ""),
                "target_type": details.get("target_type", ""),
                "target_name": details.get("target_details", ""),
                "per_details": f"{details.get('from', '')}:{details.get('to', '')}" if "change" in details else "",
                "mem_details": details.get("as", ""),
                "add_message": details.get("custom_message", "")
            }
            all_audit_records.append(record)
        
        page += 1
    
    return all_audit_records

# 处理和格式化系统记录
def process_records(records, event_type, affected_entity):
    processed_records = []
    for record in records:
        event_id = record.get("id", "")
        event_time = record.get("updated_at") or record.get("created_at", "")
        entity_name = record.get("name", "") or record.get("url", "")
        processed_records.append({
            "event_id": event_id,
            "time": event_time,
            "event_type": event_type,
            "affected_entity": affected_entity,
            "entity_name": entity_name,
            "feature_flag_state": record.get('state', ''),
            "webhook_events": ', '.join([k for k, v in record.items() if k.endswith('_events') and v])
        })
    return processed_records

# 获取通用变更记录
async def get_changes(session, endpoint, event_type, affected_entity):
    url = f"{GITLAB_URL}/{endpoint}"
    changes = await make_api_request(session, url, HEADERS)
    if isinstance(changes, dict):  # 如果返回的是单个对象而不是列表，则转换为列表
        changes = [changes]
    filtered_changes = [change for change in changes if is_within_time(change)]
    return process_records(filtered_changes, event_type, affected_entity)

# 获取应用设置变更
async def get_application_settings_changes(session):
    return await get_changes(session, "application/settings", "setting_change", "application_setting")

# 获取功能标志变更
async def get_feature_flag_changes(session):
    return await get_changes(session, "features", "feature_flag_change", "feature_flag")

# 获取Webhooks变更
async def get_webhooks(session):
    return await get_changes(session, "hooks", "webhook_change", "webhook")

# 获取所有系统级别变更记录
async def get_system_level_changes(session):
    async with aiohttp.ClientSession() as session:
        tasks = [
            get_application_settings_changes(session),
            get_feature_flag_changes(session),
            get_webhooks(session)
        ]
        
        results = await asyncio.gather(*tasks)
        all_records = []
        for result in results:
            all_records.extend(result)  # 每个result已经是经过process_records处理过的
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

async def main():
    
    async with aiohttp.ClientSession() as session:
        # 获取项目ID并初步筛选
        project_ids = await get_project_ids(session)
        
        # 并发获取代码变更记录
        tasks = [get_code_changes(session, project_id) for project_id in project_ids]
        code_changes = await asyncio.gather(*tasks)
        code_changes = [item for sublist in code_changes for item in sublist]
        fieldnames = [
            "operation", "time", "author_id", "author", "email",
            "message", "sha", "project_id", "project_name", "state"
        ]
        write_to_csv(code_changes, "code_changes.csv", fieldnames, sort_key="time")
        
        # 并发获取审查和合规记录
        tasks = [get_mr_review(session, project_id) for project_id in project_ids]
        mr_reviews = await asyncio.gather(*tasks)
        mr_reviews = [item for sublist in mr_reviews for item in sublist]
        fieldnames = [
            "author_id", "author", "mr_title", "mr_description", "assignee_id", "assignee", 
            "reviewers_ids", "reviewers", "time", "project_id", "project_name", "source_branch", 
            "target_branch", "mr_id", "approval_status", "comments"
        ]
        write_to_csv(mr_reviews, "mr_reviews.csv", fieldnames, sort_key="time")
        
        # 并发获取CI/CD管道记录
        tasks = [get_cicd_pipelines(session, project_id) for project_id in project_ids]
        cicd_pipelines = await asyncio.gather(*tasks)
        cicd_pipelines = [item for sublist in cicd_pipelines for item in sublist]
        fieldnames = [
            "project_id", "project_name", "branch", "pipeline_id", "stage_name", 
            "job_name", "job_status", "time", "end_time", "duration", 
            "triggered_by", "environment", "commit_sha"
        ]
        write_to_csv(cicd_pipelines, "cicd_pipelines.csv", fieldnames, sort_key="time")
        
        # 并发获取CI/CD配置变更记录
        tasks = [track_cicd_config_changes(session, project_id) for project_id in project_ids]
        cicd_changes = await asyncio.gather(*tasks)
        cicd_changes = [item for sublist in cicd_changes for item in sublist]
        fieldnames = [
            "change_type", "change_content", "time", "author",
            "project_id", "project_name", "message", "commit_sha"
        ]
        write_to_csv(cicd_changes, "cicd_changes.csv", fieldnames, sort_key="time")

        # 获取审计记录
        audit_records = await get_audit_records(session)
        fieldnames = [
            "author_id", "author", "entity_id", "entity_type", "time", "operation", "event", 
            "target_id", "target_type", "target_name", "per_details", "mem_details", "add_message"
        ]
        write_to_csv(audit_records, "audit_records.csv", fieldnames, sort_key="time")

        # 获取所有系统级别变更记录
        all_changes = await get_system_level_changes(session)
        fieldnames = [
            "event_id", "time", "event_type", "affected_entity",
            "entity_name", "feature_flag_state", "webhook_events"
        ]
        write_to_csv(all_changes, "all_system_changes.csv", fieldnames, sort_key="time")

if __name__ == "__main__":
    asyncio.run(main())