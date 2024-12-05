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

async def make_api_request(session, url, headers=None):
    """
    发送GET请求并处理响应
    """
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logging.error(f"请求失败: {e}")
        return []

def time_filters(url, since_param='updated_after', until_param='updated_before'):
    """ 时间过滤 """
    query_params = []
    if SINCE:
        query_params.append(f"{since_param}={SINCE}")
    if UNTIL:
        query_params.append(f"{until_param}={UNTIL}")

    if query_params:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}{'&'.join(query_params)}"
    return url

async def get_project_ids(session):
    """
    获取所有项目的ID，并根据时间段筛选
    """
    project_ids = []
    page = 1
    while True:
        projects_url = time_filters(f"{GITLAB_URL}/projects?page={page}&per_page={PER_PAGE}&order_by=updated_at")
        projects = await make_api_request(session, projects_url, HEADERS)
        
        if not projects:
            break
        
        project_ids.extend([project["id"] for project in projects])
        page += 1
    
    return project_ids

async def get_project_name(session, project_id):
    """ 获取项目名称 """
    project_url = f"{GITLAB_URL}/projects/{project_id}"
    project_info = await make_api_request(session, project_url, HEADERS)
    return project_info.get("name", "Unknown Project")

async def get_file_content(session, project_id, commit_sha, file_path):
    """ 获取指定提交中文件的内容 """
    content_url = f"{GITLAB_URL}/projects/{project_id}/repository/files/{file_path}?ref={commit_sha}"
    response = await make_api_request(session, content_url, HEADERS)
    if 'content' in response:
        return base64.b64decode(response['content']).decode('utf-8')
    return None

def compare_yml_files(old_content, new_content):
    """ 比较两个版本的yml文件并返回差异 """
    old_lines = old_content.splitlines(keepends=True) if old_content else []
    new_lines = new_content.splitlines(keepends=True) if new_content else []
    diff = list(unified_diff(old_lines, new_lines, fromfile='old', tofile='new'))
    change_type = "modified"
    if not old_content and new_content:
        change_type = "added"
    elif old_content and not new_content:
        change_type = "deleted"
    return change_type, ''.join(diff)

async def get_code_changes(session, project_id):
    """
    获取指定项目ID的代码变更记录
    """
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
            "author": commit["author_name"],
            "email": commit.get("author_email", ""),
            "message": commit["message"],
            "sha": commit["id"],
            "project_id": project_id,
            "project_name": project_name
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
    pulls_url = f"{GITLAB_URL}/projects/{project_id}/events?action=pulled"
    pulls_url = time_filters(pulls_url, 'after', 'before')
    events = await make_api_request(session, pulls_url, HEADERS)
    for event in events:
        author = event.get("author", {})
        pull_record = {
            "operation": "pull",
            "time": event["created_at"],
            "author": author.get("username", "Unknown"),
            "email": author.get("email", ""),
            "message": f"拉取了仓库更新, 涉及的提交: {', '.join([commit['id'] for commit in commits])}",
            "sha": "",
            "project_id": project_id,
            "project_name": project_name
        }
        all_code_changes.append(pull_record)

    return all_code_changes

async def get_mr_notes(session, project_id, mr_iid):
    """
    获取指定项目ID和MR IID的所有注释
    """
    notes_url = f"{GITLAB_URL}/projects/{project_id}/merge_requests/{mr_iid}/notes"
    notes = await make_api_request(session, notes_url, HEADERS)
    return [
        {
            "commenter": note["author"]["username"],
            "content": note["body"],
            "time": note["created_at"]
        } for note in notes
    ]

async def get_audit_records(session, project_id):
    """
    获取指定项目ID的审查和合规记录
    """
    all_audit_records = []
    project_name = await get_project_name(session, project_id)
    
        # 获取所有MR
    merge_requests_url = time_filters(f"{GITLAB_URL}/projects/{project_id}/merge_requests")
    merge_requests = await make_api_request(session, merge_requests_url, HEADERS)
    
    for merge_request in merge_requests:
        base_mr_record = {
            "mr_author": merge_request["author"]["username"],
            "mr_title": merge_request["title"],
            "mr_description": merge_request["description"],
            "assignee": merge_request["assignee"]["username"] if merge_request["assignee"] else None,
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
                all_audit_records.append(record)

        # 获取MR的注释
        notes = await get_mr_notes(session, project_id, merge_request["iid"])
        for note in notes:
            comment = {
                "commenter": note["commenter"],
                "content": note["content"],
                "time": note["time"]
            }
            if all_audit_records:
                all_audit_records[-1]["comments"].append(comment)

    return all_audit_records

async def get_cicd_pipelines(session, project_id):
    """
    获取指定项目ID的CI/CD管道记录
    """
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

async def track_cicd_config_changes(session, project_id):
    """ 跟踪指定项目中的CI/CD配置变更 """
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

def write_to_csv(records, filename, fieldnames, sort_key="time"):
    """
    将记录按时间升序追加到CSV文件中，不存在则创建
    """
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
        write_to_csv(code_changes, "code_changes.csv", ["operation", "time", "author", "email", "message", "sha", "project_id", "project_name"], sort_key="time")
        
        # 并发获取审查和合规记录
        tasks = [get_audit_records(session, project_id) for project_id in project_ids]
        audit_records = await asyncio.gather(*tasks)
        audit_records = [item for sublist in audit_records for item in sublist]
        fieldnames = [
            "mr_author", "mr_title", "mr_description", "assignee", "reviewers", "time", 
            "project_id", "project_name", "source_branch", "target_branch", "mr_id", "approval_status", "comments"
        ]
        write_to_csv(audit_records, "audit_records.csv", fieldnames, sort_key="time")
        
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
        fieldnames = ["change_type", "change_content", "time", "author", "email", "project_id", "project_name", "message", "commit_sha"]
        write_to_csv(cicd_changes, "cicd_changes.csv", fieldnames, sort_key="time")

if __name__ == "__main__":
    asyncio.run(main())