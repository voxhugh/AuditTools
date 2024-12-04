import aiohttp
import asyncio
import csv
from datetime import datetime, timezone
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

# 常量定义
HEADERS = {"PRIVATE-TOKEN": ACCESS_TOKEN}
PER_PAGE = 100
START_TIME = None  # 起始时间
END_TIME = None    # 结束时间

async def make_api_request(session, url):
    """
    发送GET请求并处理响应
    """
    try:
        async with session.get(url, headers=HEADERS) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        logging.error(f"请求失败: {e}")
        return []

def time_filters(url, order_by=None):
    """ 时间过滤 """
    query_params = []
    if START_TIME:
        query_params.append(f"created_after={START_TIME.isoformat()}")
    if END_TIME:
        query_params.append(f"created_before={END_TIME.isoformat()}")
    if order_by:
        query_params.append(f"order_by={order_by}")
        
    if query_params:
        separator = '&' if '?' in url else '?'
        return f"{url}{separator}{'&'.join(query_params)}"
    return url

async def get_audit_events(session):
    """获取审计事件"""
    events = []
    page = 1
    while True:
        time_filters(f"{GITLAB_URL}/audit_events?page={page}&per_page={PER_PAGE}")
        batch = await make_api_request(session, audit_url)
        if not batch:
            break
        events.extend(batch)
        page += 1
    return events

def parse_event(event):
    """解析审计事件为所需格式"""
    return {
        "object_type": event["entity_type"],
        "time": event["created_at"],
        "event_type": event["details"]["action_name"],
        "event_object": f"{event['target_title']} (ID: {event['target_id']})",
        "event_details": event["details"].get("message", ""),
        "actor_username_and_id": f"{event['author_name']} (ID: {event['author_id']})",
        "status": "success" if event["status"] == "success" else "failure"
    }

async def get_projects(session):
    """
    获取所有项目的ID，并根据时间段筛选（通过API请求参数实现）
    """
    project_ids = []
    page = 1
    while True:
        projects_url = time_filters(f"{GITLAB_URL}/projects?page={page}&per_page={PER_PAGE}",order_by='id')
        projects = await make_api_request(session, projects_url, HEADERS)
        
        if not projects:
            break
        
        project_ids.extend([project["id"] for project in projects])
        page += 1
    
    return project_ids

async def get_webhooks(session, project_id):
    """获取项目的Webhooks"""
    webhooks_url = f"{GITLAB_URL}/projects/{project_id}/hooks"
    return await make_api_request(session, webhooks_url)

async def get_project_access_tokens(session, project_id):
    """获取项目的访问令牌"""
    tokens_url = f"{GITLAB_URL}/projects/{project_id}/access_tokens"
    return await make_api_request(session, tokens_url)

def parse_webhook(webhook):
    """解析Webhook为所需格式"""
    webhook_events = [event for event, enabled in webhook.items() if isinstance(enabled, bool) and enabled]
    return {
        "change_object": "Webhook",
        "time": webhook["created_at"],
        "change_type": "create" if webhook["id"] else "update",
        "webhook_events": ", ".join(webhook_events),
        "api_endpoint": webhook["url"],
        "api_permissions": "",
        "ip_restrictions": webhook["ip_url_whitelist"] or "",
        "rate_limit": str(webhook["max_content_length"]) + " bytes"
    }

def parse_access_token(token):
    """解析访问令牌为所需格式"""
    return {
        "change_object": "API Access Token",
        "time": token["created_at"],
        "change_type": "create",
        "webhook_events": "",
        "api_endpoint": "",
        "api_permissions": token["scopes"],
        "ip_restrictions": "",
        "rate_limit": ""
    }

async def get_system_config_changes(session):
    # 获取所有项目，并根据last_activity_at过滤
    projects = await get_projects(session)
    changes = []
    for project in projects:
        project_id = project["id"]
        # 获取并解析Webhooks
        webhooks = await get_webhooks(session, project_id)
        for webhook in webhooks:
            parsed_webhook = parse_webhook(webhook)
            changes.append(parsed_webhook)
        # 获取并解析访问令牌
        access_tokens = await get_project_access_tokens(session, project_id)
        for token in access_tokens:
            parsed_token = parse_access_token(token)
            changes.append(parsed_token)
    return changes

def write_to_csv(records, filename, fieldnames, sort_key="time"):
    """将记录按时间升序追加到CSV文件中，不存在则创建"""
    sorted_records = sorted(records, key=lambda x: x[sort_key])
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in sorted_records:
            writer.writerow(record)

async def main():

    async with aiohttp.ClientSession() as session:
        # 获取审计事件
        audit_events = await get_audit_events(session)
        parsed_events = [parse_event(event) for event in audit_events]
        fieldnames = ["object_type", "time", "event_type", "event_object", "event_details", "actor_username_and_id", "status"]
        write_to_csv(parsed_events, "audit_log.csv", fieldnames)
        
        # 获取系统配置变更
        system_config_changes = await get_system_config_changes(session)
        system_config_fieldnames = [
            "change_object", "time", "change_type", "webhook_events", "api_endpoint",
            "api_permissions", "ip_restrictions", "rate_limit"
        ]
        write_to_csv(system_config_changes, "system_config_changes.csv", system_config_fieldnames)

if __name__ == "__main__":
    asyncio.run(main())