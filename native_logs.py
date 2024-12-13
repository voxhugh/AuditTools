import json
import csv
from datetime import datetime, timezone
from pathlib import Path
import logging
import asyncio

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 使用高效的JSON库
try:
    import ujson as json
except ImportError:
    import json

# 日志文件的字段
FIELDS = {
    'application_json': [
        'severity', 'time', 'correlation_id', 'meta.caller_id', 'meta.remote_ip',
        'meta.feature_category', 'meta.user', 'meta.user_id', 'meta.client_id',
        'meta.root_caller_id', 'message', 'mail_subject', 'silent_mode_enabled'
    ],
    'production_json': [
        'method', 'path', 'format', 'controller', 'action', 'status', 'time',
        'params', 'remote_ip', 'user_id', 'username', 'correlation_id'
    ],
    'audit_json': [
        'time', 'author_id', 'author_name', 'entity_type', 'change', 'from',
        'to', 'target_id', 'target_type', 'target_details'
    ],
    'integrations_json': [
        'time', 'service_class', 'project_id', 'project_path', 'message',
        'client_url', 'error'
    ],
    'features_json': [
        'severity', 'time', 'correlation_id', 'key', 'action', 'extra.thing',
        'extra.percentage'
    ]
}

# 基路径
BASE_LOG_PATH = '/var/log/gitlab/gitlab-rails/'

# 日志文件路径
LOG_FILES = {
    'application_json': BASE_LOG_PATH + 'application_json.log',
    'audit_json': BASE_LOG_PATH + 'audit_json.log',
    'production_json': BASE_LOG_PATH + 'production_json.log',
    'integrations_json': BASE_LOG_PATH + 'integrations_json.log',
    'features_json': BASE_LOG_PATH + 'features_json.log'
}

# 输出CSV文件路径
OUTPUT_CSVS = {
    'application_json': 'application_json_logs.csv',
    'audit_json': 'audit_json_logs.csv',
    'production_json': 'production_json_logs.csv',
    'integrations_json': 'integrations_json_logs.csv',
    'features_json': 'features_json_logs.csv'
}

START_TIME = None  # 开始时间，设置为None表示不对时间过滤

# 标准化时间处理
def parse_iso8601_time(time_str):
    if not time_str:
        return None
    try:
        if time_str.endswith('Z'):
            time_str = time_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(time_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        logging.warning(f"Invalid time format: {time_str}")
        return None

# 解析日志文件并返回生成器
async def parse_log_file(file_path, category):
    if not Path(file_path).exists():
        logging.warning(f"File {file_path} does not exist, skipping.")
        return
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                log_entry = json.loads(line)
                if START_TIME:
                    log_time = parse_iso8601_time(log_entry.get('time', ''))
                    if log_time is None or log_time < START_TIME:
                        continue
                entry = {field: log_entry.get(field, '') for field in FIELDS[category]}
                entry['category'] = category
                yield entry  # 使用生成器逐行返回
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode JSON from line: {line[:50]}...")  # 打印前50个字符作为示例
                continue

# 将数据保存到CSV文件
async def save_to_csv(data_generator, output_csv, category):
    fieldnames = ['category'] + FIELDS[category]
    file_exists = Path(output_csv).exists()
    with open(output_csv, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        async for entry in data_generator:
            writer.writerow(entry)

# 创建一个新的协程来处理每个日志文件的解析和保存
async def process_log_file_and_save(file_path, category):
    try:
        data_gen = parse_log_file(file_path, category)
        await save_to_csv(data_gen, OUTPUT_CSVS[category], category)
    except Exception as e:
        logging.error(f"Error processing and saving file for category {category}: {e}")

# 处理日志文件
async def process_log_files(log_files):
    tasks = []
    for category, file_path in log_files.items():
        task = asyncio.create_task(process_log_file_and_save(file_path, category))
        tasks.append(task)

    await asyncio.gather(*tasks)

async def main():
    logging.info("Starting the script execution.")
    
    await process_log_files(LOG_FILES)
    
    logging.info("Script execution completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())