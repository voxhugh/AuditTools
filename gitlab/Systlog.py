import json
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging
import asyncio
import os

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 使用高效的JSON库
try:
    import ujson as json
except ImportError:
    import json

BEGIN = None    # 指定开始时间（例如："2024-12-24T02:00:00Z"）

# 日志文件的字段配置
FIELDS_CONFIG = {
    'application': [
        'severity', 'time', 'correlation_id', 'meta.caller_id', 'meta.remote_ip',
        'meta.feature_category', 'meta.user', 'meta.user_id', 'meta.client_id',
        'meta.root_caller_id', 'message', 'mail_subject', 'silent_mode_enabled'
    ],
    'production': [
        'method', 'path', 'format', 'controller', 'action', 'status', 'time',
        'params', 'remote_ip', 'user_id', 'username', 'correlation_id'
    ],
    'audit': [
        'time', 'author_id', 'author_name', 'entity_type', 'change', 'from',
        'to', 'target_id', 'target_type', 'target_details'
    ],
    'integrations': [
        'time', 'service_class', 'project_id', 'project_path', 'message',
        'client_url', 'error'
    ],
    'features': [
        'severity', 'time', 'correlation_id', 'key', 'action', 'extra.thing',
        'extra.percentage'
    ]
}

# 源文件目录
BASE_LOG_PATH = Path('/var/log/gitlab/gitlab-rails/')

# 解析并设置开始时间（UTC）
def set_start_time(start_time_str=None):
    if start_time_str:
        try:
            return datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        except ValueError:
            logging.warning(f"Invalid time format for START_TIME: {start_time_str}")
    return None

START_TIME = set_start_time(BEGIN)

# 创建基于日期的输出目录
def create_output_directory(base_dir='.'):
    date_str = (START_TIME or datetime.now(timezone(timedelta(hours=8)))).strftime('%Y%m%d')
    output_dir = Path(base_dir) / f'LOG_{date_str}'
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

# CSV文件的输出目录
OUTPUT_DIR = create_output_directory(os.path.dirname(__file__))

OUTPUT_CSVS = {
    category: OUTPUT_DIR / f'{category}_logs.csv'
    for category in FIELDS_CONFIG
}

# 日志文件路径
LOG_FILES = {
    category: BASE_LOG_PATH / f'{category}_json.log'
    for category in FIELDS_CONFIG
}

# 标准化时间处理
def parse_iso8601_time(time_str):
    try:
        dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        logging.warning(f"Invalid time format: {time_str}")
        return None

# 解析日志文件并返回生成器
async def parse_log_file(file_path, category):
    if not file_path.exists():
        logging.warning(f"File {file_path} does not exist, skipping.")
        return
    
    with file_path.open('r', encoding='utf-8') as file:
        for line in file:
            try:
                log_entry = json.loads(line)
                log_time = parse_iso8601_time(log_entry.get('time', ''))
                
                if log_time is None or (START_TIME and log_time < START_TIME):
                    continue
                
                entry_fields = {field: log_entry.get(field, '') for field in FIELDS_CONFIG[category]}
                yield {'category': category, **entry_fields}
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode JSON from line: {line[:50]}...")  # 打印前50个字符作为示例
                continue

# 将数据保存到CSV文件
async def save_to_csv(data_generator, output_csv, category):
    fieldnames = ['category'] + FIELDS_CONFIG[category]
    file_exists = output_csv.exists()
    
    with output_csv.open('a', newline='', encoding='utf-8') as csvfile:
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
    tasks = [asyncio.create_task(process_log_file_and_save(LOG_FILES[category], category)) 
             for category in log_files]
    await asyncio.gather(*tasks)

async def main():
    logging.info("Starting the script execution.")
    await process_log_files(LOG_FILES)
    logging.info("Script execution completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())