import json
import csv
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# 使用高效的JSON库
try:
    import ujson as json
except ImportError:
    import json

# 定义每个日志文件的字段
fields = {
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

# 定义日志文件路径的基路径
base_log_path = '/var/log/gitlab/gitlab-rails/'

# 定义日志文件路径
log_files = {
    'application_json': base_log_path + 'application_json.log',
    'audit_json': base_log_path + 'audit_json.log',
    'production_json': base_log_path + 'production_json.log',
    'integrations_json': base_log_path + 'integrations_json.log',
    'features_json': base_log_path + 'features_json.log'
}

# 定义输出CSV文件路径
output_csvs = {
    'application_json': 'application_json_logs.csv',
    'audit_json': 'audit_json_logs.csv',
    'production_json': 'production_json_logs.csv',
    'integrations_json': 'integrations_json_logs.csv',
    'features_json': 'features_json_logs.csv'
}

# 开始时间，例如：datetime.fromisoformat("2024-12-04T00:02:00Z")
START_TIME = None

# 用于存储提取的数据
data = {category: [] for category in fields}

from datetime import datetime, timezone

# 标准化时间处理
def parse_iso8601_time(time_str):
    if not time_str:
        return None
    try:
        # 处理带Z的ISO 8601格式时间字符串
        if time_str.endswith('Z'):
            time_str = time_str[:-1] + '+00:00'
        
        dt = datetime.fromisoformat(time_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        # print(f"Warning: Invalid time format: {time_str}")
        return None

# 解析日志文件
def parse_log_file(file_path, category):
    if not Path(file_path).exists():
        print(f"文件 {file_path} 不存在，跳过处理。")
        return

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                log_entry = json.loads(line)
                if START_TIME:
                    log_time = parse_iso8601_time(log_entry.get('time', ''))
                    if log_time is None or log_time < START_TIME:
                        continue
                entry = {field: log_entry.get(field, '') for field in fields[category]}
                entry['category'] = category
                yield entry  # 使用生成器逐行返回数据以节省内存
            except json.JSONDecodeError:
                continue

# 将数据保存到CSV文件
def save_to_csv(data_generator, output_csv, category):
    fieldnames = ['category'] + fields[category]
    file_exists = Path(output_csv).exists()

    with open(output_csv, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for entry in data_generator:
            writer.writerow(entry)

# 多线程处理日志文件
def process_log_files(log_files):
    with ThreadPoolExecutor() as executor:
        # 提交任务时同时传递 category 和 file_path
        futures = {executor.submit(parse_log_file, file_path, category): category for category, file_path in log_files.items()}
        for future in futures:
            try:
                category = futures[future]
                data_gen = future.result()
                save_to_csv(data_gen, output_csvs[category], category)
            except Exception as e:  # 更广泛的异常捕获以确保所有异常都能被捕获
                print(f"Error processing file for category {futures[future]}: {e}")

# 主函数
def main():
    process_log_files(log_files)
    print("日志数据已提取并保存到相应文件")

if __name__ == "__main__":
    main()