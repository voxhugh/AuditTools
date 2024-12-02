import json
import csv
from datetime import datetime
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

# 预留时间点，默认None表示提取所有日志
start_time = None  # 示例：datetime(2023, 1, 1, 0, 0, 0)

# 用于存储提取的数据
data = {category: [] for category in fields}

# 解析日志文件
def parse_log_file(file_path, category, start_time=None):
    if not Path(file_path).exists():
        print(f"文件 {file_path} 不存在，跳过处理。")
        return

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                log_entry = json.loads(line)
                if start_time is not None:
                    log_time = datetime.fromisoformat(log_entry.get('time', ''))
                    if log_time < start_time:
                        continue
                entry = {field: log_entry.get(field, '') for field in fields[category]}
                entry['category'] = category
                entry['time'] = log_entry.get('time', '')
                data[category].append(entry)
            except json.JSONDecodeError:
                continue

# 将数据保存到CSV文件
def save_to_csv(data, output_csv, category):
    fieldnames = ['category', 'time'] + fields[category]
    file_exists = Path(output_csv).exists()

    with open(output_csv, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

# 多线程处理日志文件
def process_log_files(log_files, start_time=None):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(parse_log_file, file_path, category, start_time) for category, file_path in log_files.items()]
        for future in futures:
            try:
                future.result()
            except FileNotFoundError as e:
                print(e)

# 主函数
def main():
    process_log_files(log_files, start_time)
    for category, entries in data.items():
        if entries:
            save_to_csv(entries, output_csvs[category], category)

if __name__ == "__main__":
    main()
    print("日志数据已提取并保存到相应文件")
