-- 创建名为 dev 的数据库（若不存在），用于存放 GitLab 审计相关表，适用于开发环境
CREATE DATABASE IF NOT EXISTS dev;
-- 切换到 dev 数据库，后续操作在此数据库中进行
USE dev;

-- 用户维度表 dim_users_info，存储用户基本及拓展信息，用于用户相关分析
CREATE TABLE dim_users_info (
    user_id INT NOT NULL COMMENT '用户唯一标识',
    user_name VARCHAR(255) NOT NULL COMMENT '用户名或真实姓名',
    email VARCHAR(255) COMMENT '用户邮箱',
    user_status VARCHAR(20) COMMENT '用户状态，如 active 等',
    user_tags ARRAY<STRING> COMMENT '用户标签，如角色等',
    user_attributes MAP<STRING, STRING> COMMENT '用户额外属性'
) ENGINE=OLAP
UNIQUE KEY (user_id, user_name) COMMENT '保证用户标识与名称组合唯一'
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 项目维度表 dim_projects_info，存放项目基础及拓展信息，辅助项目分析
CREATE TABLE dim_projects_info (
    project_id INT NOT NULL COMMENT '项目唯一标识',
    project_name VARCHAR(255) NOT NULL COMMENT '项目名称',
    project_desc TEXT COMMENT '项目描述',
    project_tags ARRAY<STRING> COMMENT '项目标签',
    project_metadata JSON COMMENT '项目元数据'
) ENGINE=OLAP
UNIQUE KEY (project_id) COMMENT '确保项目标识唯一'
DISTRIBUTED BY HASH(project_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 组维度表 dim_groups_info，存储组的基础及额外信息，便于组相关操作分析
CREATE TABLE dim_groups_info (
    group_id INT NOT NULL COMMENT '组唯一标识',
    group_name VARCHAR(255) NOT NULL COMMENT '组名称',
    group_desc TEXT COMMENT '组描述',
    group_members ARRAY<INT> COMMENT '组成员 ID 列表',
    group_attributes MAP<STRING, STRING> COMMENT '组额外属性'
) ENGINE=OLAP
UNIQUE KEY (group_id) COMMENT '保证组标识唯一'
DISTRIBUTED BY HASH(group_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 用户操作事实表 fact_user_operations_records，记录用户操作详情，用于审计用户行为
CREATE TABLE fact_user_operations_records (
    operation_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '操作记录唯一 ID',
    user_id INT NOT NULL COMMENT '操作用户 ID',
    operation_type VARCHAR(50) NOT NULL COMMENT '操作类型',
    operation_time DATETIME NOT NULL COMMENT '操作时间',
    related_project_id INT COMMENT '关联项目 ID',
    related_group_id INT COMMENT '关联组 ID',
    operation_details JSON COMMENT '操作详细信息',
    operation_metadata MAP<STRING, STRING> COMMENT '操作元数据'
) ENGINE=OLAP
UNIQUE KEY (operation_id, user_id) COMMENT '确保操作与用户组合唯一'
DISTRIBUTED BY HASH(operation_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 项目和组变更事实表 fact_project_group_changes_records，记录项目和组变更情况，助力变更审计
CREATE TABLE fact_project_group_changes_records (
    change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '变更记录唯一 ID',
    object_type VARCHAR(10) NOT NULL COMMENT '变更对象类型',
    object_id INT NOT NULL COMMENT '变更对象 ID',
    change_type VARCHAR(20) NOT NULL COMMENT '变更类型',
    change_time DATETIME NOT NULL COMMENT '变更时间',
    operator_id INT NOT NULL COMMENT '操作人 ID',
    change_details TEXT COMMENT '变更详细内容',
    change_metadata MAP<STRING, STRING> COMMENT '变更元数据'
) ENGINE=OLAP
UNIQUE KEY (change_id) COMMENT '保证变更记录唯一'
DISTRIBUTED BY HASH(change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 代码变更事实表 fact_code_changes_records，记录代码变更活动，用于代码管理分析
CREATE TABLE fact_code_changes_records (
    code_change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '代码变更唯一 ID',
    operation_type VARCHAR(50) NOT NULL COMMENT '代码变更类型',
    author_id INT COMMENT '作者 ID',
    time_stamp DATETIME NOT NULL COMMENT '变更时间',
    content TEXT NOT NULL COMMENT '变更内容',
    hash_value STRING COMMENT '提交哈希值',
    project_id INT NOT NULL COMMENT '所属项目 ID',
    merge_request_id INT COMMENT '合并请求 ID',
    code_change_metadata MAP<STRING, STRING> COMMENT '代码变更元数据'
) ENGINE=OLAP
UNIQUE KEY (code_change_id) COMMENT '确保代码变更记录唯一'
DISTRIBUTED BY HASH(code_change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 审核事实表 fact_audit_records_info，存储审核信息，辅助审核流程分析
CREATE TABLE fact_audit_records_info (
    audit_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '审核记录唯一 ID',
    related_object_id INT COMMENT '关联对象 ID',
    object_type VARCHAR(20) NOT NULL COMMENT '关联对象类型',
    reviewers_ids ARRAY<INT> COMMENT '审核人 ID 列表',
    audit_time DATETIME NOT NULL COMMENT '审核时间',
    audit_result VARCHAR(20) NOT NULL COMMENT '审核结果',
    comment TEXT COMMENT '审核评论',
    project_id INT NOT NULL COMMENT '所属项目 ID',
    audit_metadata MAP<STRING, STRING> COMMENT '审核元数据'
) ENGINE=OLAP
UNIQUE KEY (audit_id) COMMENT '保证审核记录唯一'
DISTRIBUTED BY HASH(audit_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- CI/CD 管道活动事实表 fact_cicd_pipeline_activities_records，记录管道活动，用于 CI/CD 流程分析
CREATE TABLE fact_cicd_pipeline_activities_records (
    activity_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '管道活动唯一 ID',
    pipeline_id INT COMMENT '管道 ID',
    stage_name VARCHAR(255) NOT NULL COMMENT '阶段名称',
    job_name VARCHAR(255) NOT NULL COMMENT '任务名称',
    start_time DATETIME NOT NULL COMMENT '开始时间',
    end_time DATETIME NOT NULL COMMENT '结束时间',
    job_status VARCHAR(20) NOT NULL COMMENT '任务状态',
    triggered_by_id INT COMMENT '触发人 ID',
    project_id INT NOT NULL COMMENT '所属项目 ID',
    job_metadata MAP<STRING, STRING> NOT NULL COMMENT '作业元数据'
) ENGINE=OLAP
UNIQUE KEY (activity_id) COMMENT '确保管道活动记录唯一'
DISTRIBUTED BY HASH(activity_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- CI/CD 配置变更事实表 fact_cicd_config_changes_records，记录配置变更，辅助 CI/CD 配置管理
CREATE TABLE fact_cicd_config_changes_records (
    config_change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '配置变更唯一 ID',
    change_type VARCHAR(50) NOT NULL COMMENT '变更类型',
    change_detail TEXT NOT NULL COMMENT '变更内容',
    author_id INT COMMENT '作者 ID',
    change_time DATETIME NOT NULL COMMENT '变更时间',
    project_id INT NOT NULL COMMENT '所属项目 ID',
    config_metadata MAP<STRING, STRING> NOT NULL COMMENT '配置元数据'
) ENGINE=OLAP
UNIQUE KEY (config_change_id) COMMENT '保证配置变更记录唯一'
DISTRIBUTED BY HASH(config_change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 系统配置变更事实表 fact_system_config_changes_records，记录系统配置变更，用于系统配置管理分析
CREATE TABLE fact_system_config_changes_records (
    config_change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '系统配置变更唯一 ID',
    change_object VARCHAR(255) NOT NULL COMMENT '变更对象（具体的配置项名称等）',
    change_type VARCHAR(20) NOT NULL COMMENT '变更类型',
    change_detail TEXT NOT NULL COMMENT '变更详细内容',
    change_time DATETIME NOT NULL COMMENT '变更时间',
    operator_id INT COMMENT '操作人 ID'
) ENGINE=OLAP
UNIQUE KEY (config_change_id) COMMENT '保证系统配置变更记录唯一'
DISTRIBUTED BY HASH(config_change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);