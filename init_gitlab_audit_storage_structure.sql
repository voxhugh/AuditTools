-- 创建数据库（如果不存在），用于存储GitLab审计相关的表，这里假设开发环境，命名为dev
CREATE DATABASE IF NOT EXISTS dev;
-- 使用创建好的数据库
USE dev;

-- 用户维度表（dim_users_info），存储用户基本信息及相关拓展信息
CREATE TABLE dim_users_info (
    user_id INT NOT NULL COMMENT '用户唯一标识符',
    user_name VARCHAR(255) NOT NULL COMMENT '用户真实姓名或用户名',
    email VARCHAR(255) COMMENT '用户的电子邮箱地址',
    user_status VARCHAR(20) COMMENT '用户当前状态，使用"枚举类型"明确取值范围', -- 'active', 'inactive','suspended'
    user_tags ARRAY<STRING> COMMENT '用户相关标签列表，如角色标签等，方便对用户进行分类标记',
    user_attributes MAP<STRING, STRING> COMMENT '用户额外属性信息，以键值对形式存储，可用于扩展存储一些其他相关信息'
) ENGINE=OLAP
UNIQUE KEY (user_id, user_name) -- 确保用户标识唯一性
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4" -- 示例添加lz4压缩算法，可根据测试和需求调整
);

-- 项目维度表（dim_projects_info），存放项目基础信息及拓展信息
CREATE TABLE dim_projects_info (
    project_id INT NOT NULL COMMENT '项目的唯一标识',
    project_name VARCHAR(255) NOT NULL COMMENT '项目名称',
    project_desc TEXT COMMENT '项目的详细描述内容',
    project_tags ARRAY<STRING> COMMENT '项目相关标签，便于分类查询项目，比如项目所属业务领域等',
    project_metadata JSON COMMENT '项目的元数据，以JSON格式存储，可灵活存放一些项目相关的复杂配置或其他拓展信息'
) ENGINE=OLAP
UNIQUE KEY (project_id) -- 项目标识唯一键
DISTRIBUTED BY HASH(project_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 组维度表（dim_groups_info），存放组的基础信息及相关额外信息
CREATE TABLE dim_groups_info (
    group_id INT NOT NULL COMMENT '组的唯一标识符',
    group_name VARCHAR(255) NOT NULL COMMENT '组的名称',
    group_desc TEXT COMMENT '组的详细描述信息',
    group_members ARRAY<INT> COMMENT '组成员的用户ID列表，方便直观体现组内成员情况',
    group_attributes MAP<STRING, STRING> COMMENT '组的额外属性信息，以键值对形式存储其他相关信息'
) ENGINE=OLAP
UNIQUE KEY (group_id) -- 组标识唯一键
DISTRIBUTED BY HASH(group_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 业务事实表（fact_user_operations_records），记录用户操作信息
CREATE TABLE fact_user_operations_records (
    operation_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '操作记录唯一标识符',
    user_id INT NOT NULL COMMENT '执行操作的用户 ID',
    operation_type VARCHAR(50) NOT NULL COMMENT '操作类型',
    operation_time DATETIME NOT NULL COMMENT '操作发生时间戳',
    related_project_id INT COMMENT '关联项目 ID，指向dim_projects_info表，用于分析项目相关的用户操作',
    related_group_id INT COMMENT '关联组 ID，指向dim_groups_info表，用于分析组相关的用户操作',
    operation_details JSON COMMENT '操作详细内容描述，使用JSON格式可更好应对复杂多样的操作详情结构',
    operation_metadata MAP<STRING, STRING> COMMENT '操作相关的额外元数据，如操作来源、操作影响范围等，辅助审计分析'
) ENGINE=OLAP
UNIQUE KEY (operation_id, user_id) -- 确保记录唯一性
DISTRIBUTED BY HASH(operation_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 项目和组变更事实表（fact_project_group_changes_records），记录项目和组的变更情况
CREATE TABLE fact_project_group_changes_records (
    change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '变更记录唯一标识符',
    object_type VARCHAR(10) NOT NULL COMMENT '变更对象类型 (\'项目\' 或 \'组\')',
    object_id INT NOT NULL COMMENT '变更对象 ID，对应项目的project_id或组的group_id，关联相应维度表',
    change_type VARCHAR(20) NOT NULL COMMENT '变更类型，如新增、删除、修改等，用于分类统计变更情况',
    change_time DATETIME NOT NULL COMMENT '变更发生时间戳',
    operator_id INT NOT NULL COMMENT '执行变更操作的用户 ID，关联dim_users_info表，确定变更操作人',
    change_details TEXT COMMENT '变更详细内容描述，详细记录项目或组的变更信息',
    change_metadata MAP<STRING, STRING> COMMENT '变更相关的额外元数据，如变更的影响范围等，辅助审计分析'
) ENGINE=OLAP
UNIQUE KEY (change_id) -- 变更记录唯一键
DISTRIBUTED BY HASH(change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 代码变更事实表（fact_code_changes_records），记录代码变更相关活动
CREATE TABLE fact_code_changes_records (
    code_change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '代码变更记录唯一标识符',
    operation_type VARCHAR(50) NOT NULL COMMENT '代码变更操作类型，如提交、合并等，用于分类统计代码变更操作',
    author_id INT COMMENT '执行代码变更操作的用户 ID，关联dim_users_info表，确定代码变更操作人',
    time_stamp DATETIME NOT NULL COMMENT '代码变更发生时间戳',
    content TEXT NOT NULL COMMENT '代码变更详细内容描述，详细记录代码变更的具体内容',
    hash_value STRING COMMENT '代码提交的完整哈希值',
    project_id INT NOT NULL COMMENT '所属项目 ID，指向dim_projects_info表，用于分析项目相关的代码变更情况',
    merge_request_id INT COMMENT '合并请求 ID（若有），可关联到对应的合并请求相关记录',
    code_change_metadata MAP<STRING, STRING> COMMENT '代码变更相关的额外元数据，例如变更涉及的代码模块等信息，辅助审计分析'
) ENGINE=OLAP
UNIQUE KEY (code_change_id) -- 代码变更唯一键
DISTRIBUTED BY HASH(code_change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 审核事实表（fact_audit_records_info），存储审核相关信息
CREATE TABLE fact_audit_records_info (
    audit_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '审核记录唯一标识符',
    related_object_id INT COMMENT '关联对象 ID（如合并请求的mr_id、代码变更的code_change_id等），用于关联到具体被审核的对象记录',
    object_type VARCHAR(20) NOT NULL COMMENT '关联对象类型 (\'合并请求\', \'代码变更\' 等)',
    reviewers_ids ARRAY<INT> COMMENT '参与审核的用户 ID，关联dim_users_info表，确定审核人员',
    audit_time DATETIME NOT NULL COMMENT '审核发生时间戳',
    audit_result VARCHAR(20) NOT NULL COMMENT '审核结果 (\'批准\', \'拒绝\', \'建议修改\' 等)',
    comment TEXT COMMENT '审核评论内容，记录审核人员给出的具体意见和建议',
    project_id INT NOT NULL COMMENT '所属项目 ID，指向dim_projects_info表，用于分析项目相关的审核情况',
    audit_metadata MAP<STRING, STRING> COMMENT '审核相关的额外元数据，如审核依据等信息，辅助审计分析'
) ENGINE=OLAP
UNIQUE KEY (audit_id) -- 审核记录唯一键
DISTRIBUTED BY HASH(audit_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- CI/CD管道活动事实表（fact_cicd_pipeline_activities_records），记录CI/CD管道活动相关信息
CREATE TABLE fact_cicd_pipeline_activities_records (
    activity_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '管道活动记录唯一标识符',
    pipeline_id INT COMMENT '所属CI/CD管道 ID，可关联到对应的管道配置等相关记录（应用层面需确保其值存在对应的管道配置表中）',
    stage_name VARCHAR(255) NOT NULL COMMENT '活动所在管道阶段名称，明确活动所处的具体阶段',
    job_name VARCHAR(255) NOT NULL COMMENT '任务名称，用于区分不同的任务',
    start_time DATETIME NOT NULL COMMENT '任务开始时间戳，便于按时间顺序分析管道活动情况',
    end_time DATETIME NOT NULL COMMENT '任务结束时间戳，结合开始时间可分析任务执行时长等情况',
    job_status VARCHAR(20) NOT NULL COMMENT '任务执行状态 (\'成功\', \'失败\', \'运行中\' 等)',
    triggered_by_id INT COMMENT '触发用户 ID（若有），关联dim_users_info表，确定触发活动的用户',
    project_id INT NOT NULL COMMENT '所属项目 ID，指向dim_projects_info表，用于分析项目相关的管道活动情况'
) ENGINE=OLAP
UNIQUE KEY (activity_id) -- 管道活动记录唯一键
DISTRIBUTED BY HASH(activity_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- CI/CD配置变更事实表（fact_cicd_config_changes_records），记录CI/CD配置变更相关信息
CREATE TABLE fact_cicd_config_changes_records (
    config_change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT 'CI/CD配置变更记录唯一标识符',
    change_type VARCHAR(50) NOT NULL COMMENT '配置变更类型 (\'参数调整\', \'新增模块\', \'删除规则\' 等)',
    change_detail TEXT NOT NULL COMMENT '配置变更详细内容描述，详细记录配置变更的具体内容',
    author_id INT NOT NULL COMMENT '发起配置变更的用户 ID，关联dim_users_info表，确定变更发起人',
    change_time DATETIME NOT NULL COMMENT '配置变更发生时间戳，便于按时间维度分析配置变更情况',
    project_id INT NOT NULL COMMENT '所属项目 ID，指向dim_projects_info表，用于分析项目相关的配置变更情况'
) ENGINE=OLAP
UNIQUE KEY (config_change_id) -- 配置变更唯一键
DISTRIBUTED BY HASH(config_change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);

-- 系统配置变更事实表（fact_system_config_changes_records），记录系统配置变更相关信息
CREATE TABLE fact_system_config_changes_records (
    config_change_id BIGINT NOT NULL AUTO_INCREMENT COMMENT '系统配置变更记录唯一标识符',
    change_object VARCHAR(255) NOT NULL COMMENT '变更对象（具体的配置项名称等），明确具体是哪个配置项发生了变更',
    change_type VARCHAR(20) NOT NULL COMMENT '变更类型 (\'添加配置\', \'修改配置\', \'删除配置\' 等)',
    change_detail TEXT NOT NULL COMMENT '变更详细内容描述，详细记录系统配置变更的具体内容',
    change_time DATETIME NOT NULL COMMENT '变更发生时间戳，便于按时间维度分析系统配置变更情况',
    operator_id INT NOT NULL COMMENT '执行变更操作的用户 ID，关联dim_users_info表，确定变更操作人'
) ENGINE=OLAP
UNIQUE KEY (config_change_id) -- 配置变更唯一键
DISTRIBUTED BY HASH(config_change_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "compression" = "lz4"
);