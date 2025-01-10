-- 首先为所有commit记录添加user_id
UPDATE fact_code_changes_records c
SET author_id = (
    SELECT MIN(u.user_id)
    FROM dim_users_info u
    WHERE u.email = c.code_change_metadata['email']
)
WHERE c.operation_type = 'commit';

-- 视图：用户操作记录
CREATE VIEW user_operations_view AS
SELECT
    uo.operation_id,
    uo.user_id,
    uo.operation_type,
    uo.operation_time,
    ui.user_name
FROM
    fact_user_operations_records uo
JOIN
    dim_users_info ui ON uo.user_id = ui.user_id;

-- 视图：项目变更记录
CREATE VIEW project_changes_view AS
SELECT
    pc.change_id,
    pc.object_type,
    pc.change_type,
    pc.change_time,
    pi.project_name
FROM
    fact_project_group_changes_records pc
LEFT JOIN
    dim_projects_info pi ON pc.object_id = pi.project_id
WHERE
    pc.object_type = 'Project'
UNION ALL
SELECT
    pc.change_id,
    pc.object_type,
    pc.change_type,
    pc.change_time,
    gi.group_name
FROM
    fact_project_group_changes_records pc
LEFT JOIN
    dim_groups_info gi ON pc.object_id = gi.group_id
WHERE
    pc.object_type = 'Group';

-- 视图：代码变更记录
CREATE VIEW code_changes_view AS
SELECT
    cc.code_change_id,
    cc.operation_type,
    cc.author_id,
    cc.time_stamp,
    pi.project_name
FROM
    fact_code_changes_records cc
JOIN
    dim_projects_info pi ON cc.project_id = pi.project_id;

-- 视图：审核记录
CREATE VIEW audit_records_view AS
SELECT
    ar.audit_id,
    ar.audit_result,
    ar.comment,
    pi.project_name
FROM
    fact_audit_records_info ar
JOIN
    dim_projects_info pi ON ar.project_id = pi.project_id;

-- 视图：CICD 管道活动
CREATE VIEW cicd_activities_view AS
SELECT
    ca.activity_id,
    ca.pipeline_id,
    ca.stage_name,
    ca.job_name,
    ca.start_time,
    ca.end_time,
    ca.job_status,
    pi.project_name
FROM
    fact_cicd_pipeline_activities_records ca
JOIN
    dim_projects_info pi ON ca.project_id = pi.project_id;

-- 视图：系统配置变更
CREATE VIEW system_config_changes_view AS
SELECT
    sc.config_change_id,
    sc.change_type,
    sc.change_object,
    sc.change_detail,
    sc.change_time
FROM
    fact_system_config_changes_records sc;