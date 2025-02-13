-- 用户操作记录
WITH user_operations_data AS (
    SELECT
        uo.operation_id,
        uo.user_id,
        uo.operation_type,
        uo.operation_time,
        ui.nickname
    FROM
        fact_user_operations_records uo
    JOIN
        dim_users_info ui ON uo.user_id = ui.user_id
)
SELECT * FROM user_operations_data;

-- 项目/组变更记录
WITH project_changes_data AS (
    (SELECT
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
        pc.object_type = 'Project')
    UNION ALL
    (SELECT
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
        pc.object_type = 'Group')
)
SELECT * FROM project_changes_data;

-- 代码变更记录
WITH code_changes_commit AS (
    SELECT
        cc.code_change_id,
        cc.operation_type,
        dui.nickname AS author_nickname,
        cc.time_stamp,
        pi.project_name
    FROM
        fact_code_changes_records cc
    JOIN
        dim_projects_info pi ON cc.project_id = pi.project_id
    JOIN
        dim_users_info dui ON 
            cc.operation_type = 'commit' 
            AND cc.code_change_metadata['email'] = dui.email
    WHERE
        cc.operation_type = 'commit'
),
code_changes_non_commit AS (
    SELECT
        cc.code_change_id,
        cc.operation_type,
        dui.nickname AS author_nickname,
        cc.time_stamp,
        pi.project_name
    FROM
        fact_code_changes_records cc
    JOIN
        dim_projects_info pi ON cc.project_id = pi.project_id
    JOIN
        dim_users_info dui ON 
            cc.operation_type != 'commit' 
            AND cc.author_id = dui.user_id
    WHERE
        cc.operation_type != 'commit'
)
SELECT * FROM code_changes_commit
UNION ALL
SELECT * FROM code_changes_non_commit;

-- 审核记录
WITH audit_records_data AS (
    SELECT
        ar.audit_id,
        ar.audit_result,
        ar.comment,
        pi.project_name
    FROM
        fact_audit_records_info ar
    JOIN
        dim_projects_info pi ON ar.project_id = pi.project_id
)
SELECT * FROM audit_records_data;

-- CICD管道活动
WITH cicd_activities_data AS (
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
        dim_projects_info pi ON ca.project_id = pi.project_id
)
SELECT * FROM cicd_activities_data;

-- CICD配置变更
WITH cicd_changes_data AS (
    SELECT
        cc.config_change_id,
        cc.change_type,
        cc.change_time,
        pi.project_name
    FROM 
        fact_cicd_config_changes_records cc
    JOIN
        dim_projects_info pi ON cc.project_id = pi.project_id
)
SELECT * FROM cicd_changes_data;

-- 系统配置变更
WITH system_config_changes_data AS (
    SELECT
        sc.config_change_id,
        sc.change_type,
        sc.change_object,
        sc.change_detail,
        sc.change_time
    FROM
        fact_system_config_changes_records sc
)
SELECT * FROM system_config_changes_data;