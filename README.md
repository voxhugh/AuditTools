<h1 align = "center">CodeAudit tools based on gitlab-api</h1>

### Introduce

This is an instance-level auditing tool based on gitlab-api.

Including but not limited to:

- User managment
- Proj/Group managment
- Code changes
- Review and Compliance

- CI/CD Pipelines
- System Configuration

### explain

`audit_logs.py`

- Acquire users/proj/groups and system changes

`code_logs.py`

- Acquire code changes, audits and compliance, CI/CD pipelines

`native_logs.py`

- Get the log in the gitlab file system