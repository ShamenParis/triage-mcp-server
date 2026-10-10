import re
import os
import contextvars
from typing import Dict, Any, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from mcp.server.fastmcp import FastMCP
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Hybrid auth: SP (workspace admin) + per-user permission enforcement
# ---------------------------------------------------------------------------
# The "jobs" scope is NOT supported by Databricks Apps user OAuth tokens.
# Therefore we use the app's service principal (must be workspace admin)
# for all Jobs API calls, then filter results using the Permissions API
# to enforce each user's actual job-level access.
#
# Flow:  SP lists all jobs  →  Permissions API checks each job's ACL
#        against the calling user's email & group memberships  →  only
#        permitted jobs are returned.
#
# When X-Forwarded-Email is absent (e.g. local dev), no filtering is
# applied and all jobs the service principal can see are returned.
_user_email: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_user_email", default=None
)

_sp_client = WorkspaceClient()  # uses DATABRICKS_CLIENT_ID / SECRET from env


def _get_workspace_client() -> WorkspaceClient:
    """Return the service-principal WorkspaceClient for Jobs API calls."""
    return _sp_client


def _get_user_email() -> Optional[str]:
    """Return the calling user's email from the forwarded header, or None."""
    return _user_email.get()


def _get_user_identity(w: WorkspaceClient) -> tuple:
    """Resolve the calling user's email, group memberships, and admin status.
    Returns (email, groups, is_admin).  When no user context (local dev),
    returns (None, set(), True) so all jobs are visible."""
    email = _get_user_email()
    if not email:
        return None, set(), True  # local dev — no restrictions
    try:
        users = list(w.users.list(filter=f'userName eq "{email}"'))
        if not users:
            return email, set(), False
        groups: Set[str] = {g.display for g in (users[0].groups or []) if g.display}
        is_admin = "admins" in groups
        return email, groups, is_admin
    except Exception:
        return email, set(), False


def _user_can_view_job(
    w: WorkspaceClient, job_id: int, user_email: str, user_groups: Set[str]
) -> bool:
    """Check if a user has at least CAN_VIEW on a job (directly or via group)."""
    try:
        perms = w.permissions.get("jobs", str(job_id))
        for acl in perms.access_control_list or []:
            if acl.user_name and acl.user_name.lower() == user_email.lower():
                return True
            if acl.group_name and acl.group_name in user_groups:
                return True
        return False
    except Exception:
        return False  # deny on error


# ---------------------------------------------------------------------------
# ASGI middleware: extract X-Forwarded-Email set by Databricks Apps
# and populate the _user_email ContextVar so tools can filter by owner.
# Uses raw ASGI wrapping (safe with MCP's streaming/SSE responses).
# ---------------------------------------------------------------------------
class _UserAuthMiddleware:
    def __init__(self, asgi_app):
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))

            raw_email = headers.get(b"x-forwarded-email")
            email = raw_email.decode("utf-8") if raw_email else None
            email_cv = _user_email.set(email)

            try:
                await self.app(scope, receive, send)
            finally:
                _user_email.reset(email_cv)
        else:
            await self.app(scope, receive, send)


# Create an MCP server
mcp = FastMCP("Triage MCP Server")

@mcp.tool()
def health() -> str:
    """Health check. Returns 'ok' if the server is running."""
    return "ok"

@mcp.tool()
def debug_info() -> Dict[str, Any]:
    """Returns diagnostic info: forwarded user email and a sample job count."""
    try:
        w = _get_workspace_client()
        user_email = _get_user_email()
        job_count = 0
        sample_creator = None
        for job in w.jobs.list():
            job_count += 1
            if sample_creator is None:
                sample_creator = getattr(job, 'creator_user_name', None)
            if job_count >= 5:
                break
        return {
            "user_email": user_email,
            "auth_mode": "service_principal",
            "job_count_sample": job_count,
            "sample_creator": sample_creator,
            "host": w.config.host,
        }
    except Exception as e:
        return {"error": str(e)}

def strip_ansi_codes(text: str) -> str:
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

@mcp.tool()
def get_job_names() -> List[Dict[str, Any]]:
    """Retrieve Databricks jobs the calling user has permission to view."""
    try:
        w = _get_workspace_client()
        email, groups, is_admin = _get_user_identity(w)

        # Collect all jobs via SP (workspace admin)
        all_jobs = []
        for job in w.jobs.list():
            if not job.settings or not job.settings.name:
                continue
            all_jobs.append({
                "job_id": job.job_id,
                "job_name": job.settings.name,
                "creator": getattr(job, 'creator_user_name', None),
            })

        if not all_jobs:
            return [{"message": "No jobs found in workspace."}]

        # Admin or local dev — return all jobs
        if is_admin:
            return sorted(all_jobs, key=lambda j: j["job_name"])

        # Non-admin — filter by per-job permissions (parallel checks)
        def check(job):
            return _user_can_view_job(w, job["job_id"], email, groups)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check, j): j for j in all_jobs}
            allowed = [futures[f] for f in as_completed(futures) if f.result()]

        if not allowed:
            return [{"message": f"No jobs found that {email} has permission to view."}]
        return sorted(allowed, key=lambda j: j["job_name"])
    except Exception as e:
        return [{"error": f"Failed to retrieve jobs: {str(e)}"}]

@mcp.tool()
def get_job_info(job_id: int, limit_runs: int = 5) -> Dict[str, Any]:
    """Retrieves the configuration and recent execution history (success/failure) for a job."""
    try:
        w = _get_workspace_client()
        # Permission check
        email, groups, is_admin = _get_user_identity(w)
        if not is_admin and email:
            if not _user_can_view_job(w, job_id, email, groups):
                return {"error": f"Access denied: {email} does not have permission to view job {job_id}."}
        job = w.jobs.get(job_id)
        job_details = {
            "job_id": job.job_id,
            "job_name": job.settings.name if job.settings else "Unknown",
            "tasks": [task.task_key for task in job.settings.tasks] if job.settings and job.settings.tasks else []
        }
        
        runs_iterator = w.jobs.list_runs(job_id=job_id)
        recent_runs = []
        for i, run in enumerate(runs_iterator):
            if i >= limit_runs: break
            
            error_message = run.state.state_message if run.state and run.state.result_state and run.state.result_state.value == 'FAILED' else None
            recent_runs.append({
                "run_id": run.run_id,
                "result_state": run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN",
                "error_message": error_message,
                "run_url": run.run_page_url
            })
            
        return {"job_config": job_details, "recent_runs": recent_runs}
    except Exception as e:
        return {"error": f"Failed to retrieve info for job_id {job_id}: {str(e)}"}

@mcp.tool()
def get_run_error_logs(run_id: int) -> List[Dict[str, Any]]:
    """Fetches the deep notebook stack trace and task-level errors for a failed job run."""
    try:
        w = _get_workspace_client()
        run = w.jobs.get_run(run_id=run_id)
        # Permission check on the parent job
        email, groups, is_admin = _get_user_identity(w)
        job_id = getattr(run, 'job_id', None)
        if not is_admin and email and job_id:
            if not _user_can_view_job(w, job_id, email, groups):
                return [{"error": f"Access denied: {email} does not have permission to view job {job_id}."}]
        failed_logs = []
        tasks = run.tasks if getattr(run, 'tasks', None) else [run]
        
        for task in tasks:
            if task.state and task.state.result_state and task.state.result_state.value == 'FAILED':
                output = w.jobs.get_run_output(run_id=task.run_id)
                task_error_data = {
                    "task_key": getattr(task, 'task_key', 'single_task'),
                    "task_run_id": task.run_id,
                }
                if getattr(output, 'error_trace', None):
                    task_error_data["stack_trace"] = strip_ansi_codes(output.error_trace)[-3500:] 
                elif getattr(output, 'error', None):
                    task_error_data["stack_trace"] = strip_ansi_codes(output.error)[-1500:]
                else:
                    task_error_data["stack_trace"] = "No stack trace available."
                failed_logs.append(task_error_data)
                
        return failed_logs if failed_logs else [{"status": "No failed tasks found."}]
    except Exception as e:
        return [{"error": f"Failed to retrieve logs: {str(e)}"}]


# Use the MCP streamable HTTP app directly as the ASGI app.
# The streamable_http_app() handles session management, routing,
# and the /mcp endpoint internally.
# Wrap the MCP app with middleware that populates _user_email from headers
_mcp_app = mcp.streamable_http_app()
app = _UserAuthMiddleware(_mcp_app)