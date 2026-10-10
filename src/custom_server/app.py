import re
import os
import contextvars
from typing import Dict, Any, List, Optional
from mcp.server.fastmcp import FastMCP
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# User-level OAuth authentication via Databricks Apps
# ---------------------------------------------------------------------------
# Databricks Apps forwards the calling user's OAuth access token in the
# X-Forwarded-Access-Token header.  We create a per-request WorkspaceClient
# with that token so every API call runs as the user and respects the
# user's own permissions — no service-principal job grants needed.
#
# When the forwarded token is absent (e.g. local dev), falls back to
# default WorkspaceClient() auth (PAT, SP, or CLI profile).
_user_token: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_user_token", default=None
)

_user_email: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_user_email", default=None
)


def _get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient authenticated as the calling user when
    running in Databricks Apps, or default auth for local dev."""
    token = _user_token.get()
    if token:
        return WorkspaceClient(
            host=os.environ.get("DATABRICKS_HOST", ""),
            token=token,
        )
    return WorkspaceClient()


def _get_user_email() -> Optional[str]:
    """Return the calling user's email from the forwarded header, or None."""
    return _user_email.get()


# ---------------------------------------------------------------------------
# ASGI middleware: extract X-Forwarded-Access-Token and X-Forwarded-Email
# set by Databricks Apps and populate ContextVars for per-request auth.
# Uses raw ASGI wrapping (safe with MCP's streaming/SSE responses).
# ---------------------------------------------------------------------------
class _UserAuthMiddleware:
    def __init__(self, asgi_app):
        self.app = asgi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope.get("headers", []))

            raw_token = headers.get(b"x-forwarded-access-token")
            access_token = raw_token.decode("utf-8") if raw_token else None
            token_cv = _user_token.set(access_token)

            raw_email = headers.get(b"x-forwarded-email")
            email = raw_email.decode("utf-8") if raw_email else None
            email_cv = _user_email.set(email)

            try:
                await self.app(scope, receive, send)
            finally:
                _user_token.reset(token_cv)
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
            "has_user_token": _user_token.get() is not None,
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
    """Retrieve all Databricks jobs available in the workspace for the user."""
    try:
        w = _get_workspace_client()
        jobs = []
        for job in w.jobs.list():
            if not job.settings or not job.settings.name:
                continue
            jobs.append({"job_id": job.job_id, "job_name": job.settings.name})
        if not jobs:
            return [{"message": "No jobs found. Check your permissions or verify the workspace."}]
        return jobs
    except Exception as e:
        return [{"error": f"Failed to retrieve jobs: {str(e)}"}]

@mcp.tool()
def get_job_info(job_id: int, limit_runs: int = 5) -> Dict[str, Any]:
    """Retrieves the configuration and recent execution history (success/failure) for a job."""
    try:
        w = _get_workspace_client()
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