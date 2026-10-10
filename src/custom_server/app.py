from pathlib import Path
import re
import contextvars
from typing import Dict, Any, List, Optional
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient

STATIC_DIR = Path(__file__).parent / "static"

# ---------------------------------------------------------------------------
# Hybrid authentication: service principal + user-identity filtering
# ---------------------------------------------------------------------------
# The Databricks Apps user-authorization OAuth token does not include a
# "jobs" scope, so the forwarded user token cannot call the Jobs API
# directly.  Instead, we use the app's service principal for all Jobs API
# calls and filter results by the calling user's email (forwarded in the
# X-Forwarded-Email header).  This gives each user a personalized view of
# only the jobs they own, without requiring the jobs OAuth scope.
#
# When X-Forwarded-Email is absent (e.g. local dev), no filtering is applied
# and all jobs the service principal can see are returned.
_user_email: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_user_email", default=None
)

_default_client = WorkspaceClient()


def _get_workspace_client() -> WorkspaceClient:
    """Return the service-principal WorkspaceClient for API calls.
    Per-user filtering is applied at the tool level using _user_email."""
    return _default_client


def _get_user_email() -> Optional[str]:
    """Return the calling user's email from the forwarded header, or None."""
    return _user_email.get()


# Create an MCP server
mcp = FastMCP("Triage MCP Server")

def strip_ansi_codes(text: str) -> str:
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

@mcp.tool()
def get_job_names() -> List[Dict[str, Any]]:
    """Retrieves all Databricks jobs the calling user has access to (filtered by owner).
    Returns job_id and job_name. In local dev (no forwarded user), returns all jobs."""
    try:
        w = _get_workspace_client()
        user_email = _get_user_email()
        jobs = []
        for job in w.jobs.list():
            if not job.settings or not job.settings.name:
                continue
            # Filter by owner when running inside Databricks Apps
            if user_email:
                creator = getattr(job, 'creator_user_name', None) or ''
                if creator != user_email:
                    continue
            jobs.append({"job_id": job.job_id, "job_name": job.settings.name})
        return jobs
    except Exception as e:
        return [{"error": f"Failed to retrieve jobs: {str(e)}"}]

@mcp.tool()
def get_job_info(job_id: int, limit_runs: int = 5) -> Dict[str, Any]:
    """Retrieves the configuration and recent execution history (success/failure) for a job."""
    try:
        w = _get_workspace_client()
        user_email = _get_user_email()
        job = w.jobs.get(job_id)
        # Verify ownership when running inside Databricks Apps
        if user_email:
            creator = getattr(job, 'creator_user_name', None) or ''
            if creator != user_email:
                return {"error": f"Access denied: job {job_id} is not owned by {user_email}"}
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
        # Verify ownership when running inside Databricks Apps
        user_email = _get_user_email()
        if user_email:
            creator = getattr(run, 'creator_user_name', None) or ''
            if creator != user_email:
                return [{"error": f"Access denied: run {run_id} is not owned by {user_email}"}]
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


mcp_app = mcp.streamable_http_app()

# This is what Uvicorn is looking for. It must be named 'app'.
app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.middleware("http")
async def capture_user_token(request: Request, call_next):
    """Extract the calling user's email from the X-Forwarded-Email header
    forwarded by Databricks Apps and store it in a ContextVar for the request
    lifetime.  Used for filtering jobs by owner."""
    email = request.headers.get("x-forwarded-email")
    email_set = _user_email.set(email) if email else None
    try:
        return await call_next(request)
    finally:
        if email_set is not None:
            _user_email.reset(email_set)

@app.get("/", include_in_schema=False)
async def serve_index():
    if (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    return {"status": "Databricks Triage MCP Server is running."}

# Mount the MCP app AFTER defining middleware so the middleware applies
# to the mounted sub-application as well.
app.mount("/", mcp_app)