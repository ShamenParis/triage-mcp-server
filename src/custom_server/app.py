from pathlib import Path
import re
import contextvars
from typing import Dict, Any, List
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

STATIC_DIR = Path(__file__).parent / "static"

# ---------------------------------------------------------------------------
# Per-user authentication
# ---------------------------------------------------------------------------
# Databricks Apps forward the calling user's OAuth access token in the
# x-forwarded-access-token HTTP header.  We capture it per-request via a
# ContextVar and build a WorkspaceClient that authenticates as that user,
# so Databricks enforces the user's own permissions (job visibility, run
# access, etc.).  When the header is absent (e.g. local dev), we fall back
# to the app's service principal.
_user_token: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_user_token", default=None
)

_default_client = WorkspaceClient()


def _get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient authenticated as the calling user, or the
    app's service principal if no user token is present."""
    token = _user_token.get()
    if token:
        # auth_type="pat" tells the SDK to prefer PAT auth. This bypasses the
        # _validate() check that rejects configs with multiple auth methods
        # (the Databricks Apps env has DATABRICKS_CLIENT_ID/CLIENT_SECRET for
        # the service principal, which the SDK would otherwise detect as OAuth).
        # DefaultCredentials.__call__ also skips non-matching providers when
        # auth_type is explicitly set, so only pat_auth is attempted.
        cfg = Config(
            host=_default_client.config.host,
            token=token,
            auth_type="pat",
        )
        return WorkspaceClient(config=cfg)
    return _default_client


# Create an MCP server
mcp = FastMCP("Triage MCP Server")

def strip_ansi_codes(text: str) -> str:
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

@mcp.tool()
def get_job_names() -> List[Dict[str, Any]]:
    """Retrieves all Databricks jobs in the workspace. Returns job_id and job_name."""
    try:
        w = _get_workspace_client()
        return [{"job_id": job.job_id, "job_name": job.settings.name} 
                for job in w.jobs.list() if job.settings and job.settings.name]
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


mcp_app = mcp.streamable_http_app()

# This is what Uvicorn is looking for. It must be named 'app'.
app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.middleware("http")
async def capture_user_token(request: Request, call_next):
    """Extract the calling user's access token from the header forwarded by
    Databricks Apps and store it in a ContextVar for the request lifetime."""
    token = request.headers.get("x-forwarded-access-token")
    token_set = _user_token.set(token) if token else None
    try:
        return await call_next(request)
    finally:
        if token_set is not None:
            _user_token.reset(token_set)

@app.get("/", include_in_schema=False)
async def serve_index():
    if (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    return {"status": "Databricks Triage MCP Server is running."}

# Mount the MCP app AFTER defining middleware so the middleware applies
# to the mounted sub-application as well.
app.mount("/", mcp_app)