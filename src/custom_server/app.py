import os
import re
from typing import Dict, Any, List
from databricks.sdk import WorkspaceClient
from mcp.server.fastmcp import FastMCP

# Create MCP server 
mcp = FastMCP("Triage MCP Server")
w = WorkspaceClient()

def strip_ansi_codes(text: str) -> str:
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

@mcp.tool(
    name="get_job_names",
    description="Retrieves all Databricks jobs in the workspace. Returns job_id and job_name."
)
def get_job_names() -> List[Dict[str, Any]]:
    try:
        return [{"job_id": job.job_id, "job_name": job.settings.name} 
                for job in w.jobs.list() if job.settings and job.settings.name]
    except Exception as e:
        return [{"error": f"Failed to retrieve jobs: {str(e)}"}]

@mcp.tool(
    name="get_job_info",
    description="Retrieves the configuration and recent execution history (success/failure) for a job."
)
def get_job_info(job_id: int, limit_runs: int = 5) -> Dict[str, Any]:
    try:
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

@mcp.tool(
    name="get_run_error_logs",
    description="Fetches the deep notebook stack trace and task-level errors for a failed job run."
)
def get_run_error_logs(run_id: int) -> List[Dict[str, Any]]:
    try:
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


def main():
    # Force the server to bind to 0.0.0.0 and the Databricks assigned port
    # This prevents the 502 Bad Gateway error.
    port = int(os.getenv("DATABRICKS_APP_PORT", 8000))
    mcp.run(transport='sse', host="0.0.0.0", port=port)