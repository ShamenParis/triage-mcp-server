import os
import re
import json
from typing import Dict, Any, List
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Route
from mcp.server import Server
from mcp.server.sse import SseServerTransport
import mcp.types as types
from databricks.sdk import WorkspaceClient

# Initialize Databricks SDK and the native MCP Server
w = WorkspaceClient()
mcp_server = Server("Triage MCP Server")

def strip_ansi_codes(text: str) -> str:
    if not text: return ""
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)

# Define the tools manually for the native Server
@mcp_server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_job_names",
            description="Retrieves all Databricks jobs in the workspace. Returns job_id and job_name.",
            inputSchema={"type": "object", "properties": {}}
        ),
        types.Tool(
            name="get_job_info",
            description="Retrieves the configuration and recent execution history (success/failure) for a job.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer", "description": "The ID of the job"},
                    "limit_runs": {"type": "integer", "description": "Number of runs to limit (default 5)"}
                },
                "required": ["job_id"]
            }
        ),
        types.Tool(
            name="get_run_error_logs",
            description="Fetches the deep notebook stack trace and task-level errors for a failed job run.",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {"type": "integer", "description": "The ID of the run"}
                },
                "required": ["run_id"]
            }
        )
    ]

@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None) -> list[types.TextContent]:
    arguments = arguments or {}
    try:
        if name == "get_job_names":
            jobs = [{"job_id": job.job_id, "job_name": job.settings.name} 
                    for job in w.jobs.list() if job.settings and job.settings.name]
            return [types.TextContent(type="text", text=json.dumps(jobs))]
            
        elif name == "get_job_info":
            job_id = arguments.get("job_id")
            limit_runs = arguments.get("limit_runs", 5)
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
            res = {"job_config": job_details, "recent_runs": recent_runs}
            return [types.TextContent(type="text", text=json.dumps(res))]
            
        elif name == "get_run_error_logs":
            run_id = arguments.get("run_id")
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
            res = failed_logs if failed_logs else [{"status": "No failed tasks found."}]
            return [types.TextContent(type="text", text=json.dumps(res))]
            
        else:
            raise ValueError(f"Unknown tool: {name}")
            
    except Exception as e:
        return [types.TextContent(type="text", text=json.dumps({"error": str(e)}))]


# --- STRICT DATABRICKS ROUTING ---
# This explicitly creates the /mcp and /messages endpoints Databricks UI requires
sse = SseServerTransport("/messages")

async def handle_mcp(request):
    async with sse.connect_sse(request.scope, request.receive, request.send) as streams:
        await mcp_server.run_sse_async(streams[0], streams[1])

async def handle_messages(request):
    await sse.handle_post_message(request.scope, request.receive, request.send)

app = Starlette(routes=[
    Route("/mcp", endpoint=handle_mcp, methods=["GET"]),
    Route("/messages", endpoint=handle_messages, methods=["POST"])
])

def main():
    # Properly binds to 0.0.0.0 to clear the 502 Bad Gateway proxy block
    port = int(os.getenv("DATABRICKS_APP_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)