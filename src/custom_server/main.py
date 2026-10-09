import uvicorn
import os

def main():
    port = int(os.getenv("DATABRICKS_APP_PORT", 8000))
    uvicorn.run(
        "custom_server.app:app",  # import path to your `app`
        host="0.0.0.0",
        port=port,
        reload=False,  # Set to False for production deployment
    )