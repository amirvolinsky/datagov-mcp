# server.py
from fastmcp import FastMCP, Context
from fastapi import Request
from fastapi.responses import JSONResponse
import requests

# יצירת MCP
mcp = FastMCP("DataGovIL", dependencies=["requests"])
app = mcp.http_app  # אל תשתמש בסוגריים!!!

BASE_URL = "https://data.gov.il/api/3"

@mcp.tool()
async def fetch_data(ctx: Context, dataset_name: str, limit: int = 100):
    """Fetch dataset by name from data.gov.il"""
    r = requests.get(f"{BASE_URL}/action/package_show?id={dataset_name}")
    r.raise_for_status()
    result = r.json()["result"]
    resource_id = result["resources"][0]["id"]

    data_r = requests.get(f"{BASE_URL}/action/datastore_search", params={
        "resource_id": resource_id,
        "limit": limit
    })
    data_r.raise_for_status()
    return data_r.json()["result"]["records"]

@app.get("/fetch_data")
async def fetch_data_http(request: Request):
    dataset_name = request.query_params.get("dataset_name")
    if not dataset_name:
        return JSONResponse({"error": "dataset_name missing"}, status_code=400)
    ctx = Context("http")
    return await fetch_data(ctx, dataset_name)

if __name__ == "__main__":
    mcp.run()
