# server.py
from fastmcp import FastMCP, Context
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests

mcp = FastMCP("DataGovIL", dependencies=["requests"])
app: FastAPI = mcp.http_app  # ✅ זה האובייקט של FastAPI

BASE_URL = "https://data.gov.il/api/3"

@mcp.tool()
async def fetch_data(ctx: Context, dataset_name: str, limit: int = 100, offset: int = 0):
    """Get records from dataset by name using CKAN API"""
    await ctx.info(f"Fetching dataset: {dataset_name}")
    res = requests.get(f"{BASE_URL}/action/package_show?id={dataset_name}")
    if res.status_code != 200:
        return {"error": "Dataset not found"}
    
    resources = res.json()["result"].get("resources", [])
    if not resources:
        return {"error": "No resources found in dataset"}
    
    resource_id = resources[0]["id"]
    params = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset
    }
    data_res = requests.get(f"{BASE_URL}/action/datastore_search", params=params)
    data_res.raise_for_status()
    data = data_res.json()
    return data.get("result", {}).get("records", [])

# HTTP route
@app.get("/fetch_data")
async def fetch_data_http(request: Request):
    dataset_name = request.query_params.get("dataset_name")
    limit = int(request.query_params.get("limit", 100))
    offset = int(request.query_params.get("offset", 0))

    if not dataset_name:
        return JSONResponse({"error": "Missing dataset_name"}, status_code=400)
    
    try:
        ctx = Context("http")
        data = await fetch_data(ctx, dataset_name, limit, offset)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

if __name__ == "__main__":
    mcp.run()
