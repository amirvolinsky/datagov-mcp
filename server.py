from fastmcp import FastMCP, Context
from fastapi.responses import JSONResponse
from fastapi import Request
import requests

# Create the MCP instance
mcp = FastMCP("DataGovIL", dependencies=["requests"])
app = mcp.http_app  # ✅ Use the correct FastAPI app reference

BASE_URL = "https://data.gov.il/api/3"

@mcp.tool()
async def status_show(ctx: Context):
    await ctx.info("Fetching CKAN status...")
    response = requests.post(f"{BASE_URL}/action/status_show")
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def fetch_data(ctx: Context, dataset_name: str, limit: int = 100, offset: int = 0):
    """Fetch data from public API based on dataset name."""
    await ctx.info(f"Fetching data from dataset: {dataset_name}")
    def find_resource_id(name):
        res = requests.get(f"{BASE_URL}/action/package_show?id={name}")
        if res.status_code == 200:
            data = res.json()
            resources = data['result'].get('resources', [])
            if resources:
                return resources[0]['id']
        return None

    resource_id = find_resource_id(dataset_name)
    if not resource_id:
        return {"error": f"No dataset found for '{dataset_name}'"}

    params = {"resource_id": resource_id, "limit": limit, "offset": offset}
    res = requests.get(f"{BASE_URL}/action/datastore_search", params=params)
    res.raise_for_status()
    data = res.json()

    if data.get("success"):
        return data["result"]["records"]
    return {"error": "Failed to retrieve records"}

# Optional HTTP route to access fetch_data directly
@app.get("/fetch_data")
async def fetch_data_http(request: Request):
    dataset_name = request.query_params.get("dataset_name")
    limit = int(request.query_params.get("limit", 100))
    offset = int(request.query_params.get("offset", 0))
    if not dataset_name:
        return JSONResponse({"error": "Missing dataset_name"}, status_code=400)
    try:
        ctx = Context("http")  # dummy context for logs
        result = await fetch_data(ctx, dataset_name, limit, offset)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# Run the MCP server
if __name__ == "__main__":
    mcp.run()
