# server.py
from fastmcp import FastMCP, Context
from fastapi import FastAPI
import requests

# Create the MCP server
mcp = FastMCP("DataGovIL", dependencies=["requests"])

# Define your tools
BASE_URL = "https://data.gov.il/api/3"

@mcp.tool()
async def status_show(ctx: Context):
    await ctx.info("Fetching CKAN status...")
    response = requests.post(f"{BASE_URL}/action/status_show")
    response.raise_for_status()
    return response.json()

# ... (שמור את כל שאר הפונקציות שלך בדיוק כמו שהן)

@mcp.tool()
def fetch_data(dataset_name: str, limit: int = 100, offset: int = 0):
    def find_resource_id(dataset_name):
        dataset_url = f"{BASE_URL}/action/package_show?id={dataset_name}"
        response = requests.get(dataset_url)
        if response.status_code == 200:
            dataset_data = response.json()
            resources = dataset_data['result']['resources']
            if resources:
                return resources[0]['id']
        return None

    resource_id = find_resource_id(dataset_name)
    if not resource_id:
        return {"error": f"No dataset found matching '{dataset_name}'"}

    base_url = f"{BASE_URL}/action/datastore_search"
    params = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset
    }
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    api_data = response.json()

    if api_data.get("success"):
        return api_data["result"]["records"]
    else:
        raise Exception(api_data.get("error", "Unknown error occurred"))

# Expose as FastAPI app
app = mcp.http_app

# Uvicorn launch (only when run directly)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
