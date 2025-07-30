# server.py
from fastmcp import FastMCP, Context
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests

# יצירת שרת MCP
mcp = FastMCP("DataGovIL", dependencies=["requests"])
BASE_URL = "https://data.gov.il/api/3"

@mcp.tool()
async def status_show(ctx: Context):
    await ctx.info("Fetching CKAN status...")
    response = requests.post(f"{BASE_URL}/action/status_show")
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def license_list(ctx: Context):
    await ctx.info("Fetching license list...")
    response = requests.get(f"{BASE_URL}/action/license_list")
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def package_list(ctx: Context):
    await ctx.info("Fetching package list...")
    response = requests.get(f"{BASE_URL}/action/package_list")
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def package_search(ctx: Context, q: str = "", fq: str = "", sort: str = "", rows: int = 20, start: int = 0, include_private: bool = False):
    await ctx.info("Searching for packages...")
    params = {
        "q": q, "fq": fq, "sort": sort, "rows": rows, "start": start, "include_private": include_private
    }
    response = requests.get(f"{BASE_URL}/action/package_search", params=params)
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def package_show(ctx: Context, id: str):
    await ctx.info(f"Fetching metadata for package: {id}")
    response = requests.get(f"{BASE_URL}/action/package_show", params={"id": id})
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def organization_list(ctx: Context):
    await ctx.info("Fetching organization list...")
    response = requests.get(f"{BASE_URL}/action/organization_list")
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def organization_show(ctx: Context, id: str):
    await ctx.info(f"Fetching details for organization: {id}")
    response = requests.get(f"{BASE_URL}/action/organization_show", params={"id": id})
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def resource_search(ctx: Context, query: str = "", order_by: str = "", offset: int = 0, limit: int = 100):
    await ctx.info("Searching for resources...")
    params = {"query": query, "order_by": order_by, "offset": offset, "limit": limit}
    response = requests.get(f"{BASE_URL}/action/resource_search", params=params)
    response.raise_for_status()
    return response.json()

@mcp.tool()
async def datastore_search(ctx: Context, resource_id: str, q: str = "", distinct: bool = False, plain: bool = True, limit: int = 100, offset: int = 0, fields: str = "", sort: str = "", include_total: bool = True, records_format: str = "objects"):
    await ctx.info(f"Searching datastore for resource: {resource_id}")
    params = {
        "resource_id": resource_id,
        "q": q,
        "distinct": distinct,
        "plain": plain,
        "limit": limit,
        "offset": offset,
        "fields": fields,
        "sort": sort,
        "include_total": include_total,
        "records_format": records_format
    }
    response = requests.get(f"{BASE_URL}/action/datastore_search", params=params)
    response.raise_for_status()
    return response.json()

# כלי fetch_data
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

# יצירת אפליקציית FastAPI
app = mcp.http_app

# הוספת route ישיר
@app.get("/fetch_data")
async def fetch_data_http(request: Request):
    dataset_name = request.query_params.get("dataset_name")
    limit = int(request.query_params.get("limit", 100))
    offset = int(request.query_params.get("offset", 0))

    if not dataset_name:
        return JSONResponse({"error": "Missing dataset_name"}, status_code=400)

    try:
        records = fetch_data(dataset_name, limit=limit, offset=offset)
        return JSONResponse(records)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# הרצה לוקאלית או בענן
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
