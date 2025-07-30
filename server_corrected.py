from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastmcp import FastMCP, Context
import requests

# MCP instance — לשימוש פנימי בלבד
mcp = FastMCP("DataGovIL", dependencies=["requests"])

# זהו FastAPI app אמיתי, לא הפונקציה של MCP!
app = FastAPI()

# MCP Tool — רק לשימוש על ידי MCP
@mcp.tool()
async def fetch_data_tool(ctx: Context, dataset_name: str, limit: int = 100):
    r = requests.get(f"https://data.gov.il/api/3/action/package_show?id={dataset_name}")
    r.raise_for_status()
    result = r.json()["result"]
    resource_id = result["resources"][0]["id"]
    data_r = requests.get(f"https://data.gov.il/api/3/action/datastore_search", params={
        "resource_id": resource_id,
        "limit": limit
    })
    data_r.raise_for_status()
    return data_r.json()["result"]["records"]

# HTTP route — לפנייה מה-UI או מבחוץ
@app.get("/fetch_data")
async def fetch_data_http(request: Request):
    dataset_name = request.query_params.get("dataset_name")
    if not dataset_name:
        return JSONResponse({'error': 'dataset_name is required'}, status_code=400)
    try:
        ctx = Context("http")
        result = await fetch_data_tool(ctx, dataset_name)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({'error': str(e)}, status_code=500)
