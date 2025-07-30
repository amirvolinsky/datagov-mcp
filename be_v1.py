from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from mcp import get_data
from typing import Optional

def create_app():
    app = FastAPI()

    # אפשר להתאים את הדומיינים אם צריך
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/fetch_data")
    async def fetch_data(
        source: Optional[str] = Query(None),
        filters: Optional[str] = Query(None),
        dimensions: Optional[str] = Query(None),
        metrics: Optional[str] = Query(None),
        group_by: Optional[str] = Query(None),
        order_by: Optional[str] = Query(None),
        limit: Optional[int] = Query(None)
    ):
        try:
            data = get_data(
                source=source,
                filters=filters,
                dimensions=dimensions,
                metrics=metrics,
                group_by=group_by,
                order_by=order_by,
                limit=limit
            )
            return {"status": "success", "data": data}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    return app

app = create_app()
