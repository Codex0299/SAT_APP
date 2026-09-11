# debug_routes.py
from fastapi import APIRouter
from fastapi.responses import JSONResponse

def get_debug_router(conn):
    router = APIRouter(prefix="/debug", tags=["Debug"])

    @router.get("/view-table/{table_name}")
    async def view_table(table_name: str, limit: int = 100):
        """View the first N rows of any table directly in your browser."""
        try:
            db = conn.cursor()
            
            # Double quotes handle case-sensitive table names cleanly
            columns = [col[0] for col in db.execute(f'DESCRIBE "{table_name}"').fetchall()]
            rows = db.execute(f'SELECT * FROM "{table_name}" LIMIT {limit}').fetchall()
            
            return {
                "table": table_name,
                "columns": columns,
                "row_count_preview": len(rows),
                "data": [dict(zip(columns, row)) for row in rows]
            }
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={"error": f"Failed to query table '{table_name}': {str(e)}"}
            )

    return router