import hashlib
import os
from pathlib import Path
from typing import Dict, Optional

import duckdb
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import httpx
from starlette.middleware.sessions import SessionMiddleware



# SAT-APP/frontend/my-main-app.py -> parents[0] = frontend, parents[1] = SAT-APP
FRONTEND_DIR = Path(__file__).resolve().parent
SAT_APP_DIR = Path(os.getenv("SAT_APP_ROOT", FRONTEND_DIR.parent))
DATABASE_DIR = Path(os.getenv("DATABASE_DIR", SAT_APP_DIR / "database"))

HTML_PATH = FRONTEND_DIR / "html"
CSS_PATH = FRONTEND_DIR / "css"
JS_PATH = FRONTEND_DIR / "js"
STATIC_PATH = FRONTEND_DIR / "static"

USER_DBS_DIR = Path(os.getenv("DUCKDB_DATA_DIR", DATABASE_DIR / "duckdb_data"))
AUTH_DB_PATH = Path(
    os.getenv("AUTH_DB_PATH", USER_DBS_DIR / "auth_users.duckdb")
).as_posix()
BACKEND_API_URL = os.getenv(
    "BACKEND_API_URL", "http://127.0.0.4:8001").rstrip("/")

app = FastAPI(title="SAT Reconciliation Frontend")


if STATIC_PATH.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_PATH)), name="static")
if CSS_PATH.exists():
    app.mount("/css", StaticFiles(directory=str(CSS_PATH)), name="css")
if JS_PATH.exists():
    app.mount("/js", StaticFiles(directory=str(JS_PATH)), name="js")

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET_KEY",
                         "change-this-secret-key-in-prod"),
)

templates = Jinja2Templates(directory=str(HTML_PATH))

if CSS_PATH.exists():
    app.mount("/css", StaticFiles(directory=str(CSS_PATH)), name="css")
if JS_PATH.exists():
    app.mount("/js", StaticFiles(directory=str(JS_PATH)), name="js")


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def init_auth_db():
    """Creates duckdb_data directory, users registry DB, and default admin."""
    USER_DBS_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(database=AUTH_DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username VARCHAR PRIMARY KEY,
                password_hash VARCHAR NOT NULL,
                role VARCHAR NOT NULL DEFAULT 'user',
                db_filename VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        existing = conn.execute(
            "SELECT COUNT(*) FROM users WHERE username = 'admin'"
        ).fetchone()[0]
        if existing == 0:
            conn.execute(
                "INSERT INTO users (username, password_hash, role, db_filename) VALUES (?, ?, ?, ?)",
                ["admin", hash_password("admin123"),
                 "admin", "user_admin.duckdb"],
            )
    finally:
        conn.close()


init_auth_db()


def get_user_db_path(username: str) -> str:
    safe_user = "".join(c for c in username if c.isalnum() or c in ("_", "-"))
    return (USER_DBS_DIR / f"user_{safe_user}.duckdb").as_posix()


def get_current_user(request: Request) -> Optional[Dict[str, str]]:
    username = request.session.get("username")
    role = request.session.get("role")
    if not username:
        return None
    return {"username": username, "role": role}


# ==========================================
# 🌐 PAGE & AUTH ROUTES
# ==========================================


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="sat.html",
        context={"user": user, "backend_url": BACKEND_API_URL},
    )


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(
        request=request, name="login.html", context={"error": None}
    )


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request, username: str = Form(...), password: str = Form(...)
):
    conn = duckdb.connect(database=AUTH_DB_PATH)
    try:
        user = conn.execute(
            "SELECT username, password_hash, role FROM users WHERE username = ?",
            [username.strip()],
        ).fetchone()
    finally:
        conn.close()

    if not user or user[1] != hash_password(password):
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Invalid username or password."},
        )

    request.session["username"] = user[0]
    request.session["role"] = user[2]

    # Touch/initialize user's isolated disk DuckDB inside database/duckdb_data/
    user_db = duckdb.connect(database=get_user_db_path(user[0]))
    try:
        user_db.execute("PRAGMA memory_limit='2GB';")
    finally:
        user_db.close()

    return RedirectResponse(url="/", status_code=303)


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    current_user = get_current_user(request)
    if not current_user or current_user["role"] != "admin":
        return RedirectResponse(url="/login", status_code=303)

    conn = duckdb.connect(database=AUTH_DB_PATH)
    try:
        users_rows = conn.execute(
            "SELECT username, role, db_filename, created_at FROM users ORDER BY created_at DESC"
        ).fetchall()
    finally:
        conn.close()

    users = []
    for row in users_rows:
        db_file = USER_DBS_DIR / row[2]
        size_mb = (
            round(db_file.stat().st_size / (1024 * 1024), 2)
            if db_file.exists()
            else 0.0
        )
        users.append({
            "username": row[0],
            "role": row[1],
            "db_filename": row[2],
            "created_at": str(row[3])[:19],
            "db_size_mb": size_mb,
        })

    return templates.TemplateResponse(
        request=request,
        name="admin.html",
        context={
            "current_user": current_user,
            "users": users,
            "msg": request.query_params.get("msg"),
            "err": request.query_params.get("err"),
        },
    )


@app.post("/admin/add-user")
async def admin_add_user(
    request: Request,
    new_username: str = Form(...),
    new_password: str = Form(...),
    role: str = Form("user"),
):
    current_user = get_current_user(request)
    if not current_user or current_user["role"] != "admin":
        return RedirectResponse(url="/login", status_code=303)

    clean_user = "".join(
        c for c in new_username.strip() if c.isalnum() or c in ("_", "-")
    )
    if not clean_user or not new_password.strip():
        return RedirectResponse(
            url="/admin?err=Username+and+password+are+required", status_code=303
        )

    db_filename = f"user_{clean_user}.duckdb"
    conn = duckdb.connect(database=AUTH_DB_PATH)
    try:
        exists = conn.execute(
            "SELECT COUNT(*) FROM users WHERE username = ?", [clean_user]
        ).fetchone()[0]
        if exists > 0:
            return RedirectResponse(
                url="/admin?err=User+already+exists", status_code=303
            )
        conn.execute(
            "INSERT INTO users (username, password_hash, role, db_filename) VALUES (?, ?, ?, ?)",
            [clean_user, hash_password(
                new_password.strip()), role, db_filename],
        )
    finally:
        conn.close()

    u_conn = duckdb.connect(database=get_user_db_path(clean_user))
    try:
        u_conn.execute("PRAGMA memory_limit='2GB';")
    finally:
        u_conn.close()

    return RedirectResponse(
        url=f"/admin?msg=User+{clean_user}+created+successfully", status_code=303
    )


@app.post("/admin/delete-user/{target_username}")
async def admin_delete_user(request: Request, target_username: str):
    current_user = get_current_user(request)
    if not current_user or current_user["role"] != "admin":
        return RedirectResponse(url="/login", status_code=303)

    if target_username == "admin":
        return RedirectResponse(
            url="/admin?err=Cannot+delete+primary+admin+account", status_code=303
        )

    conn = duckdb.connect(database=AUTH_DB_PATH)
    try:
        conn.execute("DELETE FROM users WHERE username = ?", [target_username])
    finally:
        conn.close()

    db_file = Path(get_user_db_path(target_username))
    if db_file.exists():
        try:
            db_file.unlink()
        except Exception as e:
            print(f"Warning deleting DB file: {e}")

    return RedirectResponse(
        url=f"/admin?msg=User+{target_username}+and+their+database+removed",
        status_code=303,
    )


# ==========================================
# 🔀 PROXY ROUTES TO BACKEND API (:8001)
# ==========================================


@app.get("/progress/{task_id}")
async def proxy_progress(task_id: str):
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(f"{BACKEND_API_URL}/progress/{task_id}")
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "text/html"),
    )


@app.post("/ingest/{dataset_key}")
async def proxy_ingest(dataset_key: str, request: Request):
    user = get_current_user(request)
    if not user:
        return HTMLResponse(
            '<p class="text-red-400 fs-8">Session expired. Please refresh and log in.</p>'
        )

    form_data = await request.form()
    payload: Dict[str, object] = {"username": user["username"]}
    files = {}

    for key, value in form_data.multi_items():
        if hasattr(value, "filename") and getattr(value, "filename", None):
            files[key] = (
                value.filename,
                await value.read(),
                value.content_type or "application/octet-stream",
            )
        else:
            payload[key] = value

    async with httpx.AsyncClient(timeout=120.0) as client:
        if files:
            resp = await client.post(
                f"{BACKEND_API_URL}/ingest/{dataset_key}",
                data=payload,
                files=files,
            )
        else:
            resp = await client.post(
                f"{BACKEND_API_URL}/ingest/{dataset_key}",
                data=payload,
            )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "text/html"),
    )


@app.post("/run-audit")
async def proxy_run_audit(request: Request):
    user = get_current_user(request)
    if not user:
        return HTMLResponse(
            '<p class="text-red-400 fs-8">Session expired. Please log in.</p>'
        )

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            f"{BACKEND_API_URL}/run-audit",
            data={"username": user["username"]},
        )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "text/html"),
    )


@app.get("/download-csv/{table_key}")
async def proxy_download_csv(request: Request, table_key: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    client = httpx.AsyncClient(timeout=120.0)
    resp = await client.get(
        f"{BACKEND_API_URL}/download-csv/{table_key}",
        params={"username": user["username"]},
    )
    headers = {}
    if "content-disposition" in resp.headers:
        headers["Content-Disposition"] = resp.headers["content-disposition"]

    async def iter_body():
        try:
            yield resp.content
        finally:
            await client.aclose()

    return StreamingResponse(
        iter_body(),
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "text/csv"),
        headers=headers,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("my-main-app:app", host="127.0.0.4", port=8000, reload=False)
