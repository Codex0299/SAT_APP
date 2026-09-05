import os
import glob
import uuid
import duckdb
from typing import Dict, Any
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates_path = os.path.join(BASE_DIR, "templates")

app = FastAPI()
templates = Jinja2Templates(directory=templates_path)

# In-memory DuckDB connection
conn = duckdb.connect(database=":memory:")

# Maximize performance on a 16 GB system
conn.execute("SET max_memory = '3GB'")   # Allocates 4 GB specifically to DuckDB
conn.execute("SET threads = 8")          # Allows multi-threaded execution (adjust based on your CPU cores)
# Task progress tracker store: {task_id: {"percent": int, "status": str, "result_html": str, "completed": bool, "error": str}}
PROGRESS_STORE: Dict[str, Dict[str, Any]] = {}

# Map of exact required datasets and their schema tables
DATASET_CONFIG = {
    "MI": {"table": "MI", "type": "parquet"},
    "NSC": {"table": "NSC", "type": "parquet"},
    "SSR": {"table": "SSR", "type": "parquet"},
    "MDM": {"table": "MDM", "type": "csv_folder"},
    "MDS": {"table": "MDS", "type": "csv_folder"},
    "CP": {"table": "CP", "type": "csv_folder"},
    "sat_combined": {"table": "sat", "type": "parquet"},
    "fit_combined": {"table": "fit", "type": "parquet"},
    "lp": {"table": "lp", "type": "datewise_csv"},
    "dp": {"table": "dp", "type": "datewise_csv"},
    "bp": {"table": "bp", "type": "datewise_csv"},
}


def update_progress(task_id: str, percent: int, status: str, result_html: str = "", error: str = None):
    """Updates global task state for HTMX polling."""
    PROGRESS_STORE[task_id] = {
        "percent": min(percent, 100),
        "status": status,
        "result_html": result_html,
        "completed": percent >= 100 or error is not None,
        "error": error
    }


# ==========================================
# 🛠️ BACKEND INGESTION ENGINE WITH PROGRESS
# ==========================================


def bg_ingest_file_or_folder(task_id: str, dataset_key: str, file_path: str = None, folder_path: str = None):
    """Background worker function for file/folder ingestion with stage progress updates."""
    try:
        db = conn.cursor()
        config = DATASET_CONFIG[dataset_key]
        table_name = config["table"]

        if folder_path:
            folder_path = folder_path.strip('\'" ').strip().replace("\\", "/")
        if file_path:
            file_path = file_path.strip('\'" ').strip().replace("\\", "/")

        update_progress(
            task_id, 10, f"Preparing target table <code>{table_name}</code>...")
        db.execute(f"DROP TABLE IF EXISTS {table_name}")

        if config["type"] == "parquet" and file_path:
            update_progress(
                task_id, 40, f"Reading Parquet file for {table_name}...")
            db.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
            update_progress(task_id, 90, f"Finalizing table structure...")

        elif config["type"] in ["csv_folder", "datewise_csv"] and folder_path:
            pattern = f"{folder_path}/*.csv" if config["type"] == "csv_folder" else f"{folder_path}/**/*.csv"
            all_files = glob.glob(pattern, recursive=(
                config["type"] == "datewise_csv"))

            total_files = len(all_files)
            if total_files == 0:
                update_progress(
                    task_id, 100, "", error=f"No CSV files found in folder path: <code>{folder_path}</code>")
                return

            update_progress(
                task_id, 20, f"Found {total_files} CSV file(s). Creating base table...")

            # Batch process files with union_by_name=true to avoid generating unwanted filename columns
            for idx, fpath in enumerate(all_files, start=1):
                clean_fpath = fpath.replace("\\", "/")
                pct = int(20 + ((idx / total_files) * 75))
                update_progress(
                    task_id, pct, f"Ingesting file {idx} of {total_files} ({pct}%)...")

                if idx == 1:
                    db.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{clean_fpath}', union_by_name=true, ignore_errors=true)")
                else:
                    db.execute(
                        f"INSERT INTO {table_name} BY NAME SELECT * FROM read_csv_auto('{clean_fpath}', union_by_name=true, ignore_errors=true)")

        count = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        success_html = f"""
        <div class="text-xs text-[#FFFFE3] bg-[#6D8196]/30 px-2.5 py-1.5 rounded border border-[#6D8196] font-medium">
            ✅ Ingested <code>{table_name}</code> ({count:,} records)
        </div>
        """
        update_progress(task_id, 100, "Done!", result_html=success_html)

    except Exception as e:
        update_progress(task_id, 100, "Error", error=str(e))


# ==========================================
# 🛠️ RECONCILIATION ENGINE WITH PROGRESS
# ==========================================


def bg_build_reconciliation_output(task_id: str):
    """Executes the reconciliation pipeline with stage progress updates."""
    try:
        db = conn.cursor()

        # Step 1: Base WFM creation from SSR
        update_progress(
            task_id, 10, "Stage 1/8: Creating base WFM table from Approved SSR records...")
        db.execute("""
            CREATE OR REPLACE TABLE WFM AS
            SELECT 
                "Consumer Number" AS CONSUMER_NUMBER,
                "SSR_New Meter Number" AS METER_NUMBER,
                "Installation Date" AS INSTALLATION_DATE,
            FROM SSR
            WHERE "MDM Status" = 'Approve';
        """)

        # Step 2: Merge NSC and MI records
        update_progress(
            task_id, 25, "Stage 2/8: Merging Approved NSC & MI records into WFM...")
        db.execute("""
            INSERT INTO WFM (CONSUMER_NUMBER, METER_NUMBER,INSTALLATION_DATE)
            SELECT NSC.permanent_consumer_no, NSC.new_meter_number , NSC.installation_date as INSTALLATION_DATE
            FROM NSC
            WHERE NOT EXISTS (
                SELECT 1 FROM WFM
                WHERE WFM.CONSUMER_NUMBER = NSC.permanent_consumer_no
                  AND WFM.METER_NUMBER = NSC.new_meter_number
                  AND api_MDM_status = 'Approve'
            );

            INSERT INTO WFM (CONSUMER_NUMBER, METER_NUMBER,INSTALLATION_DATE)
            SELECT MI."Consumer Number", MI."Consumer Number",MI."Installation Date" AS INSTALLATION_DATE
            FROM MI
            WHERE NOT EXISTS (
                SELECT 1 FROM WFM
                WHERE WFM.CONSUMER_NUMBER = MI."Consumer Number"
                  AND WFM.METER_NUMBER = MI."Consumer Number"
                  AND "API MDM Status" = 'Approve'
            );
        """)

        # Step 3: Filter SAT records
        update_progress(
            task_id, 40, "Stage 3/8: Indexing SAT table & removing matching records...")
        db.execute("""
            CREATE INDEX idx_sat_consumer ON sat ("consumer number");
            CREATE INDEX idx_sat_meter ON sat ("meter no");

            DELETE FROM WFM w
            WHERE EXISTS (SELECT 1 FROM sat s WHERE s."consumer number" = w.CONSUMER_NUMBER)
               OR EXISTS (SELECT 1 FROM sat s WHERE s."meter no" = w.METER_NUMBER);
        """)

        # Step 4: Filter FIT records
        update_progress(
            task_id, 50, "Stage 4/8: Indexing FIT table & removing matching records...")
        db.execute("""
            CREATE INDEX idx_fit_consumer ON fit ("consumer number");
            CREATE INDEX idx_fit_meter ON fit ("meter no");

            DELETE FROM WFM w
            WHERE EXISTS (SELECT 1 FROM fit s WHERE s."consumer number" = w.CONSUMER_NUMBER)
               OR EXISTS (SELECT 1 FROM fit s WHERE s."meter no" = w.METER_NUMBER);
        """)

        # Step 5: MDM, MDS, CP validation filtering
        update_progress(
            task_id, 65, "Stage 5/8: Validating against MDM, MDS, and CP master lists...")
        db.execute("""
            CREATE INDEX idx_MDM_consumer ON MDM ("ConsumerNumber");
            CREATE INDEX idx_MDM_meter ON MDM ("DeviceSerialNumber");
            DELETE FROM WFM w WHERE NOT EXISTS (
                SELECT 1 FROM MDM s WHERE s."ConsumerNumber" = w.CONSUMER_NUMBER AND s."DeviceSerialNumber" = w.METER_NUMBER
            );

            CREATE INDEX idx_MDS_consumer ON MDS ("Consumer No");
            CREATE INDEX idx_MDS_meter ON MDS ("Meter No");
            DELETE FROM WFM w WHERE NOT EXISTS (
                SELECT 1 FROM MDS s WHERE s."Consumer No" = w.CONSUMER_NUMBER AND s."Meter No" = w.METER_NUMBER
            );

            CREATE INDEX idx_CP_consumer ON CP ("consumer_no");
            CREATE INDEX idx_CP_meter ON CP ("meter_no");
            DELETE FROM WFM w WHERE NOT EXISTS (
                SELECT 1 FROM CP s WHERE s."consumer_no" = w.CONSUMER_NUMBER AND s."meter_no" = w.METER_NUMBER
            );
        """)

        # Step 6: Clean meter serial strings
        update_progress(
            task_id, 75, "Stage 6/8: Cleaning meter device prefixes across LP, DP, BP...")
        db.execute("""
            CREATE OR REPLACE TABLE LP_clean AS
            SELECT *, REGEXP_REPLACE(devicename, '^(ISK|LNT)[-_]?', '', 'i') AS meter_no FROM lp;

            CREATE OR REPLACE TABLE DP_clean AS
            SELECT *, REGEXP_REPLACE(devicename, '^(ISK|LNT)[-_]?', '', 'i') AS meter_no FROM dp;

            CREATE OR REPLACE TABLE BP_clean AS
            SELECT *, REGEXP_REPLACE(devicename, '^(ISK|LNT)[-_]?', '', 'i') AS meter_no FROM bp;
        """)

        # Step 7: Unpivot and calculate Final DP, LP, BP metrics
        update_progress(
            task_id, 85, "Stage 7/8: Unpivoting date columns and calculating completion ratios...")
        db.execute("""
            -- Final DP
            CREATE OR REPLACE TABLE Final_DP AS
            WITH dp_unpivoted AS (
                UNPIVOT DP_clean
                ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
                INTO NAME date_col VALUE val
            ),
            dp_aggregated AS (
                SELECT meter_no, type, COUNT(DISTINCT date_col) AS EXPECTED, SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
                FROM dp_unpivoted GROUP BY meter_no, type
            )
            SELECT dp.* EXCLUDE (meter_no), COALESCE(a.RECIEVED, 0) AS RECIEVED, COALESCE(a.EXPECTED, 0) AS EXPECTED,
                   ROUND((COALESCE(a.RECIEVED, 0) * 100.0) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE , w.INSTALLATION_DATE
            FROM WFM w
            LEFT JOIN DP_clean dp ON w.METER_NUMBER = dp.meter_no
            LEFT JOIN dp_aggregated a ON dp.meter_no = a.meter_no AND dp.type IS NOT DISTINCT FROM a.type;

            -- Final LP
            CREATE OR REPLACE TABLE Final_LP AS
            WITH lp_unpivoted AS (
                UNPIVOT LP_clean
                ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
                INTO NAME date_col VALUE val
            ),
            lp_aggregated AS (
                SELECT meter_no, COUNT(DISTINCT date_col) * 48 AS EXPECTED, SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
                FROM lp_unpivoted GROUP BY meter_no
            )                                         
            SELECT lp.* EXCLUDE (meter_no), COALESCE(a.RECIEVED, 0) AS RECIEVED, COALESCE(a.EXPECTED, 0) AS EXPECTED,
                   ROUND((COALESCE(a.RECIEVED, 0) * 100.0) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE,w.INSTALLATION_DATE
            FROM WFM w
            LEFT JOIN LP_clean lp ON w.METER_NUMBER = lp.meter_no
            LEFT JOIN lp_aggregated a ON lp.meter_no = a.meter_no;

            -- Final BP
            CREATE OR REPLACE TABLE Final_BP AS
            WITH bp_unpivoted AS (
                UNPIVOT BP_clean
                ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
                INTO NAME date_col VALUE val
            ),
            bp_aggregated AS (
                SELECT meter_no, COUNT(DISTINCT date_col) AS EXPECTED, SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
                FROM bp_unpivoted GROUP BY meter_no
            )
            SELECT bp.* EXCLUDE (meter_no), COALESCE(a.RECIEVED, 0) AS RECIEVED, COALESCE(a.EXPECTED, 0) AS EXPECTED,
                   ROUND((COALESCE(a.RECIEVED, 0) * 100.0) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE , w.INSTALLATION_DATE
            FROM WFM w
            LEFT JOIN BP_clean bp ON w.METER_NUMBER = bp.meter_no
            LEFT JOIN bp_aggregated a ON bp.meter_no = a.meter_no;
        """)

        # Step 8: Exporting CSV files
        update_progress(
            task_id, 95, "Stage 8/8: Exporting audit results to CSV files...")
        exports = {
            "Final_LP": "final_lp.csv",
            "Final_BP": "final_bp.csv",
            "Final_DP": "final_dp.csv",
        }
        for table, filename in exports.items():
            output_path = os.path.join(BASE_DIR, filename)
            db.execute(
                f"COPY {table} TO '{output_path}' (HEADER, DELIMITER ',')")

            temp_tables = ["LP_clean", "DP_clean", "BP_clean", "WFM"]
        for tbl in temp_tables:
            db.execute(f"DROP TABLE IF EXISTS {tbl}")

        # Force DuckDB memory garbage collection
        db.execute("CHECKPOINT;")

        columns = [col[0]
                   for col in db.execute("DESCRIBE Final_DP").fetchall()]
        rows = db.execute("SELECT * FROM Final_DP LIMIT 100").fetchall()

        header = "".join(
            [f'<th class="p-2 border-b border-[#CBCBCB]/30 bg-[#4A4A4A] text-[#FFFFE3] text-left font-semibold">{c}</th>' for c in columns])
        body = "".join(
            [f"<tr class=\"hover:bg-[#4A4A4A]/50 transition\">{''.join([f'<td class=\"p-2 border-b border-[#CBCBCB]/20 text-[#FFFFE3]/90\">{v}</td>' for v in r])}</tr>" for r in rows])

        final_html = f"""
        <div class="space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3 bg-[#4A4A4A] p-4 rounded-lg border border-[#CBCBCB]">
                <span class="text-xs text-[#FFFFE3] font-semibold tracking-wide">
                    ✨ Reconciliation Audit Complete. Final Outputs Ready for Download:
                </span>
                <div class="flex items-center gap-2">
                    <a href="/download-csv/lp" class="px-3.5 py-2 bg-[#6D8196] hover:bg-[#6D8196]/80 text-[#FFFFE3] text-xs font-bold rounded-md shadow-sm flex items-center gap-1.5 transition">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download LP
                    </a>
                    <a href="/download-csv/bp" class="px-3.5 py-2 bg-[#6D8196] hover:bg-[#6D8196]/80 text-[#FFFFE3] text-xs font-bold rounded-md shadow-sm flex items-center gap-1.5 transition">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download BP
                    </a>
                    <a href="/download-csv/dp" class="px-3.5 py-2 bg-[#6D8196] hover:bg-[#6D8196]/80 text-[#FFFFE3] text-xs font-bold rounded-md shadow-sm flex items-center gap-1.5 transition">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download DP
                    </a>
                </div>
            </div>

            <div class="overflow-x-auto max-h-96 border border-[#CBCBCB] rounded-lg bg-[#4A4A4A]">
                <table class="w-full text-xs border-collapse">
                    <thead><tr class="sticky top-0 shadow-sm">{header}</tr></thead>
                    <tbody>{body}</tbody>
                </table>
            </div>
        </div>
        """
        update_progress(task_id, 100, "Done!", result_html=final_html)

    except Exception as e:
        update_progress(task_id, 100, "Error", error=str(e))


# ==========================================
# 🌐 ROUTES & HTMX PROGRESS POLLING
# ==========================================


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/progress/{task_id}", response_class=HTMLResponse)
async def get_progress(task_id: str):
    """HTMX Polling endpoint that returns status/progress bar or final output."""
    task = PROGRESS_STORE.get(task_id)

    if not task:
        return '<p class="text-red-400 text-xs">Task state missing or expired.</p>'

    if task["error"]:
        return f'<div class="text-red-300 text-xs p-3 bg-red-950/40 rounded border border-red-500/40">Execution Error: {task["error"]}</div>'

    if task["completed"]:
        # Return final HTML output when task finishes
        return task["result_html"]

    # Return HTMX progress component with auto-poll trigger
    pct = task["percent"]
    status_msg = task["status"]

    return f"""
    <div hx-get="/progress/{task_id}" hx-trigger="every 50ms" hx-swap="outerHTML" class="space-y-2 p-3 bg-[#4A4A4A]/50 border border-[#6D8196]/40 rounded-lg">
        <div class="flex items-center justify-between text-xs text-[#FFFFE3]">
            <span class="font-medium flex items-center gap-2">
                <svg class="animate-spin h-3.5 w-3.5 text-[#6D8196]" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                {status_msg}
            </span>
            <span class="font-bold">{pct}%</span>
        </div>
        <div class="w-full bg-[#3A3A3A] rounded-full h-2 overflow-hidden border border-[#CBCBCB]/20">
            <div class="bg-[#6D8196] h-2 rounded-full transition-all duration-300 ease-out" style="width: {pct}%"></div>
        </div>
    </div>
    """


@app.post("/ingest/{dataset_key}", response_class=HTMLResponse)
async def handle_ingestion(dataset_key: str, request: Request, bg_tasks: BackgroundTasks):
    form = await request.form()

    if dataset_key not in DATASET_CONFIG:
        return f'<p class="text-red-400">Invalid Dataset Key</p>'

    config = DATASET_CONFIG[dataset_key]
    file_path = form.get("file_path")
    folder_path = form.get("folder_path")
    file = form.get("file")

    task_id = str(uuid.uuid4())

    if config["type"] == "parquet":
        if file_path and str(file_path).strip():
            clean_path = str(file_path).strip('\'" ').replace("\\", "/")
            bg_tasks.add_task(bg_ingest_file_or_folder, task_id,
                              dataset_key, file_path=clean_path)

        elif file and getattr(file, "filename", None):
            temp_filename = f"temp_{file.filename}"
            with open(temp_filename, "wb") as f:
                f.write(await file.read())
            bg_tasks.add_task(bg_ingest_file_or_folder, task_id,
                              dataset_key, file_path=temp_filename)
        else:
            return f'<p class="text-amber-300 text-xs">No file or path provided.</p>'

    else:
        if not folder_path or not str(folder_path).strip():
            return f'<p class="text-amber-300 text-xs">Folder path is missing.</p>'

        clean_folder = str(folder_path).strip('\'" ').replace("\\", "/")
        bg_tasks.add_task(bg_ingest_file_or_folder, task_id,
                          dataset_key, folder_path=clean_folder)

    # Initialize progress store entry & return HTMX progress component
    update_progress(task_id, 0, "Starting ingestion...")
    return f"""
    <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
    """


@app.post("/run-audit", response_class=HTMLResponse)
async def run_audit(bg_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    update_progress(task_id, 0, "Initializing Reconciliation Audit Engine...")
    bg_tasks.add_task(bg_build_reconciliation_output, task_id)

    # Initial trigger component for HTMX polling
    return f"""
    <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
    """


@app.get("/download-csv/{table_key}")
async def download_csv(table_key: str):
    valid_tables = {
        "lp": "final_lp.csv",
        "bp": "final_bp.csv",
        "dp": "final_dp.csv",
    }

    if table_key not in valid_tables:
        return HTMLResponse('<p class="text-red-400 text-xs">Invalid export requested.</p>')

    file_name = valid_tables[table_key]
    file_path = os.path.join(BASE_DIR, file_name)

    if not os.path.exists(file_path):
        return HTMLResponse(f'<p class="text-red-400 text-xs">File <code>{file_name}</code> not found. Run the audit step first.</p>')

    return FileResponse(
        path=file_path,
        filename=file_name,
        media_type="text/csv",
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
