import os
import duckdb
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates_path = os.path.join(BASE_DIR, "templates")

app = FastAPI()
templates = Jinja2Templates(directory=templates_path)

# In-memory DuckDB connection
conn = duckdb.connect(database=":memory:")

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


# ==========================================
# 🛠️ BACKEND INGESTION ENGINE
# ==========================================


def ingest_file_or_folder(
    dataset_key: str, file_path: str = None, folder_path: str = None
):
    """Ingests data into dedicated DuckDB tables."""
    config = DATASET_CONFIG[dataset_key]
    table_name = config["table"]

    if folder_path:
        folder_path = folder_path.strip('\'" ').strip().replace("\\", "/")

    if file_path:
        file_path = file_path.strip('\'" ').strip().replace("\\", "/")

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    if config["type"] == "parquet" and file_path:
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
        )
    elif config["type"] == "csv_folder" and folder_path:
        search_path = f"{folder_path}/*.csv"
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{search_path}', ignore_errors=true)"
        )
    elif config["type"] == "datewise_csv" and folder_path:
        search_path = f"{folder_path}/**/*.csv"
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{search_path}', filename=true, ignore_errors=true)"
        )


def build_reconciliation_output():
    """Performs cross-table audit and exports Final_LP, Final_BP, Final_DP tables."""
    query = """
    CREATE OR REPLACE TABLE WFM AS
    SELECT 
        "Consumer Number" AS CONSUMER_NUMBER,
        "SSR_New Meter Number" AS METER_NUMBER
    FROM SSR
    WHERE "MDM Status" = 'Approve'; 

    INSERT INTO WFM (CONSUMER_NUMBER, METER_NUMBER)
    SELECT
        NSC.permanent_consumer_no,
        NSC.new_meter_number
    FROM NSC
    WHERE NOT EXISTS (
        SELECT 1
        FROM WFM
        WHERE WFM.CONSUMER_NUMBER = NSC.permanent_consumer_no
          AND WFM.METER_NUMBER = NSC.new_meter_number
          AND api_MDM_status = 'Approve'
    );

    INSERT INTO WFM (CONSUMER_NUMBER, METER_NUMBER)
    SELECT
        MI."Consumer Number",
        MI."Consumer Number"
    FROM MI
    WHERE NOT EXISTS (
        SELECT 1
        FROM WFM
        WHERE WFM.CONSUMER_NUMBER = MI."Consumer Number"
          AND WFM.METER_NUMBER = MI."Consumer Number"
          AND "API MDM Status" = 'Approve'
    );

    CREATE INDEX idx_sat_consumer ON sat ("consumer number");
    CREATE INDEX idx_sat_meter ON sat ("meter no");

    DELETE FROM WFM w
    WHERE EXISTS (
        SELECT 1 FROM sat s WHERE s."consumer number" = w.CONSUMER_NUMBER
    )
    OR EXISTS (
        SELECT 1 FROM sat s WHERE s."meter no" = w.METER_NUMBER
    );

    CREATE INDEX idx_fit_consumer ON fit ("consumer number");
    CREATE INDEX idx_fit_meter ON fit ("meter no");

    DELETE FROM WFM w
    WHERE EXISTS (
        SELECT 1 FROM fit s WHERE s."consumer number" = w.CONSUMER_NUMBER
    )
    OR EXISTS (
        SELECT 1 FROM fit s WHERE s."meter no" = w.METER_NUMBER
    );

    CREATE INDEX idx_MDM_consumer ON MDM ("ConsumerNumber");
    CREATE INDEX idx_MDM_meter ON MDM ("DeviceSerialNumber");

    DELETE FROM WFM w
    WHERE NOT EXISTS (
        SELECT 1
        FROM MDM s
        WHERE s."ConsumerNumber" = w.CONSUMER_NUMBER
          AND s."DeviceSerialNumber" = w.METER_NUMBER
    );

    CREATE INDEX idx_MDS_consumer ON MDS ("Consumer No");
    CREATE INDEX idx_MDS_meter ON MDS ("Meter No");

    DELETE FROM WFM w
    WHERE NOT EXISTS (
        SELECT 1
        FROM MDS s
        WHERE s."Consumer No" = w.CONSUMER_NUMBER
          AND s."Meter No" = w.METER_NUMBER
    );

    CREATE INDEX idx_CP_consumer ON CP ("consumer_no");
    CREATE INDEX idx_CP_meter ON CP ("meter_no");

    DELETE FROM WFM w
    WHERE NOT EXISTS (
        SELECT 1
        FROM CP s
        WHERE s."consumer_no" = w.CONSUMER_NUMBER
          AND s."meter_no" = w.METER_NUMBER
    );

    CREATE OR REPLACE TABLE LP_clean AS
    SELECT *, REGEXP_REPLACE(devicename, '^(ISK|LNT)[-_]?', '', 'i') AS meter_no FROM lp;

    CREATE OR REPLACE TABLE DP_clean AS
    SELECT *, REGEXP_REPLACE(devicename, '^(ISK|LNT)[-_]?', '', 'i') AS meter_no FROM dp;

    CREATE OR REPLACE TABLE BP_clean AS
    SELECT *, REGEXP_REPLACE(devicename, '^(ISK|LNT)[-_]?', '', 'i') AS meter_no FROM bp;

    -- Final DP
    CREATE OR REPLACE TABLE Final_DP AS
    WITH dp_unpivoted AS (
        UNPIVOT DP_clean
        ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
        INTO
            NAME date_col
            VALUE val
    ),
    dp_aggregated AS (
        SELECT 
            meter_no,
            type,
            COUNT(DISTINCT date_col) AS EXPECTED,
            SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
        FROM dp_unpivoted
        GROUP BY meter_no, type
    )
    SELECT 
        dp.* EXCLUDE (meter_no),
        COALESCE(a.RECIEVED, 0) AS RECIEVED,
        COALESCE(a.EXPECTED, 0) AS EXPECTED,
        ROUND((COALESCE(a.RECIEVED, 0) * 100.0) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE
    FROM WFM w
    LEFT JOIN DP_clean dp ON w.METER_NUMBER = dp.meter_no
    LEFT JOIN dp_aggregated a ON dp.meter_no = a.meter_no AND dp.type IS NOT DISTINCT FROM a.type;

    -- Final LP
    CREATE OR REPLACE TABLE Final_LP AS
    WITH lp_unpivoted AS (
        UNPIVOT LP_clean
        ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
        INTO
            NAME date_col
            VALUE val
    ),
    lp_aggregated AS (
        SELECT 
            meter_no,
            COUNT(DISTINCT date_col) * 48 AS EXPECTED,
            SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
        FROM lp_unpivoted
        GROUP BY meter_no
    )                                     
    SELECT 
        lp.* EXCLUDE (meter_no),
        COALESCE(a.RECIEVED, 0) AS RECIEVED,
        COALESCE(a.EXPECTED, 0) AS EXPECTED,
        ROUND((COALESCE(a.RECIEVED, 0) * 100.0) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE
    FROM WFM w
    LEFT JOIN LP_clean lp ON w.METER_NUMBER = lp.meter_no
    LEFT JOIN lp_aggregated a ON lp.meter_no = a.meter_no;

    -- Final BP
    CREATE OR REPLACE TABLE Final_BP AS
    WITH bp_unpivoted AS (
        UNPIVOT BP_clean
        ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
        INTO
            NAME date_col
            VALUE val
    ),
    bp_aggregated AS (
        SELECT 
            meter_no,
            COUNT(DISTINCT date_col) AS EXPECTED,
            SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
        FROM bp_unpivoted
        GROUP BY meter_no
    )
    SELECT 
        bp.* EXCLUDE (meter_no),
        COALESCE(a.RECIEVED, 0) AS RECIEVED,
        COALESCE(a.EXPECTED, 0) AS EXPECTED,
        ROUND((COALESCE(a.RECIEVED, 0) * 100.0) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE
    FROM WFM w
    LEFT JOIN BP_clean bp ON w.METER_NUMBER = bp.meter_no
    LEFT JOIN bp_aggregated a ON bp.meter_no = a.meter_no;
    """

    conn.execute(query)

    # Export audit summaries to CSV files for download
    exports = {
        "Final_LP": "final_lp.csv",
        "Final_BP": "final_bp.csv",
        "Final_DP": "final_dp.csv",
    }

    for table, filename in exports.items():
        output_path = os.path.join(BASE_DIR, filename)
        conn.execute(f"COPY {table} TO '{output_path}' (HEADER, DELIMITER ',')")

    columns = [col[0] for col in conn.execute("DESCRIBE Final_DP").fetchall()]
    rows = conn.execute("SELECT * FROM Final_DP LIMIT 100").fetchall()
    return columns, rows





@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.post("/ingest/{dataset_key}", response_class=HTMLResponse)
async def handle_ingestion(dataset_key: str, request: Request):
    form = await request.form()

    if dataset_key not in DATASET_CONFIG:
        return f'<p class="text-red-400">Invalid Dataset Key</p>'

    config = DATASET_CONFIG[dataset_key]

    if config["type"] == "parquet":
        file = form.get("file")
        if not file or not file.filename:
            return f'<p class="text-amber-300 text-xs">No file uploaded.</p>'

        temp_filename = f"temp_{file.filename}"
        with open(temp_filename, "wb") as f:
            f.write(await file.read())

        ingest_file_or_folder(dataset_key, file_path=temp_filename)
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

    else:
        folder_path = form.get("folder_path")
        if not folder_path:
            return f'<p class="text-amber-300 text-xs">Folder path is missing.</p>'
        ingest_file_or_folder(dataset_key, folder_path=folder_path)

    table_name = config["table"]
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    return f"""
    <div class="text-xs text-[#FFFFE3] bg-[#6D8196]/30 px-2.5 py-1.5 rounded border border-[#6D8196] font-medium">
        ✅ Ingested <code>{table_name}</code> ({count:,} records)
    </div>
    """


@app.get("/run-audit", response_class=HTMLResponse)
async def run_audit():
    try:
        columns, rows = build_reconciliation_output()

        header = "".join(
            [
                f'<th class="p-2 border-b border-[#CBCBCB]/30 bg-[#4A4A4A] text-[#FFFFE3] text-left font-semibold">{c}</th>'
                for c in columns
            ]
        )
        body = "".join(
            [
                f"<tr class=\"hover:bg-[#4A4A4A]/50 transition\">{''.join([f'<td class=\"p-2 border-b border-[#CBCBCB]/20 text-[#FFFFE3]/90\">{v}</td>' for v in r])}</tr>"
                for r in rows
            ]
        )

        return f"""
        <div class="space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3 bg-[#4A4A4A] p-4 rounded-lg border border-[#CBCBCB]">
                <span class="text-xs text-[#FFFFE3] font-semibold tracking-wide">
                    ✨ Reconciliation Audit Complete. Final Outputs Ready for Download:
                </span>
                <div class="flex items-center gap-2">
                    <a href="/download-csv/lp" 
                       class="px-3.5 py-2 bg-[#6D8196] hover:bg-[#6D8196]/80 text-[#FFFFE3] text-xs font-bold rounded-md shadow-sm flex items-center gap-1.5 transition">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
                        </svg>
                        Download LP
                    </a>
                    <a href="/download-csv/bp" 
                       class="px-3.5 py-2 bg-[#6D8196] hover:bg-[#6D8196]/80 text-[#FFFFE3] text-xs font-bold rounded-md shadow-sm flex items-center gap-1.5 transition">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
                        </svg>
                        Download BP
                    </a>
                    <a href="/download-csv/dp" 
                       class="px-3.5 py-2 bg-[#6D8196] hover:bg-[#6D8196]/80 text-[#FFFFE3] text-xs font-bold rounded-md shadow-sm flex items-center gap-1.5 transition">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
                        </svg>
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
    except Exception as e:
        return f'<div class="text-red-300 text-xs p-3 bg-red-950/40 rounded border border-red-500/40">Audit Execution Error: {str(e)}</div>'


@app.get("/download-csv/{table_key}")
async def download_csv(table_key: str):
    valid_tables = {
        "lp": "final_lp.csv",
        "bp": "final_bp.csv",
        "dp": "final_dp.csv",
    }

    if table_key not in valid_tables:
        return HTMLResponse(
            '<p class="text-red-400 text-xs">Invalid export requested.</p>'
        )

    file_name = valid_tables[table_key]
    file_path = os.path.join(BASE_DIR, file_name)

    if not os.path.exists(file_path):
        return HTMLResponse(
            f'<p class="text-red-400 text-xs">File <code>{file_name}</code> not found. Run the audit step first.</p>'
        )

    return FileResponse(
        path=file_path,
        filename=file_name,
        media_type="text/csv",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)