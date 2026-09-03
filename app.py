import os
import duckdb
from fastapi import FastAPI, File, Request, UploadFile
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
    "MI": {"table": "mi", "type": "parquet"},
    "NSC": {"table": "nsc", "type": "parquet"},
    "SSR": {"table": "ssr", "type": "parquet"},
    "mdm": {"table": "mdm", "type": "csv_folder"},
    "mds": {"table": "mds", "type": "csv_folder"},
    "cp": {"table": "cp", "type": "csv_folder"},
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
    """Strictly ingests data into dedicated DuckDB tables based on diagram requirements."""
    config = DATASET_CONFIG[dataset_key]
    table_name = config["table"]

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    # 1. PARQUET FILES (WFM, SAT COMBINED, FIT COMBINED)
    if config["type"] == "parquet" and file_path:
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')"
        )

    # 2. CSV / FOLDER DUMPS (MDM, MDS, CP)
    elif config["type"] == "csv_folder" and folder_path:
        # Reads all CSVs inside the target folder recursively
        search_path = os.path.join(folder_path, "*.csv")
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{search_path}', ignore_errors=true)"
        )

    # 3. DATE-WISE CSV FOLDERS (LP, DP, BP)
    elif config["type"] == "datewise_csv" and folder_path:
        # DuckDB Hive-partitioning / recursive path globbing for date folders
        search_path = os.path.join(folder_path, "**", "*.csv")
        conn.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{search_path}', filename=true, ignore_errors=true)"
        )


def build_reconciliation_output():
    """Backend Logic: Performs cross-table audit across loaded dumps without exposing SQL to UI."""
    # Example logic joining SAT, FIT, and LP/DP/BP tables
    query = """
      
CREATE or replace TABLE wfm AS
SELECT 
       "Consumer Number" as CONSUMER_NUMBER,
       "SSR_New Meter Number"as METER_NUMBER,
       
FROM ssr
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
      AND api_mdm_status = 'Approve'
    
);


INSERT INTO WFM (CONSUMER_NUMBER, METER_NUMBER)
SELECT
    mi."Consumer Number",
    mi."Consumer Number"
FROM MI
WHERE NOT EXISTS (
    SELECT 1
    FROM WFM
    WHERE WFM.CONSUMER_NUMBER = mi."Consumer Number"
      AND WFM.METER_NUMBER =  mi."Consumer Number"
      AND "API MDM Status" = 'Approve'
);

--- indexing for sat table to optimize deletion queries

CREATE INDEX idx_sat_consumer
ON sat ("consumer number");

CREATE INDEX idx_sat_meter
ON sat ("meter no");

--delete from wfm where consumer number or meter number exists in sat table
DELETE FROM WFM w
WHERE EXISTS (
    SELECT 1
    FROM sat s
    WHERE s."consumer number" = w.CONSUMER_NUMBER
)
OR EXISTS (
    SELECT 1
    FROM sat s
    WHERE s."meter no" = w.METER_NUMBER
);
 --- indexing for fit table to optimize deletion queries
CREATE INDEX idx_fit_consumer
ON fit ("consumer number");

CREATE INDEX idx_fit_meter
ON fit ("meter no");

delete from wfm where consumer number or meter number exists in fit table

DELETE FROM WFM w
WHERE EXISTS (
    SELECT 1
    FROM fit s
    WHERE s."consumer number" = w.CONSUMER_NUMBER
)
OR EXISTS (
    SELECT 1
    FROM sat s
    WHERE s."meter no" = w.METER_NUMBER
);

-- creating index for mdm table to optimize deletion queries
CREATE INDEX idx_mdm_consumer
ON mdm ("ConsumerNumber");

CREATE INDEX idx_mdm_meter
ON mdm ("DeviceSerialNumber");


-- delete from wfm where consumer number and meter number does not exist in mdm table
DELETE FROM WFM w
WHERE not  EXISTS (
    SELECT 1
    FROM mdm s
    WHERE s."ConsumerNumber" = w.CONSUMER_NUMBER

    and  s."DeviceSerialNumber" = w.METER_NUMBER
);


CREATE INDEX idx_mds_consumer
ON mds ("Consumer No");

CREATE INDEX idx_mds_meter
ON mds ("Meter No");


DELETE FROM WFM w
WHERE not  EXISTS (
    SELECT 1
    FROM mds s
    WHERE s."Consumer No" = w.CONSUMER_NUMBER

    and  s."Meter No" = w.METER_NUMBER
);
-- creating index for cp table to optimize deletion queries
CREATE INDEX idx_cp_consumer
ON cp ("consumer_no");

CREATE INDEX idx_cp_meter
ON cp ("meter_no");

-- delete from wfm where consumer number and meter number does not exist in cp table
DELETE FROM WFM w
WHERE not  EXISTS (
    SELECT 1
    FROM cp s
    WHERE s."consumer_no" = w.CONSUMER_NUMBER

    and  s."meter_no" = w.METER_NUMBER
);











    """
    conn.execute(query)
    columns = [
        col[0] for col in conn.execute("DESCRIBE audit_summary").fetchall()
    ]
    rows = conn.execute("SELECT * FROM audit_summary").fetchall()
    return columns, rows


# ==========================================
# 🌐 FASTAPI ROUTES
# ==========================================


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
        temp_filename = f"temp_{file.filename}"
        with open(temp_filename, "wb") as f:
            f.write(await file.read())

        ingest_file_or_folder(dataset_key, file_path=temp_filename)
        os.remove(temp_filename)

    else:
        # Accepts local folder path for CSV / Date-Wise CSV processing
        folder_path = form.get("folder_path")
        ingest_file_or_folder(dataset_key, folder_path=folder_path)

    table_name = config["table"]
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    return f"""
    <div class="text-xs text-emerald-400 font-semibold">
        ✅ Ingested into table <code>{table_name}</code> ({count:,} records)
    </div>
    """


@app.get("/run-audit", response_class=HTMLResponse)
async def run_audit():
    try:
        columns, rows = build_reconciliation_output()

        header = "".join(
            [
                f'<th class="p-2 border-b border-slate-700 bg-slate-800 text-left">{c}</th>'
                for c in columns
            ]
        )
        body = "".join(
            [
                f"<tr>{''.join([f'<td class=\"p-2 border-b border-slate-800\">{v}</td>' for v in r])}</tr>"
                for r in rows
            ]
        )

        return f"""
        <div class="overflow-x-auto">
            <table class="w-full text-xs text-slate-300 border-collapse">
                <thead><tr class="text-slate-400">{header}</tr></thead>
                <tbody>{body}</tbody>
            </table>
        </div>
        """
    except Exception as e:
        return f'<div class="text-red-400 text-xs p-3">Ingestion incomplete: {str(e)}</div>'
