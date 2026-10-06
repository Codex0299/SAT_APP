import glob
import os
from pathlib import Path
from typing import Any, Dict, Union
import uuid

import duckdb
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from routes_debug import get_debug_router
from routes_mds_missing import get_mds_missing_router

# Use Pathlib for reliable cross-platform base path resolution
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_PATH = BASE_DIR / "templates"

app = FastAPI()
templates = Jinja2Templates(directory=str(TEMPLATES_PATH))

# In-memory DuckDB connection
conn = duckdb.connect(database=":memory:")

app.mount(
    "/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static"
)

app.include_router(get_debug_router(conn))
app.include_router(get_mds_missing_router(conn))

# Task progress tracker store
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
    "rf": {"table": "rf", "type": "csv_folder"},
}


def normalize_path(raw_path: Union[str, Path]) -> str:
    """Utility to convert any raw path string or Path object into standard POSIX format for DuckDB."""
    if not raw_path:
        return ""
    clean_path = str(raw_path).strip('\'" ').strip()
    return Path(clean_path).as_posix()


def update_progress(
    task_id: str,
    percent: int,
    status: str,
    result_html: str = "",
    error: str = None,
):
    """Updates global task state for HTMX polling."""
    PROGRESS_STORE[task_id] = {
        "percent": min(percent, 100),
        "status": status,
        "result_html": result_html,
        "completed": percent >= 100 or error is not None,
        "error": error,
    }


def drop_all_indexes(db):
    """Dynamically drops all user-created indexes from DuckDB safely."""
    try:
        indexes = db.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE is_primary = FALSE;"
        ).fetchall()
        for (idx_name,) in indexes:
            if idx_name:
                db.execute(f'DROP INDEX IF EXISTS "{idx_name}";')
    except Exception:
        try:
            indexes = db.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'main';"
            ).fetchall()
            for (idx_name,) in indexes:
                if idx_name:
                    db.execute(f'DROP INDEX IF EXISTS "{idx_name}";')
        except Exception as e:
            print(f"Warning: Could not clear indexes: {e}")


def bg_ingest_file_or_folder(
    task_id: str,
    dataset_key: str,
    file_path: str = None,
    folder_path: str = None,
):
    """Background worker function for file/folder ingestion with stage progress updates."""
    try:
        db = conn.cursor()
        config = DATASET_CONFIG[dataset_key]
        table_name = config["table"]

        norm_folder = normalize_path(folder_path) if folder_path else None
        norm_file = normalize_path(file_path) if file_path else None

        update_progress(
            task_id, 10, f"Preparing target table <code>{table_name}</code>..."
        )
        db.execute(f"DROP TABLE IF EXISTS {table_name}")

        if config["type"] == "parquet" and norm_file:
            update_progress(
                task_id, 40, f"Reading Parquet file for {table_name}..."
            )
            db.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{norm_file}')"
            )
            update_progress(task_id, 90, "Finalizing table structure...")

        elif config["type"] in ["csv_folder", "datewise_csv"] and norm_folder:
            pattern = (
                os.path.join(norm_folder, "*.csv")
                if config["type"] == "csv_folder"
                else os.path.join(norm_folder, "**", "*.csv")
            )
            all_files = glob.glob(
                pattern, recursive=(config["type"] == "datewise_csv")
            )

            total_files = len(all_files)
            if total_files == 0:
                update_progress(
                    task_id,
                    100,
                    "",
                    error=f"No CSV files found in folder path: <code>{norm_folder}</code>",
                )
                return

            update_progress(
                task_id,
                20,
                f"Found {total_files} CSV file(s). Creating base table...",
            )

            for idx, fpath in enumerate(all_files, start=1):
                clean_fpath = normalize_path(fpath)
                pct = int(20 + ((idx / total_files) * 75))
                update_progress(
                    task_id,
                    pct,
                    f"Ingesting file {idx} of {total_files} ({pct}%)...",
                )

                if idx == 1:
                    db.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{clean_fpath}', union_by_name=true, ignore_errors=true)"
                    )
                else:
                    db.execute(
                        f"INSERT INTO {table_name} BY NAME SELECT * FROM read_csv_auto('{clean_fpath}', union_by_name=true, ignore_errors=true)"
                    )

        count = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Success output + HTMX Out-of-Band Swap to set card indicator dot to blue
        success_html = f"""
        <div class="d-flex align-items-center gap-2 fs-8 text-zinc-300 bg-zinc-900 px-2 py-1 rounded border border-zinc-800 vc-mono">
            <span class="text-zinc-500 fw-semibold">state</span>
            <span class="text-blue-400 fw-medium">created {table_name} in memory ({count:,} records)</span>
        </div>
        <!-- Out-Of-Band Swap: Updates card indicator dot to permanent blue -->
        <span id="dot-{dataset_key}" hx-swap-oob="true" class="vc-dot-ok"></span>
        """
        update_progress(task_id, 100, "Done!", result_html=success_html)

    except Exception as e:
        update_progress(task_id, 100, "Error", error=str(e))


def bg_build_reconciliation_output(task_id: str):
    """Executes the reconciliation pipeline with stage progress updates."""
    try:
        db = conn.cursor()

        # Step 1: Drop indexes & Base WFM creation from SSR
        update_progress(
            task_id,
            10,
            "Stage 1/8: Dropping existing indexes & Creating base WFM table"
            " from Approved SSR records...",
        )
        drop_all_indexes(db)

        db.execute("""
            CREATE OR REPLACE TABLE WFM_raw AS
            SELECT 
                "Consumer Number" AS CONSUMER_NUMBER,
                "SSR_New Meter Number" AS METER_NUMBER,
                "Installation Date" AS INSTALLATION_DATE,
                'SSR' as Source,
                "Vendor Approve Status" AS VENDOR_APPROVE_STATUS,
                "Iskraemeco QC Status" AS ISKRAEMECO_QC_STATUS,
                "PESL QC Status" AS PESL_QC_STATUS,
                "UGVCL QC Status" AS UGVCL_QC_STATUS,
                "API 50 Status" AS API_50_STATUS,
                "API 43 Status" AS API_43_49_STATUS,
                
                "MDM Status" AS MDM_STATUS

            FROM SSR;
            
        """)

        # Step 2: Merge NSC and MI records
        update_progress(
            task_id, 25, "Stage 2/8: Merging Approved NSC & MI records into WFM..."
        )
        db.execute("""
            INSERT INTO WFM_raw (CONSUMER_NUMBER, METER_NUMBER, INSTALLATION_DATE, Source,VENDOR_APPROVE_STATUS, ISKRAEMECO_QC_STATUS, PESL_QC_STATUS, UGVCL_QC_STATUS, API_50_STATUS, API_43_49_STATUS, MDM_STATUS)
            SELECT NSC.PERMANENT_CONSUMER_NUMBER, NSC.NEW_METER_NUMBER, NSC.INSTALLATION_DATE as INSTALLATION_DATE, 'NSC' as Source, NSC.VENDOR_APPROVE_STATUS as VENDOR_APPROVE_STATUS, NSC.ISK_STATUS as ISKRAEMECO_QC_STATUS, NSC.PESL_STATUS as PESL_QC_STATUS, NSC.UGVCL_STATUS as UGVCL_QC_STATUS, NSC.API_50_STATUS as API_50_STATUS, NSC.API_49_STATUS as API_43_49_STATUS, NSC.API_MDM_STATUS as MDM_STATUS
            FROM NSC
            WHERE NOT EXISTS (
                SELECT 1 FROM WFM_raw
                WHERE WFM_raw.CONSUMER_NUMBER = NSC.PERMANENT_CONSUMER_NUMBER
                  AND WFM_raw.METER_NUMBER = NSC.NEW_METER_NUMBER
                  -- AND api_MDM_status = 'Approve'
            );

            INSERT INTO WFM_raw (CONSUMER_NUMBER, METER_NUMBER, INSTALLATION_DATE, Source,VENDOR_APPROVE_STATUS, ISKRAEMECO_QC_STATUS, PESL_QC_STATUS, UGVCL_QC_STATUS, API_50_STATUS, API_43_49_STATUS, MDM_STATUS)
            SELECT MI."Consumer Number", MI."New Meter Number", MI."Installation Date" AS INSTALLATION_DATE, 'MI' as Source, MI."Vendor Approve Status" AS VENDOR_APPROVE_STATUS, MI."L1 Status" AS ISKRAEMECO_QC_STATUS, MI."L2 Status" AS PESL_QC_STATUS, MI."L3 Status" AS UGVCL_QC_STATUS, MI."API 50 Status" AS API_50_STATUS, MI."API 43 Status" AS API_43_49_STATUS, MI."API MDM Status" AS MDM_STATUS
            FROM MI
            WHERE NOT EXISTS (
                SELECT 1 FROM WFM_raw
                WHERE WFM_raw.CONSUMER_NUMBER = MI."Consumer Number"
                  AND WFM_raw.METER_NUMBER = MI."New Meter Number"
                 
            );
        """)

        # Step 3: Filter SAT records
        update_progress(
            task_id, 40, "Stage 3/8: Indexing SAT table & removing matching records..."
        )
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sat_consumer ON sat ("consumer number");
            CREATE INDEX IF NOT EXISTS idx_sat_meter ON sat ("meter no");

            DELETE FROM WFM_raw w
            WHERE EXISTS (SELECT 1 FROM sat s WHERE s."consumer number" = w.CONSUMER_NUMBER)
               OR EXISTS (SELECT 1 FROM sat s WHERE s."meter no" = w.METER_NUMBER);
        """)

        # Step 4: Filter FIT records
        update_progress(
            task_id, 50, "Stage 4/8: Indexing FIT table & removing matching records..."
        )
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_fit_consumer ON fit ("consumer number");
            CREATE INDEX IF NOT EXISTS idx_fit_meter ON fit ("meter no");

            DELETE FROM WFM_raw w
            WHERE EXISTS (SELECT 1 FROM fit s WHERE s."consumer number" = w.CONSUMER_NUMBER)
               OR EXISTS (SELECT 1 FROM fit s WHERE s."meter no" = w.METER_NUMBER);
        """)

        # step 4.2 creating table which contains sat eligible but stuck  in some other status
        # db.execute("""create or replace table sat_eligible_stuck as
        # select *,

        # case when api_50_status = 'Reject' then 'API 50 Reject' 
        # when api_50_status = 'Pending' then 'API 50 Pending'
        # when vendor_approve_status = 'Reject' then 'Vendor Reject'
        # when vendor_approve_status = 'Pending' then 'Vendor Pending'
        # when iskraemeco_qc_status = 'Reject' then 'Iskraemeco Reject'
        # when iskraemeco_qc_status = 'Pending' then 'Iskraemeco Pending'
        # when pesl_qc_status = 'Reject' then 'PESL Reject'
        # when pesl_qc_status = 'Pending' then 'PESL Pending'
        # when ugvcl_qc_status = 'Reject' then 'UGVCL Reject'
        # when ugvcl_qc_status = 'Pending' then 'UGVCL Pending'
        # when api_43_49_status = 'Reject' then 'API 43-49 Reject'
        # when api_43_49_status = 'Pending' then 'API 43-49 Pending'
        # when mdm_status = 'Reject' then 'MDM Reject'
        # when mdm_status = 'Pending' then 'MDM Pending'
        # when mdm_status = 'Request' then 'MDM Requested'
        # end as 'Remarks'

        # from WFM_raw

        # --where (VENDOR_APPROVE_STATUS != 'Approve' ) or  (ISKRAEMECO_QC_STATUS != 'Approve') or  (PESL_QC_STATUS != 'Approve') or (UGVCL_QC_STATUS != 'Approve') or  (API_50_STATUS != 'Approve') or (API_43_49_STATUS != 'Approve') or (MDM_STATUS != 'Approve')

        
        
        # """)
        db.execute("""
                    CREATE OR REPLACE TABLE sat_eligible_stuck AS
                    SELECT
                        w.*,

                        mdm_cons.DeviceSerialNumber AS MDM_CONS_TO_METER,
                        mdm_meter.ConsumerNumber AS MDM_METER_TO_CONS,

                        mds_cons."Meter No" AS MDS_CONS_TO_METER,
                        mds_meter."Consumer No" AS MDS_METER_TO_CONS,

                        cp_cons.meter_no AS CP_CONS_TO_METER,
                        cp_meter.consumer_no AS CP_METER_TO_CONS,
                        
                        case when api_50_status = 'Reject' then 'API 50 Reject' 
                                when api_50_status = 'Pending' then 'API 50 Pending'
                                when vendor_approve_status = 'Reject' then 'Vendor Reject'
                                when vendor_approve_status = 'Pending' then 'Vendor Pending'
                                when iskraemeco_qc_status = 'Reject' then 'Iskraemeco Reject'
                                when iskraemeco_qc_status = 'Pending' then 'Iskraemeco Pending'
                                when pesl_qc_status = 'Reject' then 'PESL Reject'
                                when pesl_qc_status = 'Pending' then 'PESL Pending'
                                when ugvcl_qc_status = 'Reject' then 'UGVCL Reject'
                                when ugvcl_qc_status = 'Pending' then 'UGVCL Pending'
                                when api_43_49_status = 'Reject' then 'API 43-49 Reject'
                                when api_43_49_status = 'Pending' then 'API 43-49 Pending'
                                when mdm_status = 'Reject' then 'MDM Reject'
                                when mdm_status = 'Pending' then 'MDM Pending'
                                when mdm_status = 'Request' then 'MDM Requested'
                                when (VENDOR_APPROVE_STATUS = 'Approve' ) and (ISKRAEMECO_QC_STATUS = 'Approve') and (PESL_QC_STATUS = 'Approve') and (UGVCL_QC_STATUS = 'Approve') and (API_50_STATUS = 'Approve') and (API_43_49_STATUS = 'Approve') and (MDM_STATUS = 'Approve')
                                then 'ALL APPROVED'
                                end as 'Remarks',

                        -- Consumer -> Meter validation 
                        CASE
                            WHEN w.METER_NUMBER = mdm_cons.DeviceSerialNumber
                            AND w.METER_NUMBER = mds_cons."Meter No"
                            AND w.METER_NUMBER = cp_cons.meter_no
                            THEN 'ALL MATCH'

                            ELSE CONCAT_WS(', ',
                                CASE
                                    WHEN mdm_cons.DeviceSerialNumber IS NULL
                                        THEN 'MDM NOT FOUND'
                                    WHEN w.METER_NUMBER <> mdm_cons.DeviceSerialNumber
                                        THEN 'MDM NOT MATCH'
                                END,

                                CASE
                                    WHEN mds_cons."Meter No" IS NULL
                                        THEN 'MDS NOT FOUND'
                                    WHEN w.METER_NUMBER <> mds_cons."Meter No"
                                        THEN 'MDS NOT MATCH'
                                END,

                                CASE
                                    WHEN cp_cons.meter_no IS NULL
                                        THEN 'CP NOT FOUND'
                                    WHEN w.METER_NUMBER <> cp_cons.meter_no
                                        THEN 'CP NOT MATCH'
                                END
                            )
                        END AS CONSUMER_REMARK,


                        -- Meter -> Consumer validation
                        CASE
                            WHEN w.CONSUMER_NUMBER = mdm_meter.ConsumerNumber
                            AND w.CONSUMER_NUMBER = mds_meter."Consumer No"
                            AND w.CONSUMER_NUMBER = cp_meter.consumer_no
                            THEN 'ALL MATCH'

                            ELSE CONCAT_WS(', ',
                                CASE
                                    WHEN mdm_meter.ConsumerNumber IS NULL
                                        THEN 'MDM NOT FOUND'
                                    WHEN w. CONSUMER_NUMBER<> mdm_meter.ConsumerNumber
                                        THEN 'MDM NOT MATCH'
                                END,

                                CASE
                                    WHEN mds_meter."Consumer No" IS NULL
                                        THEN 'MDS NOT FOUND'
                                    WHEN w.CONSUMER_NUMBER <> mds_meter."Consumer No"
                                        THEN 'MDS NOT MATCH'
                                END,

                                CASE
                                    WHEN cp_meter.consumer_no IS NULL
                                        THEN 'CP NOT FOUND'
                                    WHEN w.CONSUMER_NUMBER <> cp_meter.consumer_no
                                        THEN 'CP NOT MATCH'
                                END
                            )
                        END AS METER_REMARK

                    FROM WFM_raw w

                    LEFT JOIN MDM mdm_cons
                        ON w.CONSUMER_NUMBER = mdm_cons.ConsumerNumber

                    LEFT JOIN MDM mdm_meter
                        ON w.METER_NUMBER = mdm_meter.DeviceSerialNumber

                    LEFT JOIN MDS mds_cons
                        ON w.CONSUMER_NUMBER = mds_cons."Consumer No"

                    LEFT JOIN MDS mds_meter
                        ON w.METER_NUMBER = mds_meter."Meter No"

                    LEFT JOIN CP cp_cons
                        ON w.CONSUMER_NUMBER = cp_cons.consumer_no

                    LEFT JOIN CP cp_meter
                        ON w.METER_NUMBER = cp_meter.meter_no;
                        
                    DELETE FROM sat_eligible_stuck
                    WHERE CONSUMER_REMARK = 'ALL MATCH'
                    AND METER_REMARK = 'ALL MATCH';
                   
                
                   
                   """)

        db.execute("""
        
        create or replace table WFM as select * from WFM_raw where MDM_STATUS = 'Approve'


        """)

        # Step 5: MDM, MDS, CP validation filtering
        update_progress(
            task_id,
            65,
            "Stage 5/8: Validating against MDM, MDS, and CP master lists...",
        )
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_MDM_consumer ON MDM ("ConsumerNumber");
            CREATE INDEX IF NOT EXISTS idx_MDM_meter ON MDM ("DeviceSerialNumber");
            DELETE FROM WFM w WHERE NOT EXISTS (
                SELECT 1 FROM MDM s WHERE s."ConsumerNumber" = w.CONSUMER_NUMBER AND s."DeviceSerialNumber" = w.METER_NUMBER
            );

            CREATE INDEX IF NOT EXISTS idx_MDS_consumer ON MDS ("Consumer No");
            CREATE INDEX IF NOT EXISTS idx_MDS_meter ON MDS ("Meter No");
            DELETE FROM WFM w WHERE NOT EXISTS (
                SELECT 1 FROM MDS s WHERE s."Consumer No" = w.CONSUMER_NUMBER AND s."Meter No" = w.METER_NUMBER
            );

            CREATE INDEX IF NOT EXISTS idx_CP_consumer ON CP ("consumer_no");
            CREATE INDEX IF NOT EXISTS idx_CP_meter ON CP ("meter_no");
            DELETE FROM WFM w WHERE NOT EXISTS (
                SELECT 1 FROM CP s WHERE s."consumer_no" = w.CONSUMER_NUMBER AND s."meter_no" = w.METER_NUMBER
            );
        """)

        # Step 6: Clean meter serial strings
        update_progress(
            task_id,
            75,
            "Stage 6/8: Cleaning meter device prefixes across LP, DP, BP...",
        )
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
            task_id,
            85,
            "Stage 7/8: Unpivoting date columns and calculating completion ratios...",
        )
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
                   ROUND((COALESCE(a.RECIEVED, 0) * 100) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE, w.INSTALLATION_DATE, w.Source, w.consumer_number as WFM_CONSUMER, w.consumer_number as MDM_CONSUMER, w.consumer_number as MDS_CONSUMER
            FROM WFM w
            LEFT JOIN DP_clean dp ON w.METER_NUMBER = dp.meter_no
            LEFT JOIN dp_aggregated a ON dp.meter_no = a.meter_no AND dp.type IS NOT DISTINCT FROM a.type;

            -- Final LP
            CREATE OR REPLACE TABLE raw_lp AS
            WITH lp_unpivoted AS (
                UNPIVOT LP_clean
                ON COLUMNS('^\\d{4}-\\d{2}-\\d{2}$')
                INTO NAME date_col VALUE val
            ),
            lp_aggregated AS (
                SELECT meter_no, COUNT(DISTINCT date_col) * 48 AS EXPECTED, SUM(TRY_CAST(val AS INTEGER)) AS RECIEVED
                FROM lp_unpivoted GROUP BY meter_no
            )
            SELECT lp.*, COALESCE(a.RECIEVED, 0) AS RECIEVED, COALESCE(a.EXPECTED, 0) AS EXPECTED,
                   ROUND((COALESCE(a.RECIEVED, 0) * 100) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE, w.INSTALLATION_DATE,w.Source, w.consumer_number as WFM_CONSUMER, w.consumer_number as MDM_CONSUMER, w.consumer_number as MDS_CONSUMER
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
                   ROUND((COALESCE(a.RECIEVED, 0) * 100) / NULLIF(a.EXPECTED, 0)) AS PERCENTAGE, w.INSTALLATION_DATE, w.Source, w.consumer_number as WFM_CONSUMER, w.consumer_number as MDM_CONSUMER, w.consumer_number as MDS_CONSUMER
            FROM WFM w
            LEFT JOIN BP_clean bp ON w.METER_NUMBER = bp.meter_no
            LEFT JOIN bp_aggregated a ON bp.meter_no = a.meter_no;

            CREATE OR REPLACE TABLE LP_1 AS 
            SELECT raw_lp.*, MDS."Tariff Code", MDS."Cycle No"
            FROM raw_lp 
            LEFT JOIN MDS ON raw_lp.WFM_CONSUMER = MDS."Consumer No" 
            AND raw_lp.meter_no = MDS."Meter No";

            CREATE OR REPLACE TABLE Final_LP AS 
            SELECT LP_1.*,
              CASE WHEN LP_1.meter_no = rf."meter_serial_number" THEN 'RF' ELSE 'Cellular'
              END AS "Communication Type",
              CASE 
                WHEN LP_1.meter_no LIKE 'US%' THEN '1 Phase'
                WHEN LP_1.meter_no LIKE 'UT%' THEN '3 Phase'
                WHEN LP_1.meter_no LIKE 'UC%' THEN 'LTCT Consumers'
                ELSE 'Unknown'
              END AS "Type"
            FROM LP_1
            LEFT JOIN rf ON LP_1.meter_no = rf."meter_serial_number"
            ORDER BY cluster, WFM_CONSUMER, meter_no;
            
            DELETE FROM sat_eligible_stuck
            WHERE EXISTS (
                SELECT 1
                FROM Final_LP
                WHERE sat_eligible_stuck.CONSUMER_NUMBER = Final_LP.WFM_CONSUMER
                OR sat_eligible_stuck.METER_NUMBER = Final_LP.meter_no
            );
        """)

        # Step 8: Exporting CSV files & Final Cleanup
        update_progress(
            task_id,
            95,
            "Stage 8/8: Exporting audit results to CSV files and cleaning"
            " indexes...",
        )
        exports = {
            "Final_LP": "final_lp.csv",
            "Final_BP": "final_bp.csv",
            "Final_DP": "final_dp.csv",
            "sat_eligible_stuck": "sat_eligible_stuck.csv",
        }
        for table, filename in exports.items():
            output_path = normalize_path(BASE_DIR / filename)
            db.execute(
                f"COPY {table} TO '{output_path}' (HEADER, DELIMITER ',')")

        drop_all_indexes(db)

        temp_tables = ["LP_clean", "DP_clean", "BP_clean", "WFM"]
        for tbl in temp_tables:
            db.execute(f"DROP TABLE IF EXISTS {tbl}")

        db.execute("CHECKPOINT;")

        columns = [
            col[0] for col in db.execute("DESCRIBE Final_DP").fetchall()
        ]
        rows = db.execute("SELECT * FROM Final_DP LIMIT 100").fetchall()

        header = "".join([
            f'<th class="vc-th">{c}</th>'
            for c in columns
        ])
        body = "".join([
            '<tr class="vc-tr">'
            + "".join([
                f'<td class="vc-td">{v}</td>'
                for v in r
            ])
            + "</tr>"
            for r in rows
        ])

        final_html = f"""
        <div class="d-flex flex-column gap-3">
            <div class="d-flex flex-wrap align-items-center justify-content-between gap-3 bg-zinc-900 p-3 rounded border border-zinc-800">
                <span class="fs-8 text-zinc-200 fw-semibold vc-track">
                     Final Outputs Ready for Download:
                </span>
                <div class="d-flex align-items-center gap-2">
                    <a href="/download-csv/lp" class="btn btn-light btn-sm shadow-sm d-inline-flex align-items-center gap-1">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download LP
                    </a>
                    <a href="/download-csv/bp" class="btn btn-light btn-sm shadow-sm d-inline-flex align-items-center gap-1">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download BP
                    </a>
                    <a href="/download-csv/dp" class="btn btn-light btn-sm shadow-sm d-inline-flex align-items-center gap-1">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download DP
                    </a>
                    <a href="/download-csv/sat_eligible_stuck" class="btn btn-light btn-sm shadow-sm d-inline-flex align-items-center gap-1">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
                        Download SAT Eligible Stuck
                    </a>
                </div>
            </div>

            <div class="vc-table-wrap">
                <table class="table table-light table-sm align-middle mb-0 vc-mono fs-8 border-0">
                    <thead><tr>{header}</tr></thead>
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
        return (
            '<p class="text-red-400 fs-8">Task state missing or expired.</p>'
        )

    if task["error"]:
        return f'<div class="text-red-300 fs-8 p-3 bg-red-950-40 rounded border border-red-500-40 vc-mono">Execution Error: {task["error"]}</div>'

    if task["completed"]:
        return task["result_html"]

    pct = task["percent"]
    status_msg = task["status"]

    return f"""
    <div hx-get="/progress/{task_id}" hx-trigger="every 50ms" hx-swap="outerHTML" class="d-flex flex-column gap-2 p-3 bg-zinc-900 border border-zinc-800 rounded">
        <div class="d-flex align-items-center justify-content-between fs-8 text-zinc-300">
            <span class="fw-medium d-flex align-items-center gap-2">
                <svg class="animate-spin w-3-5 h-3-5 text-blue-500" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                {status_msg}
            </span>
            <span class="vc-mono fw-bold text-zinc-100">{pct}%</span>
        </div>
        <div class="vc-progress-track">
            <div class="vc-progress-fill" style="width: {pct}%"></div>
        </div>
    </div>
    """


@app.post("/ingest/{dataset_key}", response_class=HTMLResponse)
async def handle_ingestion(
    dataset_key: str, request: Request, bg_tasks: BackgroundTasks
):
    form = await request.form()

    if dataset_key not in DATASET_CONFIG:
        return '<p class="text-red-400 fs-8">Invalid Dataset Key</p>'

    config = DATASET_CONFIG[dataset_key]
    file_path = form.get("file_path")
    folder_path = form.get("folder_path")
    file = form.get("file")

    task_id = str(uuid.uuid4())

    if config["type"] == "parquet":
        if file_path and str(file_path).strip():
            clean_path = normalize_path(file_path)
            bg_tasks.add_task(
                bg_ingest_file_or_folder,
                task_id,
                dataset_key,
                file_path=clean_path,
            )

        elif file and getattr(file, "filename", None):
            temp_path = BASE_DIR / f"temp_{file.filename}"
            with open(temp_path, "wb") as f:
                f.write(await file.read())
            bg_tasks.add_task(
                bg_ingest_file_or_folder,
                task_id,
                dataset_key,
                file_path=str(temp_path),
            )
        else:
            return (
                '<p class="text-amber-300 fs-8 vc-mono">No file or path'
                " provided.</p>"
            )

    else:
        if not folder_path or not str(folder_path).strip():
            return '<p class="text-amber-300 fs-8 vc-mono">Folder path is missing.</p>'

        clean_folder = normalize_path(folder_path)
        bg_tasks.add_task(
            bg_ingest_file_or_folder,
            task_id,
            dataset_key,
            folder_path=clean_folder,
        )

    update_progress(task_id, 0, "Starting ingestion...")
    return f"""
    <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
    """


@app.post("/run-audit", response_class=HTMLResponse)
async def run_audit(bg_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    update_progress(
        task_id, 0, "Initializing Reconciliation Audit Engine..."
    )
    bg_tasks.add_task(bg_build_reconciliation_output, task_id)

    return f"""
    <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
    """


@app.get("/download-csv/{table_key}")
async def download_csv(table_key: str):
    valid_tables = {
        "lp": "final_lp.csv",
        "bp": "final_bp.csv",
        "dp": "final_dp.csv",
        "sat_eligible_stuck": "sat_eligible_stuck.csv",
    }

    if table_key not in valid_tables:
        return HTMLResponse(
            '<p class="text-red-400 fs-8">Invalid export requested.</p>'
        )

    file_name = valid_tables[table_key]
    file_path = BASE_DIR / file_name

    if not file_path.exists():
        return HTMLResponse(
            f'<p class="text-red-400 fs-8">File <code>{file_name}</code> not'
            " found. Run the audit step first.</p>"
        )

    return FileResponse(
        path=str(file_path),
        filename=file_name,
        media_type="text/csv",
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
