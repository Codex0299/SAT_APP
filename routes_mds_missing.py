"""MDS Missing Report pipeline - additive router for SAT_APP.

Reproduces the SQL logic from `MDS_Missing.json` inside the existing
FastAPI/DuckDB/HTMX patterns. Extends the application with:

  GET  /mds-missing                    -> page (sidebar: "MDS Missing Report")
  POST /mds/ingest/{dataset_key}        -> load a master dump table
  POST /mds/run                         -> run the daily DR-report mapping pipeline
  POST /mds/tracker                     -> re-check the master resolution tracker
  GET  /mds/download/{kind}             -> download a generated CSV

No existing logic in app.py / index.html is changed.
"""

import glob
import os
import uuid
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Master dump tables consumed by the MDS Missing pipeline.
MDS_DUMP_CONFIG = {
    "SSR": {"table": "SSR", "label": "SSR Table", "type": "parquet"},
    "NSC": {"table": "NSC", "label": "NSC Table", "type": "parquet"},
    "MI": {"table": "MI", "label": "MI Table", "type": "parquet"},
    "CRM": {"table": "CRM", "label": "CRM Master","type": "parquet"},
    "MDM": {"table": "MDM", "label": "MDM Master","type":"csv_folder"},
    "PPM_MI": {"table": "PPM_MI", "label": "PPM_MI Master","type":"csv_folder"},
    "WHM": {"table": "WHM", "label": "WHM Store Master","type": "parquet"},
}

# Latest output paths (kind -> absolute file path) for /mds/download/{kind}
MDS_LAST_OUTPUTS = {}

# Only the final date-mapped file is written to disk (and downloadable).
# Intermediate stages now stay in-memory as DuckDB tables.
OUTPUT_FILE_NAMES = {
    "date": "DR-DataReport_Mapped_Final_WHM_Date.csv",
}

TRACKER_FILE_NAMES = {
    "qc_checked": "Master_Resolution_Tracker_QC_Checked.csv",
    "tracker_final": "Master_Resolution_Tracker_Final.csv",
}


# =========================================================================
# SQL STAGES (transcribed from MDS_Missing.json)
# =========================================================================

# Cell 2 -> load the daily DR report
S_LOAD_DR = "CREATE OR REPLACE TABLE MDS_MISSING_BASE AS \nSELECT * FROM read_csv('@@CSV@@', all_varchar = true)"

# Cell 3 -> map WFM modules (SSR / NSC / MI) + QC statuses -> MDS_MAPPED_WFM
S_WFM_QUERY = """
WITH
    /* =========================================================
       1. MASTER TABLES (Grouped by METER NUMBER)
    ========================================================= */
    Unique_SSR AS (
        SELECT
            TRIM("SSR_New Meter Number"::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM("Consumer Number"::VARCHAR), '')) AS wfm_cno,
            MAX(NULLIF(TRIM("Iskraemeco QC Status"::VARCHAR), '')) AS l1_iskra_qc,
            MAX(NULLIF(TRIM("PESL QC Status"::VARCHAR), '')) AS l2_pesl_qc,
            MAX(NULLIF(TRIM("UGVCL QC Status"::VARCHAR), '')) AS l3_ugvcl_qc,
            MAX(NULLIF(TRIM("API 50 Status"::VARCHAR), '')) AS api_50,
            MAX(NULLIF(TRIM("API 43 Status"::VARCHAR), '')) AS api_43_49,
            MAX(NULLIF(TRIM("MDM Status"::VARCHAR), '')) AS mdm_status
        FROM SSR
        WHERE "SSR_New Meter Number" IS NOT NULL
        GROUP BY 1
    ),
    Unique_NSC AS (
        SELECT
            TRIM(new_meter_number::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM(PERMANENT_CONSUMER_NUMBER::VARCHAR), '')) AS wfm_cno,
            MAX(NULLIF(TRIM(vendor_approve_status::VARCHAR), '')) AS vendor_qc,
            MAX(NULLIF(TRIM(ISK_STATUS::VARCHAR), '')) AS l1_iskra_qc,
            MAX(NULLIF(TRIM(PESL_STATUS::VARCHAR), '')) AS l2_pesl_qc,
            MAX(NULLIF(TRIM(UGVCL_STATUS::VARCHAR), '')) AS l3_ugvcl_qc,
            MAX(NULLIF(TRIM(api_50_status::VARCHAR), '')) AS api_50,
            MAX(NULLIF(TRIM(api_49_status::VARCHAR), '')) AS api_43_49,
            MAX(NULLIF(TRIM(api_mdm_status::VARCHAR), '')) AS mdm_status
        FROM NSC
        WHERE new_meter_number IS NOT NULL
        GROUP BY 1
    ),
    Unique_MI AS (
        SELECT
            TRIM("New Meter Number"::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM("Consumer Number"::VARCHAR), '')) AS wfm_cno,
            MAX(NULLIF(TRIM("Vendor Approve Status"::VARCHAR), '')) AS vendor_qc,
            MAX(NULLIF(TRIM("L1 Status"::VARCHAR), '')) AS l1_iskra_qc,
            MAX(NULLIF(TRIM("L2 Status"::VARCHAR), '')) AS l2_pesl_qc,
            MAX(NULLIF(TRIM("L3 Status"::VARCHAR), '')) AS l3_ugvcl_qc,
            MAX(NULLIF(TRIM("API 50 Status"::VARCHAR), '')) AS api_50,
            MAX(NULLIF(TRIM("API 43 Status"::VARCHAR), '')) AS api_43_49,
            MAX(NULLIF(TRIM("API MDM Status"::VARCHAR), '')) AS mdm_status
        FROM MI
        WHERE "New Meter Number" IS NOT NULL
        GROUP BY 1
    ),

    /* =========================================================
       2. LOAD DR BASE FROM YOUR TABLE
    ========================================================= */
    Base_File AS (
        SELECT
            *,
            TRIM("Meter No"::VARCHAR) AS join_meter
        FROM MDS_MISSING_BASE
    ),

    /* =========================================================
       3. MODULE SOURCE IDENTIFICATION & QC STATUS PULL
    ========================================================= */
    Merged_QC AS (
        SELECT
            b.* EXCLUDE (join_meter),

            CASE
                WHEN COALESCE(ssr.wfm_cno, nsc.wfm_cno, mi.wfm_cno) IS NOT NULL
                    THEN COALESCE(ssr.wfm_cno, nsc.wfm_cno, mi.wfm_cno)
                WHEN nsc.meter_no IS NOT NULL
                    THEN 'Consumer number not generated yet'
                ELSE NULL
            END AS "WFM Consumer No",

            CASE
                WHEN ssr.meter_no IS NOT NULL THEN 'SSR'
                WHEN nsc.meter_no IS NOT NULL THEN 'NSC'
                WHEN mi.meter_no  IS NOT NULL THEN 'MI'
                ELSE 'Not Found'
            END AS "Module_Source",

            COALESCE(ssr.api_50, nsc.api_50, mi.api_50, 'Pending') AS sm50_status,
            COALESCE(nsc.vendor_qc, mi.vendor_qc, 'Pending') AS vendor_status,
            COALESCE(ssr.l1_iskra_qc, nsc.l1_iskra_qc, mi.l1_iskra_qc, 'Pending') AS l1_status,
            COALESCE(ssr.l2_pesl_qc, nsc.l2_pesl_qc, mi.l2_pesl_qc, 'Pending') AS l2_status,
            COALESCE(ssr.l3_ugvcl_qc, nsc.l3_ugvcl_qc, mi.l3_ugvcl_qc, 'Pending') AS l3_status,
            COALESCE(ssr.api_43_49, nsc.api_43_49, mi.api_43_49, 'Pending') AS sm43_49_status,
            COALESCE(ssr.mdm_status, nsc.mdm_status, mi.mdm_status, 'Pending') AS mdm_status

        FROM Base_File b
        LEFT JOIN Unique_SSR ssr ON b.join_meter = ssr.meter_no
        LEFT JOIN Unique_NSC nsc ON b.join_meter = nsc.meter_no
        LEFT JOIN Unique_MI  mi  ON b.join_meter = mi.meter_no
    )

    SELECT
        *,

        CASE
            WHEN "WFM Consumer No" IS NULL THEN 'Missing Data in WFM'
            WHEN sm50_status ILIKE '%Reject%' THEN 'Rejected at SM50'
            WHEN sm50_status ILIKE '%Pending%' THEN 'Pending at SM50'
            WHEN vendor_status ILIKE '%Reject%' THEN 'Rejected at Vendor QC'
            WHEN vendor_status ILIKE '%Pending%' THEN 'Pending at Vendor QC'
            WHEN l1_status ILIKE '%Reject%' THEN 'Rejected at L1 QC'
            WHEN l1_status ILIKE '%Pending%' THEN 'Pending at L1 QC'
            WHEN l2_status ILIKE '%Reject%' THEN 'Rejected at L2 QC'
            WHEN l2_status ILIKE '%Pending%' THEN 'Pending at L2 QC'
            WHEN l3_status ILIKE '%Reject%' THEN 'Rejected at L3 QC'
            WHEN l3_status ILIKE '%Pending%' THEN 'Pending at L3 QC'
            WHEN sm43_49_status ILIKE '%Reject%' THEN 'Rejected at SM43/49'
            WHEN sm43_49_status ILIKE '%Pending%' THEN 'Pending at SM43/49'
            WHEN mdm_status ILIKE '%Reject%' THEN 'Rejected at MDM'
            WHEN mdm_status ILIKE '%Pending%' THEN 'Pending at MDM'
            ELSE 'Fully Approved'
        END AS overall_remark

    FROM Merged_QC
"""

# Cell 5 -> map CRM account number & billing cycle -> MDS_MAPPED_CRM
S_CRM_QUERY = """
WITH
    Unique_CRM AS (
        SELECT
            TRIM("Serial Number"::VARCHAR) AS crm_meter_no,
            MAX(NULLIF(TRIM("Account Number"::VARCHAR), '')) AS crm_cno,
            MAX(NULLIF(TRIM("Billing cycle number"::VARCHAR), '')) AS crm_billing_cycle
        FROM CRM
        WHERE "Serial Number" IS NOT NULL
        GROUP BY 1
    ),
    Base_File AS (
        SELECT
            *,
            TRIM("Meter No"::VARCHAR) AS join_meter
        FROM MDS_MAPPED_WFM
    ),
    Merged_CRM AS (
        SELECT
            b.* EXCLUDE (join_meter),
            crm.crm_cno AS "CRM consumer no",
            crm.crm_billing_cycle AS "CRM billing cycle"
        FROM Base_File b
        LEFT JOIN Unique_CRM crm ON b.join_meter = crm.crm_meter_no
    )
    SELECT * FROM Merged_CRM
"""

# Cell 7 -> cross-check MDM & PPM_MI + dynamic consumer gap analysis -> MDS_MAPPED_FINAL
S_MDM_PPM_QUERY = """
WITH
    Unique_MDM AS (
        SELECT
            TRIM("DeviceSerialNumber"::VARCHAR) AS mdm_meter_no,
            MAX(NULLIF(TRIM("ConsumerNumber"::VARCHAR), '')) AS mdm_cno
        FROM MDM
        WHERE "DeviceSerialNumber" IS NOT NULL
        GROUP BY 1
    ),
    Unique_PPM_MI AS (
        SELECT
            TRIM("Meter No"::VARCHAR) AS ppm_meter_no,
            MAX(NULLIF(TRIM("Consumer No"::VARCHAR), '')) AS ppm_cno
        FROM PPM_MI
        WHERE "Meter No" IS NOT NULL
        GROUP BY 1
    ),
    Base_File AS (
        SELECT
            *,
            TRIM("Meter No"::VARCHAR) AS join_meter
        FROM MDS_MAPPED_CRM
    ),
    Merged_MDM_PPM AS (
        SELECT
            b.* EXCLUDE (join_meter),

            mdm.mdm_cno AS "MDM consumer no",
            ppm.ppm_cno AS "PPM_MI consumer no",

            'WFM: ' || COALESCE(b."WFM Consumer No", 'Missing') || ' | ' ||
            'CRM: ' || COALESCE(b."CRM consumer no", 'Missing') || ' | ' ||
            'MDM: ' || COALESCE(mdm.mdm_cno, 'Missing') || ' | ' ||
            'PPM: ' || COALESCE(ppm.ppm_cno, 'Missing') AS consumer_no_diagnostic,

            CASE
                WHEN b."WFM Consumer No" IS NULL AND b."CRM consumer no" IS NULL AND mdm.mdm_cno IS NULL AND ppm.ppm_cno IS NULL
                    THEN 'Missing in All Systems'

                WHEN (b."WFM Consumer No" IS NOT NULL AND b."CRM consumer no" IS NOT NULL AND b."WFM Consumer No" != b."CRM consumer no")
                  OR (b."WFM Consumer No" IS NOT NULL AND mdm.mdm_cno IS NOT NULL AND b."WFM Consumer No" != mdm.mdm_cno)
                  OR (b."WFM Consumer No" IS NOT NULL AND ppm.ppm_cno IS NOT NULL AND b."WFM Consumer No" != ppm.ppm_cno)
                  OR (b."CRM consumer no" IS NOT NULL AND mdm.mdm_cno IS NOT NULL AND b."CRM consumer no" != mdm.mdm_cno)
                  OR (b."CRM consumer no" IS NOT NULL AND ppm.ppm_cno IS NOT NULL AND b."CRM consumer no" != ppm.ppm_cno)
                  OR (mdm.mdm_cno IS NOT NULL AND ppm.ppm_cno IS NOT NULL AND mdm.mdm_cno != ppm.ppm_cno)
                    THEN 'Mismatch Found Between Systems'

                WHEN b."WFM Consumer No" IS NOT NULL AND b."CRM consumer no" IS NOT NULL AND mdm.mdm_cno IS NOT NULL AND ppm.ppm_cno IS NOT NULL
                    THEN 'Perfect Match (All 4 Systems)'

                ELSE 'Missing in: ' || CONCAT_WS(', ',
                        CASE WHEN b."WFM Consumer No" IS NULL THEN 'WFM' ELSE NULL END,
                        CASE WHEN b."CRM consumer no" IS NULL THEN 'CRM' ELSE NULL END,
                        CASE WHEN mdm.mdm_cno IS NULL THEN 'MDM' ELSE NULL END,
                        CASE WHEN ppm.ppm_cno IS NULL THEN 'PPM_MI' ELSE NULL END
                    )
            END AS consumer_gap_remark

        FROM Base_File b
        LEFT JOIN Unique_MDM mdm ON b.join_meter = mdm.mdm_meter_no
        LEFT JOIN Unique_PPM_MI ppm ON b.join_meter = ppm.ppm_meter_no
    )
    SELECT * FROM Merged_MDM_PPM
"""

# Cell 9 -> add Store Name from WHM -> MDS_MAPPED_WHM
S_WHM_QUERY = """
WITH
    Unique_WHM AS (
        SELECT
            TRIM(meter_serial_number::VARCHAR) AS whm_meter_no,
            MAX(NULLIF(TRIM(store_name::VARCHAR), '')) AS store_name
        FROM WHM
        WHERE meter_serial_number IS NOT NULL
        GROUP BY 1
    ),
    Base_File AS (
        SELECT
            *,
            TRIM("Meter No"::VARCHAR) AS join_meter
        FROM MDS_MAPPED_FINAL
    ),
    Merged_WHM AS (
        SELECT
            b."Calculation Status",
            b."Consumer No",
            b."Meter No",

            whm.store_name AS "Store Name",

            b.* EXCLUDE ("Calculation Status", "Consumer No", "Meter No", join_meter)

        FROM Base_File b
        LEFT JOIN Unique_WHM whm ON b.join_meter = whm.whm_meter_no
    )
    SELECT * FROM Merged_WHM
"""

# Cell 11 -> add WFM & CRM installation dates -> write DR-DataReport_Mapped_Final_WHM_Date.csv
S_DATE_MAP = """
COPY (
    WITH
    Unique_SSR_Date AS (
        SELECT
            TRIM("SSR_New Meter Number"::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM("Installation Date"::VARCHAR), '')) AS install_date
        FROM SSR
        WHERE "SSR_New Meter Number" IS NOT NULL
        GROUP BY 1
    ),
    Unique_NSC_Date AS (
        SELECT
            TRIM(new_meter_number::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM(installation_date::VARCHAR), '')) AS install_date
        FROM NSC
        WHERE new_meter_number IS NOT NULL
        GROUP BY 1
    ),
    Unique_MI_Date AS (
        SELECT
            TRIM("New Meter Number"::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM("Installation Date"::VARCHAR), '')) AS install_date
        FROM MI
        WHERE "New Meter Number" IS NOT NULL
        GROUP BY 1
    ),
    Unique_CRM_Date AS (
        SELECT
            TRIM("Serial Number"::VARCHAR) AS crm_meter_no,
            MAX(NULLIF(TRIM("Install Date"::VARCHAR), '')) AS crm_install_date
        FROM CRM
        WHERE "Serial Number" IS NOT NULL
        GROUP BY 1
    ),
    Base_File AS (
        SELECT
            *,
            TRIM("Meter No"::VARCHAR) AS join_meter
        FROM MDS_MAPPED_WHM
    ),
    Merged_Dates AS (
        SELECT
            b."Calculation Status",
            b."Consumer No",
            b."Meter No",
            b."Store Name",
            b."Reading Timestamp",
            b."Active Energy Import (kWh)",
            b."Active Energy Export (kWh)",
            b."Date of Reading",
            b."Created Date",
            b."Monthly Cumulative Readings",
            b."Error Description",
            b."WFM Consumer No",
            b."Module_Source",

            COALESCE(ssr.install_date, nsc.install_date, mi.install_date) AS "WFM installation date",
            crm.crm_install_date AS "CRM installation date",

            b.* EXCLUDE (
                "Calculation Status", "Consumer No", "Meter No", "Store Name",
                "Reading Timestamp", "Active Energy Import (kWh)", "Active Energy Export (kWh)",
                "Date of Reading", "Created Date", "Monthly Cumulative Readings",
                "Error Description", "WFM Consumer No", "Module_Source", join_meter
            )
        FROM Base_File b
        LEFT JOIN Unique_SSR_Date ssr ON b.join_meter = ssr.meter_no
        LEFT JOIN Unique_NSC_Date nsc ON b.join_meter = nsc.meter_no
        LEFT JOIN Unique_MI_Date mi ON b.join_meter = mi.meter_no
        LEFT JOIN Unique_CRM_Date crm ON b.join_meter = crm.crm_meter_no
        
    )
    SELECT * FROM Merged_Dates
        where "Meter No" not like 'UH%' --Remove UH meters from the final output
) TO '@@OUT@@' (HEADER, DELIMITER ',');
"""

# Cell 14 -> re-check resolved meters against WFM modules + CRM
S_TRACKER_QC = """
CREATE OR REPLACE TABLE MASTER_TRACKER_BASE AS
SELECT * FROM read_csv('@@IN@@', all_varchar = true);

COPY (
    WITH
    Unique_SSR AS (
        SELECT
            TRIM("SSR_New Meter Number"::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM("Consumer Number"::VARCHAR), '')) AS wfm_cno,
            MAX(NULLIF(TRIM("Vendor Approve Status"::VARCHAR), '')) AS vendor_qc,
            MAX(NULLIF(TRIM("Iskraemeco QC Status"::VARCHAR), '')) AS l1_iskra_qc,
            MAX(NULLIF(TRIM("PESL QC Status"::VARCHAR), '')) AS l2_pesl_qc,
            MAX(NULLIF(TRIM("UGVCL QC Status"::VARCHAR), '')) AS l3_ugvcl_qc,
            MAX(NULLIF(TRIM("API 50 Status"::VARCHAR), '')) AS api_50,
            MAX(NULLIF(TRIM("API 43 Status"::VARCHAR), '')) AS api_43_49,
            MAX(NULLIF(TRIM("MDM Status"::VARCHAR), '')) AS mdm_status
        FROM SSR
        WHERE "SSR_New Meter Number" IS NOT NULL
        GROUP BY 1
    ),
    Unique_NSC AS (
        SELECT
            TRIM(new_meter_number::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM(PERMANENT_CONSUMER_NUMBER::VARCHAR), '')) AS wfm_cno,
            MAX(NULLIF(TRIM(VENDOR_APPROVE_STATUS::VARCHAR), '')) AS vendor_qc,
            MAX(NULLIF(TRIM(ISK_STATUS::VARCHAR), '')) AS l1_iskra_qc,
            MAX(NULLIF(TRIM(PESL_STATUS::VARCHAR), '')) AS l2_pesl_qc,
            MAX(NULLIF(TRIM(UGVCL_STATUS::VARCHAR), '')) AS l3_ugvcl_qc,
            MAX(NULLIF(TRIM(API_50_STATUS::VARCHAR), '')) AS api_50,
            MAX(NULLIF(TRIM(API_49_STATUS::VARCHAR), '')) AS api_43_49,
            MAX(NULLIF(TRIM(API_MDM_STATUS::VARCHAR), '')) AS mdm_status
        FROM NSC
        WHERE new_meter_number IS NOT NULL
        GROUP BY 1
    ),
    Unique_MI AS (
        SELECT
            TRIM("New Meter Number"::VARCHAR) AS meter_no,
            MAX(NULLIF(TRIM("Consumer Number"::VARCHAR), '')) AS wfm_cno,
            MAX(NULLIF(TRIM("Vendor Approve Status"::VARCHAR), '')) AS vendor_qc,
            MAX(NULLIF(TRIM("L1 Status"::VARCHAR), '')) AS l1_iskra_qc,
            MAX(NULLIF(TRIM("L2 Status"::VARCHAR), '')) AS l2_pesl_qc,
            MAX(NULLIF(TRIM("L3 Status"::VARCHAR), '')) AS l3_ugvcl_qc,
            MAX(NULLIF(TRIM("API 50 Status"::VARCHAR), '')) AS api_50,
            MAX(NULLIF(TRIM("API 43 Status"::VARCHAR), '')) AS api_43_49,
            MAX(NULLIF(TRIM("API MDM Status"::VARCHAR), '')) AS mdm_status
        FROM MI
        WHERE "New Meter Number" IS NOT NULL
        GROUP BY 1
    ),
    Unique_CRM AS (
        SELECT
            TRIM("Serial Number"::VARCHAR) AS crm_meter_no,
            MAX(NULLIF(TRIM("Account Number"::VARCHAR), '')) AS crm_cno
        FROM CRM
        WHERE "Serial Number" IS NOT NULL
        GROUP BY 1
    ),
    Base_File AS (
        SELECT
            *,
            TRIM("Meter No"::VARCHAR) AS join_meter
        FROM MASTER_TRACKER_BASE
    ),
    Merged_QC AS (
        SELECT
            b.* EXCLUDE (join_meter),

            CASE
                WHEN b.Tracking_Status != 'Resolved' THEN 'NA'
                WHEN ssr.meter_no IS NOT NULL THEN 'Found in SSR'
                WHEN nsc.meter_no IS NOT NULL THEN 'Found in NSC'
                WHEN mi.meter_no  IS NOT NULL THEN 'Found in MI'
                ELSE 'Still Not Found in WFM'
            END AS "Latest_WFM_Module",

            CASE
                WHEN b.Tracking_Status != 'Resolved' THEN 'NA'
                WHEN COALESCE(ssr.wfm_cno, nsc.wfm_cno, mi.wfm_cno) IS NOT NULL
                    THEN COALESCE(ssr.wfm_cno, nsc.wfm_cno, mi.wfm_cno)
                WHEN nsc.meter_no IS NOT NULL
                    THEN 'Consumer number not generated yet'
                ELSE NULL
            END AS "WFM Consumer No",

            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.api_50, nsc.api_50, mi.api_50, 'Pending') END AS sm50_status,
            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.vendor_qc, nsc.vendor_qc, mi.vendor_qc, 'Pending') END AS vendor_status,
            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.l1_iskra_qc, nsc.l1_iskra_qc, mi.l1_iskra_qc, 'Pending') END AS l1_status,
            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.l2_pesl_qc, nsc.l2_pesl_qc, mi.l2_pesl_qc, 'Pending') END AS l2_status,
            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.l3_ugvcl_qc, nsc.l3_ugvcl_qc, mi.l3_ugvcl_qc, 'Pending') END AS l3_status,
            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.api_43_49, nsc.api_43_49, mi.api_43_49, 'Pending') END AS sm43_49_status,
            CASE WHEN b.Tracking_Status != 'Resolved' THEN 'NA' ELSE COALESCE(ssr.mdm_status, nsc.mdm_status, mi.mdm_status, 'Pending') END AS mdm_status,

            crm.crm_cno

        FROM Base_File b
        LEFT JOIN Unique_SSR ssr ON b.join_meter = ssr.meter_no
        LEFT JOIN Unique_NSC nsc ON b.join_meter = nsc.meter_no
        LEFT JOIN Unique_MI  mi  ON b.join_meter = mi.meter_no
        LEFT JOIN Unique_CRM crm ON b.join_meter = crm.crm_meter_no
    ),
    Final_Output AS (
        SELECT
            * EXCLUDE (crm_cno),

            CASE
                WHEN Tracking_Status != 'Resolved' THEN 'NA'
                WHEN "Latest_WFM_Module" = 'Still Not Found in WFM' THEN 'Missing Data in WFM'
                WHEN "WFM Consumer No" IS NULL THEN 'Missing Data in WFM'

                WHEN sm50_status ILIKE '%Reject%' THEN 'Rejected at SM50'
                WHEN sm50_status ILIKE '%Pending%' THEN 'Pending at SM50'

                WHEN vendor_status ILIKE '%Reject%' THEN 'Rejected at Vendor QC'
                WHEN vendor_status ILIKE '%Pending%' THEN 'Pending at Vendor QC'

                WHEN l1_status ILIKE '%Reject%' THEN 'Rejected at L1 QC'
                WHEN l1_status ILIKE '%Pending%' THEN 'Pending at L1 QC'

                WHEN l2_status ILIKE '%Reject%' THEN 'Rejected at L2 QC'
                WHEN l2_status ILIKE '%Pending%' THEN 'Pending at L2 QC'

                WHEN l3_status ILIKE '%Reject%' THEN 'Rejected at L3 QC'
                WHEN l3_status ILIKE '%Pending%' THEN 'Pending at L3 QC'

                WHEN sm43_49_status ILIKE '%Reject%' THEN 'Rejected at SM43/49'
                WHEN sm43_49_status ILIKE '%Pending%' THEN 'Pending at SM43/49'

                WHEN mdm_status ILIKE '%Reject%' THEN 'Rejected at MDM'
                WHEN mdm_status ILIKE '%Pending%' THEN 'Pending at MDM'

                ELSE 'Fully Approved'
            END AS "Resolved_QC_Remark",

            CASE
                WHEN Tracking_Status != 'Resolved' THEN 'NA'
                ELSE crm_cno
            END AS "CRM Account Number"

        FROM Merged_QC
    )
    SELECT * FROM Final_Output

) TO '@@OUT@@' (HEADER, DELIMITER ',');
"""

# Cell 15 -> false-positive safety net (Resolved but still missing -> Pending)
S_TRACKER_FINAL = """
COPY (
    WITH
    Tracker AS (
        SELECT *
        FROM read_csv('@@IN@@', all_varchar = true)
    ),
    Corrected_Tracker AS (
        SELECT
            t."Meter No",

            CASE
                WHEN t.Tracking_Status = 'Resolved'
                     AND t."Latest_WFM_Module" = 'Still Not Found in WFM'
                THEN 'Pending'
                ELSE t.Tracking_Status
            END AS Tracking_Status,

            t.* EXCLUDE ("Meter No", Tracking_Status)

        FROM Tracker t
    )
    SELECT * FROM Corrected_Tracker

) TO '@@OUT@@' (HEADER, DELIMITER ',');
"""


# =========================================================================
# HELPERS
# =========================================================================

def _update_progress(task_id, percent, status, result_html="", error=None):
    """Thin wrapper that lazily imports app.update_progress to avoid a
    circular import at module load time."""
    from app import update_progress

    update_progress(task_id, percent, status, result_html=result_html, error=error)


def _normalize_path(raw_path: str) -> str:
    from app import normalize_path

    return normalize_path(raw_path)


def _preview(conn, csv_path: str, limit: int = 100):
    """Fetch columns + first N rows of any CSV via DuckDB."""
    cur = conn.execute(
        f"SELECT * FROM read_csv('{_normalize_path(csv_path)}', all_varchar = true) LIMIT {limit}"
    )
    cols = [c[0] for c in cur.description]
    return cols, cur.fetchall()


def _html_table(cols, rows):
    header = "".join(f'<th class="vc-th">{c}</th>' for c in cols)
    body = "".join(
        '<tr class="vc-tr">'
        + "".join(f'<td class="vc-td">{v}</td>' for v in r)
        + "</tr>"
        for r in rows
    )
    return header, body


def _download_buttons(items):
    """items: list of (kind, label)."""
    buttons = "".join(
        f"""
        <a href="/mds/download/{kind}" class="btn btn-light btn-sm shadow-sm d-inline-flex align-items-center gap-1">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path></svg>
            {label}
        </a>
        """
        for kind, label in items
    )
    return buttons


# =========================================================================
# BACKGROUND WORKERS
# =========================================================================

def bg_mds_ingest(task_id: str, dataset_key: str, raw_path: str):
    """Load a master dump (single parquet/csv file OR folder of CSVs)."""
    try:
        from app import conn

        db = conn.cursor()
        config = MDS_DUMP_CONFIG[dataset_key]
        table_name = config["table"]
        norm_path = _normalize_path(raw_path)

        _update_progress(
            task_id, 10, f"Preparing target table <code>{table_name}</code>..."
        )
        db.execute(f"DROP TABLE IF EXISTS {table_name}")

        if os.path.isdir(raw_path):
            all_files = sorted(glob.glob(os.path.join(raw_path, "*.csv")))
            if not all_files:
                _update_progress(
                    task_id,
                    100,
                    "",
                    error=f"No CSV files found in folder path: <code>{norm_path}</code>",
                )
                return
            _update_progress(
                task_id,
                20,
                f"Found {len(all_files)} CSV file(s). Creating base table...",
            )
            for idx, fpath in enumerate(all_files, start=1):
                clean = _normalize_path(fpath)
                pct = int(20 + ((idx / len(all_files)) * 75))
                _update_progress(
                    task_id,
                    pct,
                    f"Ingesting file {idx} of {len(all_files)} ({pct}%)...",
                )
                if idx == 1:
                    db.execute(
                        f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{clean}', union_by_name=true, ignore_errors=true)"
                    )
                else:
                    db.execute(
                        f"INSERT INTO {table_name} BY NAME SELECT * FROM read_csv_auto('{clean}', union_by_name=true, ignore_errors=true)"
                    )
        elif norm_path.lower().endswith((".parquet", ".pq")):
            _update_progress(
                task_id, 40, f"Reading Parquet file for {table_name}..."
            )
            db.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{norm_path}')"
            )
            _update_progress(task_id, 90, "Finalizing table structure...")
        else:
            _update_progress(
                task_id, 40, f"Reading CSV file for {table_name}..."
            )
            db.execute(
                f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{norm_path}', union_by_name=true, ignore_errors=true)"
            )
            _update_progress(task_id, 90, "Finalizing table structure...")

        count = db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        success_html = f"""
        <div class="d-flex align-items-center gap-2 fs-8 text-zinc-300 bg-zinc-900 px-2 py-1 rounded border border-zinc-800 vc-mono">
            <span class="text-zinc-500 fw-semibold">state</span>
            <span class="text-blue-400 fw-medium">created {table_name} in memory ({count:,} records)</span>
        </div>
        <span id="dot-{dataset_key}" hx-swap-oob="true" class="vc-dot-ok"></span>
        """
        _update_progress(task_id, 100, "Done!", result_html=success_html)

    except Exception as e:
        _update_progress(task_id, 100, "Error", error=str(e))


def bg_mds_pipeline(task_id: str, dr_csv_path: str):
    """Run the daily DR-report mapping pipeline (cells 2->11)."""
    try:
        from app import conn

        db = conn.cursor()
        norm_csv = _normalize_path(dr_csv_path)
        out_dir = _normalize_path(os.path.dirname(os.path.abspath(dr_csv_path)))

        outputs = {
            "date": os.path.join(out_dir, OUTPUT_FILE_NAMES["date"]),
        }
        MDS_LAST_OUTPUTS.update(outputs)

        _update_progress(
            task_id,
            5,
            "Stage 1/6: Loading DR report CSV into <code>MDS_MISSING_BASE</code>...",
        )
        db.execute(S_LOAD_DR.replace("@@CSV@@", norm_csv))

        _update_progress(
            task_id,
            18,
            "Stage 2/6: Mapping WFM modules (SSR / NSC / MI) + QC statuses...",
        )
        db.execute(f"CREATE OR REPLACE TABLE MDS_MAPPED_WFM AS {S_WFM_QUERY}")

        _update_progress(
            task_id,
            38,
            "Stage 3/6: Mapping CRM account number & billing cycle...",
        )
        db.execute(f"CREATE OR REPLACE TABLE MDS_MAPPED_CRM AS {S_CRM_QUERY}")

        _update_progress(
            task_id,
            58,
            "Stage 4/6: Cross-checking MDM & PPM_MI + consumer gap analysis...",
        )
        db.execute(
            f"CREATE OR REPLACE TABLE MDS_MAPPED_FINAL AS {S_MDM_PPM_QUERY}"
        )

        _update_progress(
            task_id,
            76,
            "Stage 5/6: Adding Store Name from WHM...",
        )
        db.execute(f"CREATE OR REPLACE TABLE MDS_MAPPED_WHM AS {S_WHM_QUERY}")

        _update_progress(
            task_id,
            90,
            "Stage 6/6: Adding WFM & CRM installation dates...",
        )
        db.execute(S_DATE_MAP.replace("@@OUT@@", outputs["date"]))

        cols, rows = _preview(db, outputs["date"])

        header, body = _html_table(cols, rows)
        final_html = f"""
        <div class="d-flex flex-column gap-3">
            <div class="d-flex flex-wrap align-items-center justify-content-between gap-3 bg-zinc-900 p-3 rounded border border-zinc-800">
                <span class="fs-8 text-zinc-200 fw-semibold vc-track">
                     MDS Missing Final Output Ready for Download:
                </span>
                <div class="d-flex align-items-center gap-2 flex-wrap">
                    {_download_buttons([("date", "Download Consolidated CSV")])}
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
        _update_progress(task_id, 100, "Done!", result_html=final_html)

    except Exception as e:
        import traceback

        traceback.print_exc()
        _update_progress(task_id, 100, "Error", error=str(e))


# def bg_mds_tracker(task_id: str, tracker_csv_path: str):
#     """Re-check resolved meters (cells 14->15)."""
#     try:
#         from app import conn

#         db = conn.cursor()
#         out_dir = _normalize_path(os.path.dirname(os.path.abspath(tracker_csv_path)))

#         outputs = {
#             kind: os.path.join(out_dir, name)
#             for kind, name in TRACKER_FILE_NAMES.items()
#         }
#         MDS_LAST_OUTPUTS.update(outputs)

#         _update_progress(
#             task_id,
#             10,
#             "Stage 1/2: Re-pulling latest WFM module statuses + CRM for resolved meters...",
#         )
#         db.execute(
#             S_TRACKER_QC.replace("@@IN@@", _normalize_path(tracker_csv_path)).replace(
#                 "@@OUT@@", outputs["qc_checked"]
#             )
#         )

#         _update_progress(
#             task_id,
#             55,
#             "Stage 2/2: Applying false-positive safety net (Resolved -> Pending)...",
#         )
#         db.execute(
#             S_TRACKER_FINAL.replace("@@IN@@", outputs["qc_checked"]).replace(
#                 "@@OUT@@", outputs["tracker_final"]
#             )
#         )

#         cols, rows = _preview(db, outputs["tracker_final"])

#         header, body = _html_table(cols, rows)
#         final_html = f"""
#         <div class="d-flex flex-column gap-3">
#             <div class="d-flex flex-wrap align-items-center justify-content-between gap-3 bg-zinc-900 p-3 rounded border border-zinc-800">
#                 <span class="fs-8 text-zinc-200 fw-semibold vc-track">
#                      Master Resolution Tracker Outputs Ready for Download:
#                 </span>
#                 <div class="d-flex align-items-center gap-2">
#                     {_download_buttons([
#                         ("qc_checked", "QC Checked"),
#                         ("tracker_final", "Final Tracker"),
#                     ])}
#                 </div>
#             </div>
#             <div class="vc-table-wrap">
#                 <table class="table table-light table-sm align-middle mb-0 vc-mono fs-8 border-0">
#                     <thead><tr>{header}</tr></thead>
#                     <tbody>{body}</tbody>
#                 </table>
#             </div>
#         </div>
#         """
#         _update_progress(task_id, 100, "Done!", result_html=final_html)

#     except Exception as e:
#         import traceback

#         traceback.print_exc()
#         _update_progress(task_id, 100, "Error", error=str(e))


# =========================================================================
# ROUTER
# =========================================================================

def get_mds_missing_router(conn):
    router = APIRouter(tags=["MDS Missing"])

    @router.get("/mds-missing", response_class=HTMLResponse)
    async def mds_missing_page(request: Request):
        return templates.TemplateResponse(
            request=request, name="mds_missing.html"
        )

    @router.post("/mds/ingest/{dataset_key}", response_class=HTMLResponse)
    async def mds_ingest(
        dataset_key: str, request: Request, bg_tasks: BackgroundTasks
    ):
        if dataset_key not in MDS_DUMP_CONFIG:
            return '<p class="text-red-400 fs-8">Invalid MDS dataset key</p>'

        form = await request.form()
        raw_path = (form.get("path") or "").strip('\'" ')

        if not raw_path:
            return '<p class="text-amber-300 fs-8 vc-mono">No file or folder path provided.</p>'

        if not os.path.exists(raw_path):
            return (
                f'<p class="text-amber-300 fs-8 vc-mono">Path does not exist:'
                f" <code>{raw_path}</code></p>"
            )

        task_id = str(uuid.uuid4())
        bg_tasks.add_task(bg_mds_ingest, task_id, dataset_key, raw_path)

        _update_progress(task_id, 0, "Starting dump ingestion...")
        return f"""
        <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
        """

    @router.post("/mds/run", response_class=HTMLResponse)
    async def mds_run(request: Request, bg_tasks: BackgroundTasks):
        form = await request.form()
        dr_csv = (form.get("dr_csv") or "").strip('\'" ')

        if not dr_csv:
            return (
                '<p class="text-amber-300 fs-8 vc-mono">DR report CSV path is'
                " missing.</p>"
            )

        if not os.path.exists(dr_csv):
            return (
                f'<p class="text-amber-300 fs-8 vc-mono">DR report not found:'
                f" <code>{dr_csv}</code></p>"
            )

        task_id = str(uuid.uuid4())
        _update_progress(task_id, 0, "Initializing MDS Missing Pipeline...")
        bg_tasks.add_task(bg_mds_pipeline, task_id, dr_csv)

        return f"""
        <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
        """

    @router.post("/mds/tracker", response_class=HTMLResponse)
    async def mds_tracker(request: Request, bg_tasks: BackgroundTasks):
        form = await request.form()
        tracker_csv = (form.get("tracker_csv") or "").strip('\'" ')

        if not tracker_csv:
            return (
                '<p class="text-amber-300 fs-8 vc-mono">Tracker CSV path is'
                " missing.</p>"
            )

        if not os.path.exists(tracker_csv):
            return (
                f'<p class="text-amber-300 fs-8 vc-mono">Tracker file not found:'
                f" <code>{tracker_csv}</code></p>"
            )

        task_id = str(uuid.uuid4())
        _update_progress(task_id, 0, "Initializing Master Resolution Tracker...")
        bg_tasks.add_task(bg_mds_tracker, task_id, tracker_csv)

        return f"""
        <div hx-get="/progress/{task_id}" hx-trigger="load" hx-swap="outerHTML"></div>
        """

    @router.get("/mds/download/{kind}")
    async def mds_download(kind: str):
        all_kinds = {**OUTPUT_FILE_NAMES, **TRACKER_FILE_NAMES}
        if kind not in all_kinds:
            return HTMLResponse(
                '<p class="text-red-400 fs-8">Invalid MDS export requested.</p>'
            )

        file_path = MDS_LAST_OUTPUTS.get(kind)
        if not file_path or not os.path.exists(file_path):
            return HTMLResponse(
                '<p class="text-red-400 fs-8">File not found. Run the pipeline first.</p>'
            )

        return FileResponse(
            path=str(file_path),
            filename=os.path.basename(file_path),
            media_type="text/csv",
        )

    return router