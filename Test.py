import glob
import os
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Union

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from app import get_user_db_connection


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

PROGRESS_STORE: Dict[str, Dict[str, Any]] = {}

# Master dump tables consumed by the MDS Missing pipeline.
meter_installation_count = {
    "SSR": {"table": "SSR", "label": "SSR Table", "type": "parquet"},
    "NSC": {"table": "NSC", "label": "NSC Table", "type": "parquet"},
    "MI": {"table": "MI", "label": "MI Table", "type": "parquet"},
    "DT_MI": {"table": "DT_MI", "label": "DT_MI Table", "type": "csv"}
}

meter_installation_count_output = {}

OUTPUT_FILE_NAME = {
    "date": "DR-DataReport_Mapped_Final_WHM_Date.csv",
}


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


def bg_run_meter_installation_count(task_id: str, username: str):
    from app import get_user_db_connection, update_progress

    db = None
    try:
        db = get_user_db_connection(username)

        db.execute("""
        CREATE OR REPLACE TABLE WFM AS
            select 
            SSR."SSR_New Meter Number" as Meter_Number,
            SSR."Installation Date" as Inst_date,
            SSR."MDM Status" as MDM,
            'SSR' as "W.O.Type","Circle Name" as Circle,
            "Division Name" as Division
            FROM SSR;
        """)

# Step 2: Merge NSC and MI records
        update_progress(
            task_id, 25, "Stage 2/8: Merging Approved NSC & MI records into WFM..."
        )
        db.execute("""
            insert into wfm (Meter_number,inst_date,MDM,"W.O.Type",Circle,Division)
                select   NSC.new_meter_number as Meter_number,
                         NSC.installation_date as inst_date,
                         NSC.API_MDM_STATUS as MDM,
                         'NSC' as "W.O.Type",
                         NSC.CIRCLE_NAME as Circle,
                         NSC.DIVISION_NAME as Division  
            FROM NSC;

            insert into wfm (Meter_number,inst_date,MDM,"W.O.Type",Circle,Division)
                select   MI."New Meter Number" as Meter_number,
                         MI."Installation Date" as inst_date,
                         MI."API MDM Status" as MDM,
                         'MI' as "W.O.Type",
                         MI."Circle Name" as  Circle,
                         MI."Division Name" as Division
            FROM MI;
            
            insert into wfm (Meter_number,inst_date,MDM,"W.O.Type",Circle,Division)
                select   DT_MI.new_meter_number as Meter_number,
                         DT_MI.installation_date as inst_date,
                         DT_MI.MDM_status as MDM,
                         'DT_MI' as "W.O.Type",
                        circle_name as Circle ,
                        division_name as Division 
            FROM DT_MI;

        """)
        update_progress(
            task_id, 25, "Stage 2/8: Merging Approved NSC,MI & DT_MI records into WFM..."
        )

        db.execute("""

                    ALTER TABLE WFM
                    ADD COLUMN Month VARCHAR; 

                    UPDATE WFM
            SET Month =
                        CASE
                            WHEN Inst_date 
                                BETWEEN DATE '2026-01-01' AND DATE '2026-01-31' THEN 'January'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-02-01' AND DATE '2026-02-28' THEN 'February'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-03-01' AND DATE '2026-03-31' THEN 'March'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-04-01' AND DATE '2026-04-30' THEN 'April'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-05-01' AND DATE '2026-05-31' THEN 'May'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-06-01' AND DATE '2026-06-30' THEN 'June'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-07-01' AND DATE '2026-07-31' THEN 'July'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-08-01' AND DATE '2026-08-31' THEN 'August'
                            WHEN Inst_date 
                                BETWEEN DATE '2026-09-01' AND DATE '2026-09-30' THEN 'September'
                            when Inst_date 
                                BETWEEN DATE '2026-10-01' AND DATE '2026-10-31' THEN 'October' 
                            when Inst_date 
                                BETWEEN DATE '2026-11-01' AND DATE '2026-11-30' THEN 'November'       
                            when Inst_date 
                                BETWEEN DATE '2026-12-01' AND DATE '2026-12-31' THEN 'December'  

                        END;     


               
             
                
                   
                         """)

        db.execute("""

                        create or replace table WFM as select * from WFM where Month is not null;


                        alter table WFM add column Meter_Type varchar ;

                        UPDATE WFM
                        
                        
                        SET Meter_Type =
                                    CASE
                                        WHEN LEFT(Meter_Number, 2) = 'US' THEN 'Single Phase whole current'
                                        WHEN LEFT(Meter_Number, 2) = 'UT' THEN 'Three Phase whole current'
                                        WHEN LEFT(Meter_Number, 2) = 'UC' THEN 'LTCT Consumer Meter'
                                        WHEN LEFT(Meter_Number, 2) = 'UD' THEN 'LTCT DT meter'
                                        WHEN LEFT(Meter_Number, 2) = 'Ud' THEN 'LTCT DT meter'
                                        ELSE 'Unknown'
                                    END;

                        create or replace table WFM as select * from WFM 
                        where MDM = 'Approve';   





        """)

        db.execute(""" 
                        create or replace  table WFM as SELECT Circle,Division,

                        case 
                                when Month = 'January' Then '1'
                                when Month = 'February' Then '2'
                                when Month = 'March' Then '3'
                                when Month = 'April' Then '4'
                                when Month = 'May' Then '5'
                                when Month = 'June' Then '6'
                                when Month = 'July' Then '7'
                                when Month = 'August' Then '8'
                                when Month = 'September' Then '9'
                                when Month = 'October' Then '10'
                                when Month = 'November' Then '11'
                                when Month = 'December' Then '12'
                        end as Month_No,


    Month,

    COUNT(CASE WHEN "W.O.Type" = 'MI' THEN 1 END) AS MI_Count,
    COUNT(CASE WHEN "W.O.Type" = 'NSC' THEN 1 END) AS NSC_Count,
    COUNT(CASE WHEN "W.O.Type" = 'SSR' THEN 1 END) AS SSR_Count,
    COUNT(CASE WHEN "W.O.Type" = 'DT_MI' THEN 1 END) AS DT_MI_Count,

    COUNT(CASE WHEN "W.O.Type"='MI'
        AND Meter_Type='Single Phase whole current'
        THEN 1 END) AS MI_1P,

    COUNT(CASE WHEN "W.O.Type"='MI'
        AND Meter_Type='Three Phase whole current'
        THEN 1 END) AS MI_3P,

    COUNT(CASE WHEN "W.O.Type"='MI'
        AND Meter_Type='LTCT Consumer Meter'
        THEN 1 END) AS MI_LTCT,

    COUNT(CASE WHEN "W.O.Type"='MI'
        AND Meter_Type='LTCT DT meter'
        THEN 1 END) AS MI_DT,

    COUNT(CASE WHEN "W.O.Type"='NSC'
        AND Meter_Type='Single Phase whole current'
        THEN 1 END) AS NSC_1P,

    COUNT(CASE WHEN "W.O.Type"='NSC'
        AND Meter_Type='Three Phase whole current'
        THEN 1 END) AS NSC_3P,

    COUNT(CASE WHEN "W.O.Type"='NSC'
        AND Meter_Type='LTCT Consumer Meter'
        THEN 1 END) AS NSC_LTCT,

    COUNT(CASE WHEN "W.O.Type"='NSC'
        AND Meter_Type='LTCT DT meter'
        THEN 1 END) AS NSC_DT,

    COUNT(CASE WHEN "W.O.Type"='SSR'
        AND Meter_Type='Single Phase whole current'
        THEN 1 END) AS SSR_1P,

    COUNT(CASE WHEN "W.O.Type"='SSR'
        AND Meter_Type='Three Phase whole current'
        THEN 1 END) AS SSR_3P,

    COUNT(CASE WHEN "W.O.Type"='SSR'
        AND Meter_Type='LTCT Consumer Meter'
        THEN 1 END) AS SSR_LTCT,

    COUNT(CASE WHEN "W.O.Type"='SSR'
        AND Meter_Type='LTCT DT meter'
        THEN 1 END) AS SSR_DT,

  
    COUNT(CASE WHEN "W.O.Type"='DT_MI'
        AND Meter_Type='LTCT DT meter'
        THEN 1 END) AS DT_MI_DT

    FROM F1
    --where MDM = 'Approve'
    GROUP BY Circle,Division,Month
    ORDER BY CASE Month
        WHEN 'January' THEN 1
        WHEN 'February' THEN 2
        WHEN 'March' THEN 3
        WHEN 'April' THEN 4
        WHEN 'May' THEN 5
        WHEN 'June' THEN 6
        WHEN 'July' THEN 7
        WHEN 'August' THEN 8
        WHEN 'September' THEN 9
        WHEN 'October' THEN 10
        ELSE 99
    END;

        """)
    except Exception as e:
        update_progress(task_id, 100, "Error", error=str(e))
    finally:
        if db:
            db.close()
