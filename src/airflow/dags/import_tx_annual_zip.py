#!/usr/bin/env python3
from cwl_airflow.extensions.cwldag import CWLDAG

args = {
    "cwl": {
        "debug": True,
        "parallel": True
    }
}

dag = CWLDAG(
    workflow="/opt/projects/nsaph/src/cwl/tx_annual_zip.cwl",
    dag_id="tx_annual_zip_import",
    default_args=args
)