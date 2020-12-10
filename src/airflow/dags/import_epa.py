#!/usr/bin/env python3
from cwl_airflow.extensions.cwldag import CWLDAG

args = {
    "cwl": {
        "debug": True,
        "parallel": True
    }
}

dag = CWLDAG(
    workflow="/opt/projects/nsaph/src/cwl/import_epa.cwl",
    dag_id="epa_import",
    default_args=args
)