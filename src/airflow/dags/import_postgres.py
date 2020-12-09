#!/usr/bin/env python3
from cwl_airflow.extensions.cwldag import CWLDAG

args = {
    "cwl": {
        "debug": True,
        "parallel": True
    }
}

dag = CWLDAG(
    workflow="/opt/projects/nsaph/src/cwl/import.cwl",
    dag_id="postgres_import",
    default_args=args
)