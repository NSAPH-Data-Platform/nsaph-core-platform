#!/usr/bin/env python3
import os

from cwl_airflow.extensions.cwldag import CWLDAG

args = {
    "cwl": {
        "debug": True,
        "parallel": True
    }
}

project_dir=os.getenv("PROJECT_DIR")
assert project_dir
dag = CWLDAG(
    workflow=os.path.join(project_dir, "src", "cwl", "import_adi.cwl"),
    dag_id="ADI_import",
    default_args=args
)