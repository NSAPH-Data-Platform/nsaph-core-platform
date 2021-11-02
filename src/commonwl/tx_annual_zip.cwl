#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}
  InlineJavascriptRequirement: {}

inputs:
  PYTHONPATH: string

outputs:
  analysis_log:
    type: File
    outputSource: import/analysis_log
  ingestion_log:
    type: File
    outputSource: import/ingestion_log
  indexing_log:
    type: File
    outputSource: import/indexing_log


steps:
  import:
    run: import.cwl
    in:
      PYTHONPATH: PYTHONPATH
      data_file:
        valueFrom: |
          ${
            return {
              "class": "File",
              "location": "/Users/misha/harvard/projects/data_server/nsaph/data/pm25_test.csv"
            }
          }
      db_connection_params:
        valueFrom: |
          ${
            return {
              "class": "File",
              "location": "/Users/misha/harvard/projects/database.ini"
            }
          }
      db_name:
        valueFrom: "postgresql10"
      force:
        valueFrom: $(true)
    out: [analysis_log, ingestion_log, indexing_log]
