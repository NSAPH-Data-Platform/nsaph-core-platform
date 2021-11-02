#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  InlineJavascriptRequirement: {}
  StepInputExpressionRequirement: {}

inputs:
  year:
    type: string
    default: "1990:2020"
  aggregation:
    type: string
    default: "annual"
  parameter_code:
    type: string
    default: "88101"
  csv_name:
    type: string
  db_connection_params:
    type: File
    default:
      class: File
      location: "/opt/projects/nsaph/database.ini"
  db_name:
    type: string
    default: "postgresql"
  PYTHONPATH:
    type: string
    default: "/opt/projects/nsaph/src/python"
  force:
    type: boolean
    default: false
  increment:
    type: boolean
    default: false

outputs:
  job:
    type: File
    outputSource: echo/job
  data:
    type: File
    outputSource: download/csv
  analysis_log:
    type: File
    outputSource: analyze/log
  table_def:
    type: File
    outputSource: analyze/table_def
  datasource_def:
    type: File
    outputSource: analyze/datasource_def
  ingestion_log:
    type: File
    outputSource: ingest/log
  indexing_log:
    type: File
    outputSource: index/log
  link_log:
    type: File
    outputSource: link/log
  push_ds_log:
    type: File
    outputSource: push_ds/out
  push_ds_err:
    type: File
    outputSource: push_ds/err



steps:
  echo:
    run:
      class: CommandLineTool
      baseCommand: echo
      inputs:
        year:
          type: string
          inputBinding:
            prefix: "year:"
            position: 1
        aggregation:
          type: string
          inputBinding:
            prefix: "aggregation:"
            position: 2
        parameter_code:
          type: string
          inputBinding:
            prefix: "parameter_code:"
            position: 3
        csv_name:
          type: string
          inputBinding:
            prefix: "csv_name:"
            position: 4
      outputs:
        job: stdout
      stdout: job.txt
    in:
      year: year
      aggregation: aggregation
      parameter_code: parameter_code
      csv_name: csv_name
    out: [job]
  download:
    run: arepa.cwl
    in:
      year: year
      aggregation: aggregation
      parameter_code: parameter_code
      output_path: csv_name
    out: [csv]
  analyze:
    run: analyze.cwl
    in:
      PYTHONPATH: PYTHONPATH
      data_file: download/csv
    out: [table_def, datasource_def, log]

  ingest:
    run: ingest.cwl
    in:
      PYTHONPATH: PYTHONPATH
      table_def: analyze/table_def
      data_file: download/csv
      db_connection_params: db_connection_params
      db_name: db_name
      force: force
      increment: increment
    out:
      [log]

  index:
    run: index_obs.cwl
    in:
      PYTHONPATH: PYTHONPATH
      table_def: analyze/table_def
      db_connection_params: db_connection_params
      db_name: db_name
      force: force
      increment: increment
      depends_on: ingest/log
    out:
      [log]

  link:
    run: link_gis.cwl
    in:
      PYTHONPATH: PYTHONPATH
      table_def: analyze/table_def
      db_connection_params: db_connection_params
      db_name: db_name
      depends_on: ingest/log
    out:
      [log]

  push_ds:
    run: push_datasource_def.cwl
    in:
      datasource: analyze/datasource_def
      depends_on: link/log
    out: [out,err]

