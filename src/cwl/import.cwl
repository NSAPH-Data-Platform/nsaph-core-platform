#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: Workflow

inputs:
  data_file: File
  db_connection_params: File
  db_name:
    type: string
    default: "postgresql"
  PYTHONPATH: string
  force:
    type: boolean
    default: false
  increment:
    type: boolean
    default: false

outputs:
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


steps:
  analyze:
    run: analyze.cwl
    in:
      PYTHONPATH: PYTHONPATH
      data_file: data_file
    out: [table_def, datasource_def, log]

  ingest:
    run: ingest.cwl
    in:
      PYTHONPATH: PYTHONPATH
      table_def: analyze/table_def
      data_file: data_file
      db_connection_params: db_connection_params
      db_name: db_name
      force: force
      increment: increment
    out:
      [log]

  index:
    run: index.cwl
    in:
      PYTHONPATH: PYTHONPATH
      table_def: analyze/table_def
      db_connection_params: db_connection_params
      db_name: db_name
      force: force
      increment: increment
      ingestion_log: ingest/log
    out:
      [log]
