#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}
  StepInputExpressionRequirement: {}

inputs:
  data_file:
    type: File
    default:
      class: File
      location: "/opt/projects/nsaph/data/adi-download.zip"
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
  link_log:
    type: File
    outputSource: normalize/log
  push_ds_log:
    type: File
    outputSource: push_ds/out
  push_ds_err:
    type: File
    outputSource: push_ds/err



steps:
  analyze:
    run: analyze.cwl
    in:
      PYTHONPATH: PYTHONPATH
      data_file:  data_file
      mapping:
        valueFrom: "fips:fips12"
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
      depends_on: ingest/log
    out:
      [log]

  normalize:
    run: normalize_adi.cwl
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
      depends_on: normalize/log
    out: [out,err]

