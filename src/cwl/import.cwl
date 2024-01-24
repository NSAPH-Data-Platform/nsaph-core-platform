#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: Workflow

requirements:
  SubworkflowFeatureRequirement: {}

inputs:
  data_file:
    type: File
  metadata:
    type: File?
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
    default: "/home/nsaph/projects/nsaph/src/python"
  force:
    type: boolean
    default: false
  increment:
    type: boolean
    default: false
  superset:
    type: boolean
    default: True

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
      data_file: data_file
      metadata: metadata
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

  push_ds:
    #when: $(inputs.superset)
    run: push_datasource_def.cwl
    in:
      datasource: analyze/datasource_def
      depends_on: ingest/log
    out: [out,err]

