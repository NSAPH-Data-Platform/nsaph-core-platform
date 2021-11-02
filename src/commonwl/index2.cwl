#!/usr/bin/env cwl-runner

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.index]

inputs:
  PYTHONPATH: string
  depends_on:
    type: File
    default:
  table_def:
    type: File
    inputBinding:
      prefix: --tdef
  db_connection_params:
    type: File
    inputBinding:
      prefix: --db
  db_name:
    type: string
    inputBinding:
      prefix: --section
  force:
    type: boolean
    inputBinding:
      prefix: --force
  increment:
    type: boolean
    default: false
    inputBinding:
      prefix: --incremental


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"

