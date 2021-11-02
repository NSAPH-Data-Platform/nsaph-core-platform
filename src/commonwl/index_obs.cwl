#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, -m, nsaph.index]
requirements:
  EnvVarRequirement:
    envDef:
      PYTHONPATH: $(inputs.PYTHONPATH)

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

