#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, -m, nsaph.create]
requirements:
  EnvVarRequirement:
    envDef:
      PYTHONPATH: $(inputs.PYTHONPATH)

inputs:
  PYTHONPATH: string
  table_def:
    type: File
    inputBinding:
      prefix: --tdef
  data_file:
    type: File
    inputBinding:
      prefix: --data
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
      prefix: --increment


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"
