#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, -m, nsaph.link_gis]
requirements:
  EnvVarRequirement:
    envDef:
      PYTHONPATH: $(inputs.PYTHONPATH)

inputs:
  depends_on:
    type: Any
    default: "none"
  PYTHONPATH: string
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


outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"
