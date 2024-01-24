#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [python, -m, nsaph.analyze]
requirements:
  EnvVarRequirement:
    envDef:
      PYTHONPATH: $(inputs.PYTHONPATH)

inputs:
  PYTHONPATH: string
  data_file:
    type: File
    inputBinding:
      prefix: --source
  metadata:
    type: File?
    inputBinding:
      prefix: --metadata
  mapping:
    type: string?
    inputBinding:
      prefix: --map


arguments:
  - valueFrom: "."
    prefix: --outdir

outputs:
  log:
    type: File
    outputBinding:
      glob: "*.log"
  table_def:
    type: File
    outputBinding:
      glob: "*.json"
  datasource_def:
    type: File
    outputBinding:
      glob: "*.yml"

