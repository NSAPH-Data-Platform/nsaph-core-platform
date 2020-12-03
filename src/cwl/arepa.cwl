#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: /opt/anaconda3/envs/NSAPHclimate/bin/Rscript

inputs:
  rscript:
    type: File
    inputBinding:
      position: 0
  year:
    type: string
    inputBinding:
      position: 1
      prefix: --year
  output_path:
    type: string
    inputBinding:
      prefix: --out
      position: 2
  parameter_code:
    type: string
    default: "88101"
    inputBinding:
      prefix: --parameter

outputs:
  csv:
    type: File
    outputBinding:
      glob: "*.csv"
