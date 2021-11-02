#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: CommandLineTool
baseCommand: [/opt/anaconda3/envs/NSAPHclimate/bin/Rscript, /opt/projects/nsaph/src/r/download_epa.R]

requirements:
  EnvVarRequirement:
    envDef:
      HTTPS_PROXY: "http://rcproxy.rc.fas.harvard.edu:3128"
      HTTP_PROXY: "http://rcproxy.rc.fas.harvard.edu:3128"


inputs:
  year:
    type: string
    default: "1990:2020"
    inputBinding:
      position: 1
      prefix: --year
  aggregation:
    type: string
    default: "annual"
    inputBinding:
      position: 2
      prefix: --aggregation
  output_path:
    type: string
    inputBinding:
      prefix: --out
      position: 3
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
