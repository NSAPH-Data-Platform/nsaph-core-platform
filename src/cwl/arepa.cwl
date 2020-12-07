#!/usr/bin/env cwl-runner

cwlVersion: v1.0
class: CommandLineTool
baseCommand: [/opt/anaconda3/envs/NSAPHclimate/bin/Rscript, /opt/projects/nsaph/src/r/pm25.R]

requirements:
  EnvVarRequirement:
    envDef:
      HTTPS_PROXY: "http://rcproxy.rc.fas.harvard.edu:3128"
      HTTP_PROXY: "http://rcproxy.rc.fas.harvard.edu:3128"


inputs:
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
