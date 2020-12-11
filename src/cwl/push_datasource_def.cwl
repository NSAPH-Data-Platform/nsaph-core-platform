#!/usr/bin/env cwl-runner

cwlVersion: v1.1
class: Workflow

inputs:
  datasource:
    type: File

  depends_on:
    type: Any
    default: "none"

outputs:
  out:
    type: File
    outputSource: import/out
  err:
    type: File
    outputSource: import/err

steps:
  copy:
    run:
      requirements:
        InlineJavascriptRequirement: {}
      class: CommandLineTool
      baseCommand: [/usr/bin/docker, cp]
      inputs:
        file:
          type: File
          inputBinding:
            position: 1
            prefix: "--follow-link"
      arguments:
        - valueFrom: "superset_app:/app/superset_home/"
          position: 2
      outputs:
        name:
          type: string
          outputBinding:
            outputEval: $('/app/superset_home/' + inputs.file.basename)
    in:
      file: datasource
    out: [name]

  import:
    run:
      class: CommandLineTool
      baseCommand: [/usr/bin/docker, exec, superset_app, superset, import_datasources]
      inputs:
        datasource:
          type: string
          inputBinding:
            position: 1
            prefix: "-p"
      outputs:
        out: stdout
        err: stderr
      stdout: "ds_out.log"
      stderr: "ds_err.log"
    in:
      datasource: copy/name
    out: [out,err]

  log:
    run:
      class: CommandLineTool
      baseCommand: cat
      inputs:
        o:
          type: File
          inputBinding:
            position: 1
        e:
          type: File
          inputBinding:
            position: 2
      outputs: []
    in:
      o: import/out
      e: import/err
    out: []


