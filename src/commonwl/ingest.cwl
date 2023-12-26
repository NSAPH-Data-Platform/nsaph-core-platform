#!/usr/bin/env cwl-runner
### Universal uploader of the tabular data to the database
#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Michael A Bouzinier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

cwlVersion: v1.2
class: CommandLineTool
baseCommand: [python, -m, nsaph.loader.data_loader]
# baseCommand: echo
requirements:
  InlineJavascriptRequirement: {}

hints:
  DockerRequirement:
    dockerPull: forome/dorieh


doc: |
  This tool ingests tabular data, usually in CSV format into the database


inputs:
  registry:
    type: File
    inputBinding:
      prefix: --registry
    doc: |
      A path to the data model file
  table:
    type: string
    doc: the name of the table to be created
    inputBinding:
      prefix: --table
  database:
    type: File
    doc: Path to database connection file, usually database.ini
    inputBinding:
      prefix: --db
  connection_name:
    type: string
    doc: The name of the section in the database.ini file
    inputBinding:
      prefix: --connection
  domain:
    type: string
    inputBinding:
      prefix: --domain
  input:
    type:
      - File
      - File[]
    inputBinding:
      prefix: --data
    doc: |
      A path the downloaded data files
  pattern:
    type: string
    default: "*.csv*"
    inputBinding:
      prefix: --pattern
  threads:
    type: int
    default: 4
    doc: number of threads, concurrently writing into the database
  page_size:
    type: int
    default: 1000
    doc: explicit page size for the database
  log_frequency:
    type: long
    default: 100000
    doc: informational logging occurs every specified number of records
  limit:
    type: long?
    doc: |
      if specified, the process will stop after ingesting
      the specified number of records
  depends_on:
    type: Any?
    doc: a special field used to enforce dependencies and execution order

arguments:
    - valueFrom: "--reset"

outputs:
  log:
    type: File?
    outputBinding:
      glob: "*.log"
  errors:
    type: stderr

stderr:  $("ingest-" + inputs.table + ".err")
