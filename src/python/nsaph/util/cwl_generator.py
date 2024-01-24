#  Copyright (c) 2023.  Harvard University
#
#   Developed by Research Software Engineering,
#   Harvard University Research Computing and Data (RCD) Services.
#
#   Author: Michael A Bouzinier
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#          http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#

import os
import shutil
import sys
from pathlib import Path
from typing import Dict, Optional

import yaml


class CWLGenerator:
    def __init__(self, path_to_pipeline: str):
        assert path_to_pipeline.endswith(".cwl")
        self.pipeline = path_to_pipeline
        return

    def write_header(self, requirements: Dict, comment: str, hints: Dict = None):
        with open(self.pipeline, "wt") as pipeline:
            print("#!/usr/bin/env cwl-runner", file=pipeline)
            if comment:
                print("### " + comment, file=pipeline)
            print(file=pipeline)
            print("cwlVersion: v1.2", file=pipeline)
            print("class: Workflow", file=pipeline)
            print(file=pipeline)
            if requirements:
                block = yaml.dump({"requirements": requirements}, indent=2)
                print(block, file=pipeline)
                print(file=pipeline)
            if hints:
                block = yaml.dump({"hints": hints}, indent=2)
                print(block, file=pipeline)
                print(file=pipeline)
        return

    @staticmethod
    def create_docker_requirement(image: str, workdir: str = None) \
            -> Dict[str, Dict[str, str]]:
        return {
            "DockerRequirement": {
                "dockerPull": image,
                "dockerOutputDirectory": workdir
            }
        }

    @staticmethod
    def create_network_requirement() -> Dict[str, Dict[str, bool]]:
        return {
            "NetworkAccess": {
                "networkAccess": True
            }
        }

    @staticmethod
    def create_expression_requirements() -> Dict[str, Dict[str, bool]]:
        return {
            "StepInputExpressionRequirement": {},
            "InlineJavascriptRequirement": {}
        }

    def write_inputs(self, inputs: Dict):
        with open(self.pipeline, "at") as pipeline:
            if inputs:
                block = yaml.dump({"inputs": inputs}, indent=2)
                print(block, file=pipeline)
            else:
                print("inputs: {}", file=pipeline)

            print(file=pipeline)
        return

    def start_steps(self):
        with open(self.pipeline, "at") as pipeline:
            print("steps:", file=pipeline)

    def start_outputs(self):
        with open(self.pipeline, "at") as pipeline:
            print("outputs:", file=pipeline)

    def empty_line(self):
        with open(self.pipeline, "at") as pipeline:
            print(file=pipeline)

    def write_output(self,
                     name: str,
                     indent: str = "  ",
                     glob: str = None,
                     step: str = None,
                     prefix: str = "",
                     stream = None):
        close = False
        if stream is None:
            stream = open(self.pipeline, "at")
            close = True
        print(f"{indent}{prefix}{name}: ", file=stream)
        print(f"{indent}  type: File", file=stream)
        if glob:
            print(f"{indent}  outputBinding: ", file=stream)
            print(f"{indent}    glob: {glob}", file=stream)
        elif step:
            print(f"{indent}  outputSource: {step}/{name}", file=stream)
        if close:
            stream.close()
        return

    @staticmethod
    def find_tool(name: str, tool_type="cwl") -> Optional[str]:
        if not name.lower().endswith(tool_type.lower()):
            name += "." + tool_type
        p = Path(__file__).parent
        while os.path.isdir(p) and len(p.parts) > 1:
            d = os.path.join(p, "commonwl")
            if os.path.isdir(d):
                f = os.path.join(d, name)
                if os.path.isfile(f):
                    return f
                return None
            p = p.parent
        return None

    def copy_tool(self, tool: str):
        tool = self.find_tool(tool)
        d = self.get_work_dir()
        shutil.copy(tool, d)

    def get_work_dir(self):
        return os.path.dirname(os.path.abspath(self.pipeline))
