"""
Command line utility to generate a test carcass for a
CWL pipeline

"""
#  Copyright (c) 2023. Harvard University
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

import os
import sys
from typing import Dict

import yaml

from nsaph.util.cwl_collect_outputs import collect


class CWLTestGenerator:
    def __init__(self, path_to_pipeline: str, path_to_test: str):
        assert path_to_pipeline.endswith(".cwl")
        assert path_to_test.endswith(".cwl")
        self.path_to_pipeline = path_to_pipeline
        with open(path_to_pipeline) as f:
            self.cwl: Dict = yaml.safe_load(f)
        self.test = path_to_test
        self.basename = os.path.basename(path_to_pipeline)
        self.path_to_runner = os.path.join(
            os.path.dirname(self.path_to_pipeline),
            "run_test.cwl"
        )
        self.last_execute_output = None
        return

    def generate(self):
        self.write_header()
        self.write_inputs()
        self.write_steps()
        self.write_outputs()

    def write_header(self):
        requirements = self.cwl.get("requirements")
        with open(self.test, "wt") as test:
            print("#!/usr/bin/env cwl-runner", file=test)
            print(f"### Test harness for {self.basename}", file=test)
            print(file=test)
            print("cwlVersion: v1.2", file=test)
            print("class: Workflow", file=test)
            print(file=test)
            if requirements:
                block = yaml.dump({"requirements": requirements}, indent=2)
                print(block, file=test)
                print(file=test)
        return

    def write_inputs(self):
        with open(self.test, "at") as test:
            print("# All inputs of original pipeline, remove what is not needed"
                  , file=test)
            inputs = self.cwl.get("inputs").copy()
            inputs["test_script"] = {
                "type": "File",
                "doc": "File containing SQL test script"
            }
            if inputs:
                block = yaml.dump({"inputs": inputs}, indent=2)
                print(block, file=test)
                print(file=test)
        return

    def write_steps(self):
        with open(self.test, "at") as test:
            print("steps:", file=test)
            self.write_execute_step(test)
            self.write_verify_step(test)
        return

    def write_execute_step(self, test):
        print("  execute:", file=test)
        print(f"    run: {self.basename}", file=test)
        self.write_in(test)
        self.write_out(test)
        print(file=test)
        return

    def write_in(self, test):
        inputs = self.cwl.get("inputs")
        print("    in:", file=test)
        if inputs:
            for par in inputs:
                print(f"      {par}: {par}", file=test)
        return

    def write_out(self, test):
        collect("execute", self.path_to_pipeline, output=test, what="step")
        self.last_execute_output = list(self.cwl.get("outputs").keys())[-1]
        return

    def write_verify_step(self, test):
        print("  verify:", file=test)
        print("    run: run_test.cwl", file=test)
        print("    in:", file=test)
        inputs = self.cwl.get("inputs")
        for par in ["database", "connection_name"]:
            assert par in inputs
            print(f"      {par}: {par}", file=test)
        print(f"      script: test_script", file=test)
        print(f"      depends_on: execute/{self.last_execute_output}", file=test)
        collect("verify", self.path_to_runner, output=test, what="step")
        print(file=test)
        return

    def write_outputs(self):
        with open(self.test, "at") as test:
            print("outputs:", file=test)
            collect("execute", self.path_to_pipeline, output=test,
                    what="pipeline")
            collect("verify", self.path_to_runner, output=test,
                    what="pipeline")
        return


if __name__ == '__main__':
    arg1 = sys.argv[1]
    if len(sys.argv) > 2:
        arg2 = sys.argv[2]
    else:
        dirname, basename = os.path.split(arg1)
        arg2 = os.path.join(dirname, "test_" + basename)
    generator = CWLTestGenerator(arg1, arg2)
    generator.generate()



