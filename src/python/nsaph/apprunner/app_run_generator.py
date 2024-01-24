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
from enum import Enum
from typing import Dict, List

import yaml
from urllib.parse import urlparse
from github import Github, UnknownObjectException

from nsaph.util.cwl_generator import CWLGenerator


class Inputs(Enum):
    database = "database"
    connection_name = "connection_name"
    registry = "registry"

    @classmethod
    def as_dict(cls, registry_path = None):
        return {
            cls.database.name: {
                "type": "File",
                "doc": "Path to database connection file, usually database.ini"
            },
            cls.registry.name: {
                "type": "File",
                "doc": "Path to table/columns registry",
                "default": {
                    "class": "File",
                    "path": registry_path
                }
            },
            cls.connection_name.name: {
                "type": "string",
                "doc": 'The name of the section in the database.ini file'
            }
        }


class CWLAppRunnerGenerator(CWLGenerator):
    DEF_OUPUT_TYPE = "csv"

    def __init__(self, repo: str, path_to_pipeline: str, branch: str = None):
        super().__init__(path_to_pipeline)
        self.refurl = None
        self.md_ref_url = None
        if repo.lower().startswith("http"):
            url = urlparse(repo)
            if repo.endswith('/'):
                self.refurl = repo[:-1]
            if url.path.endswith(".git"):
                self.repo, _ = os.path.splitext(url.path)
                self.refurl = repo[:-4]
            else:
                self.repo = url.path
                self.refurl = repo
            self.refurl += "/tree/" + branch
        else:
            self.repo = repo
        if self.repo.startswith('/'):
            self.repo = self.repo[1:]
        self.branch = branch
        self.config = self.fetch_metadata()
        self.dockerfile = None

    def fetch_metadata(self):
        g = Github()
        repo = g.get_repo(self.repo)

        try:
            contents = repo.get_contents("app.config.yml", ref=self.branch)
        except UnknownObjectException:
            contents = repo.get_contents("app.config.yaml", ref=self.branch)
        config = yaml.safe_load(contents.decoded_content)
        md_path = config["metadata"]
        if self.refurl is not None:
            self.md_ref_url = self.refurl + '/' + md_path
        contents = repo.get_contents(md_path, ref=self.branch)
        metadata = yaml.safe_load(contents.decoded_content)
        config["metadata"] = metadata
        registry_path = config["dorieh-metadata"]
        contents = repo.get_contents(registry_path, ref=self.branch)
        registry_header = contents.decoded_content.decode("utf-8")
        config["registry_header"] = registry_header
        yaml.safe_dump(config, sys.stdout, indent=2)
        return config

    def generate_workflow(self):
        dataset_name=self.config["metadata"]["dataset_name"]
        comment=f"Workflow to ingest {dataset_name} into Dorieh Data warehouse"
        requirements = dict()
        requirements.update(self.create_network_requirement())
        requirements.update(self.create_expression_requirements())
        self.write_header(
            requirements=requirements,
            comment=comment
        )

        self.write_inputs(
            inputs=Inputs.as_dict(registry_path=self.get_registry_file_name())
        )
        self.start_steps()
        self.generate_app_step()
        self.generate_ingest_steps()
        self.write_app_outputs()

    def get_output_tables(self) -> Dict[str,Dict]:
        if "fields" not in self.config["metadata"]:
            raise ValueError("Entry 'fields' is not found in metadata")
        return self.config["metadata"]["fields"]

    def get_output_files(self) -> List[str]:
        tables = self.get_output_tables()
        files = []
        for table in tables:
            if "type" in tables[table]:
                t = tables[table]["type"].lower()
            else:
                t = self.DEF_OUPUT_TYPE
            files.append(table + '.' + t)
        return files

    def fetch_from_docker_file(self, key: str):
        if self.dockerfile is None:
            g = Github()
            repo = g.get_repo(self.repo)
            contents = repo.get_contents("Dockerfile", ref=self.branch)
            self.dockerfile = contents.decoded_content.decode("utf-8").split('\n')
        for line in self.dockerfile:
            if line.startswith(key):
                value = line[len(key):].strip()
                return value
        return None

    def generate_app_step(self):
        command: str = self.config["docker"]["run"]
        if command.startswith('$'):
            if command.upper() == "$CMD":
                command = self.fetch_from_docker_file(command[1:])
                if command is None:
                    raise ValueError("No CMD in Dockerfile")
        workdir = self.fetch_from_docker_file("WORKDIR")
        if workdir is not None:
            cmd = yaml.safe_load(command)
            cmd1 = " ".join(cmd)
            cmd1 = f"cp -R {workdir}/* . && {cmd1}"
            cmd = ["sh", "-c", cmd1]
            command = str(cmd)
        if self.config["docker"]["outputdir"]:
            outputdir = self.config["docker"]["outputdir"] + '/'
        else:
            outputdir = ""

        outputs = self.get_output_files()
        requirements: Dict = self.create_docker_requirement(
            self.config["docker"]["image"]
        )
        indent = "        "
        reqs = yaml.safe_dump(requirements, indent=2)
        reqs = indent + reqs.replace('\n', '\n' + indent)
        with open(self.pipeline, "at") as pipeline:
            print( "  execute:", file=pipeline)
            print( "    run:", file=pipeline)
            print( "      class: CommandLineTool", file=pipeline)
            print( "      requirements: ", file=pipeline)
            print(reqs, file=pipeline)
            print(f"      baseCommand: {command}", file=pipeline)
            print( "      inputs: {}", file=pipeline)
            print( "      outputs: ", file=pipeline)
            names = []
            for output in outputs:
                name, _ = os.path.splitext(os.path.basename(output))
                names.append(name)
                self.write_output(
                    name,
                    glob=f"{outputdir}{output}",
                    indent="        ",
                    stream=pipeline
                )
            print( "    in: {}", file=pipeline)
            print( "    out: ", file=pipeline)
            for name in names:
                print(f"        - {name}", file=pipeline)
            print(file=pipeline)
        return

    def generate_ingest_steps(self):
        self.copy_tool("ingest")
        with open(self.pipeline, "at") as pipeline:
            tt = list(self.get_output_tables().keys())
            tf = self.get_output_files()
            for i in range(len(tt)):
                self.generate_ingest_step(tt[i], tf[i], pipeline)
        return

    def generate_ingest_step(self, table: str, inp: str, pipeline):
        print(f"  ingest_{table}:", file=pipeline)
        print( "    run: ingest.cwl", file=pipeline)
        print( "    in: ", file=pipeline)
        print(f"      {Inputs.registry.value}: {Inputs.registry.value}", file=pipeline)
        print(f"      {Inputs.database.value}: {Inputs.database.value}", file=pipeline)
        print(f"      {Inputs.connection_name.value}: {Inputs.connection_name.value}", file=pipeline)
        print( "      table: ", file=pipeline)
        print(f"        valueFrom: {table}", file=pipeline)
        print( "      domain: ", file=pipeline)
        print(f"        valueFrom: {self.get_domain_name()}", file=pipeline)
        print(f"      input: execute/{table}", file=pipeline)
        print( "    out: ", file=pipeline)
        print( "      - log ", file=pipeline)
        print( "      - errors ", file=pipeline)
        print(file=pipeline)
        return

    def write_app_outputs(self):
        self.start_outputs()
        tt = list(self.get_output_tables().keys())
        tf = self.get_output_files()
        for i in range(len(tt)):
            name, _ = os.path.splitext(os.path.basename(tf[i]))
            self.write_output(name=name, step="execute")
            step = f"ingest_{tt[i]}"
            self.write_output(prefix=step + '_', name="log", step=step)
            self.write_output(prefix=step + '_', name="errors", step=step)
            self.empty_line()

    def get_registry_file_name(self):
        return self.get_domain_name() + ".yaml"

    def get_domain_name(self):
        registry = yaml.safe_load(self.config["registry_header"])
        return registry["domain"]

    def generate_registry(self):
        content = yaml.safe_load(self.config["registry_header"])
        domain =  content["domain"]
        del content["domain"]
        registry = {
            domain: content
        }

        outf = self.get_registry_file_name()
        outf = os.path.join(self.get_work_dir(), outf)
        if "tables" in content:
            tables_dict = content["tables"]
        else:
            tables_dict = dict()
            content["tables"] = tables_dict
        output_tables = self.get_output_tables()
        for table_name in output_tables:
            if table_name in tables_dict:
                table_def = tables_dict[table_name]
            else:
                table_def = dict()
                tables_dict[table_name] = table_def
            columns = []
            table_def["columns"] = columns
            for c1 in output_tables[table_name]:
                c_name = c1["name"]
                c_type = c1["type"]
                c_desc = c1["description"]
                if c_type == "character":
                    t = "varchar"
                else:
                    t = c_type
                c2 = {
                    c_name: {
                        "type": t,
                        "description": c_desc,
                        "reference": self.md_ref_url
                    }
                }
                columns.append(c2)
        with open(outf, 'wt') as f:
            yaml.safe_dump(registry, f)
        return

    def generate_pipeline(self):
        self.generate_workflow()
        self.generate_registry()
        return


if __name__ == '__main__':
    generator = CWLAppRunnerGenerator(sys.argv[1], sys.argv[2], sys.argv[3])
    generator.generate_pipeline()
    
    