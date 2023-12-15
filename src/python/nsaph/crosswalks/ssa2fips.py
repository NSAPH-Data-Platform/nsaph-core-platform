"""
Utility to create in-database mapping table between state and
county codes used by SSA for Medicare claims and Federal Information
Processing Series (FIPS) codes

Data has been taken from National Bureau of Economic Research

https://www.nber.org/research/data/ssa-federal-information-processing-series-fips-state-and-county-crosswalk
"""

#  Copyright (c) 2022. Harvard University
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
from pathlib import Path
from typing import Dict, List

from nsaph.data_model.utils import split

from nsaph.db import Connection

from nsaph.data_model.domain import Domain

from nsaph.loader.common import DBConnectionConfig
from nsaph.operations.domain_operations import DomainOperations

from nsaph_utils.utils.io_utils import as_csv_reader



class SSAFile:
    def __init__(self, url, state, county,
                 fips2=None, fips3=None, fips5=None,
                 ssa2=None, ssa3=None, ssa5=None):
        self.ssa5 = ssa5
        self.ssa3 = ssa3
        self.ssa2 = ssa2
        self.fips5 = fips5
        self.fips2 = fips2
        self.county = county
        self.state = state
        self.fips3 = fips3
        self.url = url
        if (not fips5) and (not fips3 or not fips2):
            raise ValueError("No FIPS")
        if (not ssa5) and (not ssa3 or not ssa2):
            raise ValueError("No FIPS")
        self.data: List[Dict] = []

    def read(self):
        reader = as_csv_reader(self.url, mode="t")
        while True:
            try:
                row = next(reader)
            except:
                break
            data = dict()
            data["state"] = row[self.state]
            data["county"] = row[self.county]
            if self.fips5:
                data["fips5"] = row[self.fips5]
            if self.fips2:
                data["fips2"] = row[self.fips2]
            if self.fips3:
                data["fips3"] = row[self.fips3]
            if not self.fips5:
                data["fips5"] = "{:2}{:3}".format(data["fips2"], data["fips3"])
            if not self.fips2:
                data["fips2"] = data["fips5"][:2]
            if not self.fips3:
                data["fips3"] = data["fips5"][2:]
                
            if self.ssa5:
                data["ssa5"] = row[self.ssa5]
            if self.ssa2:
                data["ssa2"] = row[self.ssa2]
            if self.ssa3:
                data["ssa3"] = row[self.ssa3]
            if not self.ssa5:
                data["ssa5"] = "{:2}{:3}".format(data["ssa2"], data["ssa3"])
            if not self.ssa2:
                data["ssa2"] = data["ssa5"][:2]
            if not self.ssa3:
                data["ssa3"] = data["ssa5"][2:]

            self.data.append(data)
        return

    def set_year(self, y: int):
        for record in self.data:
            record["year"] = y
        return

    def values(self, columns: List[str]) -> str:
        values = []
        for record in self.data:
            vv = [
                "'{}'".format(record[c]) if c != 'year'
                else "{:d}".format(record[c])
                for c in columns
            ]
            vs = ','.join(vv)
            values.append("({})".format(vs))
        return ",\n".join(values)


class SSA2FIPS:
    years = [2003] + list(range(2011,2019))

    meta = {
        2003: SSAFile(
            "https://data.nber.org/ssa-fips-state-county-crosswalk/msabea.csv",
            "abbr", "county", fips5="fips", ssa5="ssa"
        ),
        2018: SSAFile("https://data.nber.org/ssa-fips-state-county-crosswalk/2018/xwalk2018.csv",
            "State", "County Name", fips5="FIPS County Code", ssa5="SSACD"
        )
    }
    for y in range(2011, 2018):
        meta[y] = SSAFile(
            "https://data.nber.org/ssa-fips-state-county-crosswalk/{year:d}/ssa_fips_state_county{year:d}.csv"
                .format(year=y),
            "state", "county", ssa5="ssacounty", fips5="fipscounty",
            fips2="fipsstate", ssa2="ssastate"
        )

    def __init__(self, context: DBConnectionConfig = None):
        if not context:
            context = DBConnectionConfig(None, __doc__).instantiate()
        self.context = context
        self.connection = None
        reg_path = os.path.join(
            Path(__file__).parents[3],
            "yml",
            "ssa2fips.yaml"
        )
        self.domain = Domain(reg_path, "ssa")
        self.domain.init()
        return

    def read(self, year: int) -> SSAFile:
        ssa_file = self.meta[year]
        ssa_file.read()
        ssa_file.set_year(year)
        return ssa_file

    def print(self):
        for y in self.years:
            print("{:d}: {:d}".format(y, len(x.read(y).data)))

    def _connect(self):
        self.connection = Connection(self.context.db,
                        self.context.connection,
                        silent=True,
                        app_name_postfix=".ssa2fips")
        return self.connection.connect()

    def store(self):
        tname = "ssa"
        table = self.domain.find(tname)
        t = self.domain.fqn(tname)
        columns = [split(c)[0] for c in table["columns"]]

        insert = "INSERT INTO {table} ({columns})  VALUES ".format(
            table=t, columns=",".join(columns)
        )
        with self._connect() as cnxn:
            DomainOperations.drop(self.domain, "ssa", cnxn)
            with cnxn.cursor() as cursor:
                for ddl in self.domain.ddl:
                    print(ddl)
                    cursor.execute(ddl)
                for y in self.years:
                    f = self.read(y)
                    values = f.values(columns)
                    print(y, end = "")
                    cursor.execute(insert + values)
                    print(" Done [{:d}]".format(len(f.data)))
                for ddl in self.domain.indices:
                    print(ddl)
                    cursor.execute(ddl)

        return


if __name__ == '__main__':
    x = SSA2FIPS()
    x.store()
