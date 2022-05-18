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


"""
Utility to download ZIP to FIPS mapping from HUD site

https://www.huduser.gov/portal/datasets/usps_crosswalk.html


"""
import gzip
import os
from pathlib import Path

import pandas
from nsaph.util.resources import name2path


class Zip2FipsCrossWalk:
    table = "public.hud_zip2fips"
    url_pattern = "https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_{month}{year}.xlsx"
    m2q = {
        1: "03",
        2: "06",
        3: "09",
        4: "12"
    }

    def __init__(self):
        return

    def download(self, year: int, quarter: int) -> pandas.DataFrame:
        if quarter not in range(1,5):
            raise ValueError("Quarter must be between 1 and 4: " + str(quarter))
        m = self.m2q[quarter]
        url = self.url_pattern.format(month=m, year=str(year))
        print("Downloading: " + url + " ...", end="")
        df: pandas.DataFrame = pandas.read_excel(url)
        print(" Processing ", end="")
        df.rename(str.lower, inplace=True, axis=1)
        print('.', end = "")
        n = df.shape[0]
        df.insert(0, "year", year)
        print('.', end = "")
        df.insert(1, "month", m)
        print('.', end = "")

        df[["fips2i", "fips3i"]] = df.county.apply(
            lambda x: pandas.Series((int(x / 1000), int(x % 1000)))
        )
        print('.', end = "")
        df["zip5s"] = df.zip.apply(
            lambda x: pandas.Series("{:05d}".format(x))
        )
        print('.', end = "")
        df["fips2s"] = df.fips2i.apply(
            lambda x: pandas.Series("{:02d}".format(x))
        )
        print('.', end = "")
        df["fips3s"] = df.fips3i.apply(
            lambda x: pandas.Series("{:03d}".format(x))
        )
        print('.', end = "")
        print("Done")
        return df

    def save(self):
        f = os.path.join(Path(__file__).parents[4], "resources", name2path(self.table)) + ".json.gz"
        with gzip.open(f, "w") as output:
            df = self.download(2010, 1)
            df.to_json(output, orient="records", lines=True)
            for y in range(2011, 2022):
                df = self.download(y, 4)
                df.to_json(output, orient="records", lines=True)


if __name__ == '__main__':
    cw = Zip2FipsCrossWalk()
    cw.save()


        
