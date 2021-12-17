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

import os
from typing import List

from nsaph_utils.utils.io_utils import get_entries
from nsaph.util.resources import get_resources


def install(dest: str) -> List[str]:
    if not os.path.isdir(dest):
        os.makedirs(dest)
    shapes = []
    for shape in ["shapes.zips.", "shapes.counties."]:
        resource = get_resources(shape)['tgz']
        entries, _ = get_entries(resource)
        ss = [
            e for e in [
                os.path.basename(entry.name) for entry in entries
            ]
            if e.lower().endswith(".shp")
            and not e.startswith('.')
        ]
        if len(ss) < 1:
            raise ValueError("No shape file found in {}".format(resource))
        if len(ss) > 1:
            raise ValueError("Ambiguous shape file found in {}: {}"
                             .format(resource, ", ".join(ss)))
        os.system("tar -C {} -zxvf {}".format(dest, resource))
        shapes.append(os.path.join(dest, ss[0]))
    return shapes


if __name__ == '__main__':
    print(get_resources("shapes.zips."))
    print(get_resources("shapes.counties."))
    shps = install("/tmp/shapes")
    print(shps)
