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
