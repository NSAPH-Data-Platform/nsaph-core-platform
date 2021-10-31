import glob
import os
import sys
from pathlib import Path
from typing import Dict


def get_resource_dir() -> str:
    root = Path(__file__).parents[4]
    return os.path.join(root, "resources")


def name2path(name: str) -> str:
    return os.path.join(*(name.split('.')))


def get_resources(name: str) -> Dict[str, str]:
    rel_path = name2path(name)
    dirs = [get_resource_dir()] + [
        os.path.join(d, "nsaph_resources")
        for d in sys.path
    ]
    for d in dirs:
        rpath = os.path.join(d, rel_path + "*")
        resources = glob.glob(rpath, recursive=False)
        if resources:
            return {
                '.'.join(os.path.basename(r).split('.')[1:]): r
                for r in resources
            }
    return {}


if __name__ == '__main__':
    print(get_resources("public.us_iso"))
