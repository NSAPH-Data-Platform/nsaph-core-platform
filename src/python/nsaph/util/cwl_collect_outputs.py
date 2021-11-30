import sys
import yaml


def collect(name: str, path: str):
    with open(path) as f:
        cwl = yaml.safe_load(f)
        outputs = cwl["outputs"]
        for o in outputs:
            t = outputs[o]["type"]
            print("\t{}:".format(o))
            print("\t\ttype: {}".format(t))
            print("\t\toutputSource: {}/{}".format(name, o))


if __name__ == '__main__':
    collect(sys.argv[1], sys.argv[2])
            