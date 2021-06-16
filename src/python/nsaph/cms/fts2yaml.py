import glob

import yaml

from nsaph.reader import fopen
from nsaph.pg_keywords import *


def width(s:str):
    if '.' in s:
        x = s.split('.')
        return (int(x[0]), int(x[1]))
    return (int(s), None)


class ColumnAttribute:
    def __init__(self, start:int, end:int, conv):
        self.start = start
        self.end = end
        self.conv = conv

    def arg(self, line:str):
        try:
            return self.conv(line[self.start:self.end].strip())
        except:
            pass


class ColumnReader:
    def __init__(self, constructor, pattern):
        self.constructor = constructor
        fields = pattern.split(' ')
        assert len(fields) == constructor.nattrs
        self.attributes = []
        c = 0
        for i in range(0, len(fields)):
            l = len(fields[i])
            f = constructor.conv(i)
            self.attributes.append(ColumnAttribute(c, c+l, f))
            c += l + 1
        self.attributes[-1].end = None

    def read(self, line):
        attrs = [a.arg(line) for a in self.attributes]
        return self.constructor(*attrs)


class MedicareFTSColumn:
    @classmethod
    def conv(cls, i):
        if i in [0, 4]:
            f = int
        elif i in [5]:
            f = width
        else:
            f = str

    def __init__(self, ord:int, long_name:str, short_name:str, type:str, start:int, width, desc:str):
        self.name = short_name
        self.long_name = long_name
        self.type = type
        self.ord = ord
        self.start = start - 1
        self.length = width[0]
        self.end = self.start + self.length
        self.desc = desc
        self.d = width[1]


class MedicaidFTSColumn:
    @classmethod
    def conv(cls, i):
        if i in [0, 4]:
            f = int
        else:
            f = str
        return f

    nattrs = 6

    def __init__(self, order, column, c_type, c_format, c_width, label):
        self.order = order
        self.column = column
        self.type = c_type
        self.format = c_format
        self.width = c_width
        self.label = label
        self._attrs = [
            attr for attr in self.__dict__ if attr[0] != '_'
        ]

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, MedicaidFTSColumn):
            return False
        if o is self:
            return True
        for attr in self._attrs:
            if getattr(self, attr) != getattr(o, attr):
                return False
        return True

    def to_sql_type(self):
        t = self.type.upper()
        if self.format[0].isdigit():
            fmt = self.format
            can_be_numeric = True
        else:
            fmt = self.format[1:]
            can_be_numeric = False
        x = fmt.split('.')
        if x[0].isdigit():
            w = int(x[0])
        else:
            w = None
        if len(x) > 1 and x[1]:
            scale = int(x(1))
        else:
            scale = None
        if t == "CHAR":
            return "{}({:d})".format(PG_STR_TYPE, w)
        if t == "NUM":
            if not can_be_numeric:
                return "{}({:d})".format(PG_STR_TYPE, w)
            if scale is not None:
                return "{}({:d},{:d})".format(PG_NUMERIC_TYPE, w, scale)
            return "{}".format(PG_INT_TYPE)
        if t == "DATE":
            return PG_DATE_TYPE
        raise Exception("Unexpected column type: {}".format(t))


class MedicaidFTS:
    def __init__(self, type_of_data: str):
        """

        :param type_of_data: Can be either `ps` for personal summary or
            `ip` for inpatient admissions data
        """

        type_of_data = type_of_data.lower()
        assert type_of_data in ["ps", "ip"]
        self.name = type_of_data
        self.pattern = "**/maxdata_{}_*.fts".format(type_of_data)
        self.columns = None
        self.pk = ["MSIS_ID", "STATE_CD", "MAX_YR_DT"]
        self.indices = self.pk + [
            "BENE_ID",
            "EL_DOB",
            "EL_AGE_GRP_CD",
            "EL_SEX_CD",
            "EL_DOD"
        ]

    def init(self):
        files = glob.glob(self.pattern)
        for file in files:
            self.read_file(file)
        return self

    def read_file(self, f):
        with fopen(f) as fts:
            lines = [line for line in fts]
        i = 0
        column_reader = None
        for i in range(0, len(lines)):
            line = lines[i]
            if line.startswith('---') and '------------------' in line:
                column_reader = ColumnReader(MedicaidFTSColumn, line)
                break
            continue

        if 1 > i or i > len(lines) - 2:
            raise Exception("Column definitions are not found in {}".format(f))

        columns = []
        while i < len(lines):
            i += 1
            line = lines[i]
            if not line.strip():
                break
            if line.startswith("Note:"):
                break
            if line.startswith("-") and "End" in line:
                break
            column = column_reader.read(line)
            columns.append(column)

        if not self.columns:
            self.columns = columns
            return

        if len(columns) != len(self.columns):
            raise Exception("Reconciliation required: {}, number of columns".format(f))

        for i in range(len(columns)):
            if columns[i] != self.columns[i]:
                raise Exception("Reconciliation required: {}, column: {}".format(f, columns[i]))

    def column_to_dict(self, c: MedicaidFTSColumn) -> dict:
        d = {
            "type": c.to_sql_type(),
            "description": c.label
        }
        if c.column in self.indices:
            d["index"] = True
        return d

    def to_dict(self):
        table = dict()
        table[self.name] = dict()
        table[self.name]["columns"] = [
            {
                c.column: self.column_to_dict(c)
            } for c in self.columns
        ]
        table[self.name]["primary_key"] = self.pk
        return table

    def print_yaml(self):
        self.init()
        table = self.to_dict()
        print(yaml.dump(table))


if __name__ == '__main__':
    source = MedicaidFTS("ps")
    source.print_yaml()

