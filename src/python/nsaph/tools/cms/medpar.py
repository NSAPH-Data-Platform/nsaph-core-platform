import glob
import gzip
import os
from collections import OrderedDict
from dateutil import parser as date_parser
import csv


def log(s):
    with open("run.log", "at") as w:
        w.write(str(s) + '\n')


def width(s:str):
    if '.' in s:
        x = s.split('.')
        return (int(x[0]), int(x[1]))
    return (int(s), None)


class MedparParseException(Exception):
    def __init__(self, msg:str, pos:int):
        super(MedparParseException, self).__init__(msg, pos)
        self.pos = pos


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


class ColumnDef:
    def __init__(self, pattern):
        fields = pattern.split(' ')
        assert len(fields) == 7
        self.attributes = []
        i = 0
        c = 0
        for i in range(0, len(fields)):
            l = len(fields[i])
            if i in [0, 4]:
                f = int
            elif i in [5]:
                f = width
            else:
                f = str
            self.attributes.append(ColumnAttribute(c, c+l, f))
            c += l + 1

    def read(self, line):
        attrs = [a.arg(line) for a in self.attributes]
        return Column(*attrs)


class Column:
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




class Medpar:
    def __init__(self, dir_path: str, name: str, year:str = None):
        self.dir = dir_path
        self.fts = os.path.join(self.dir, '.'.join([name, "fts"]))
        self.csv = os.path.join(self.dir, '.'.join([name, "csv.gz"]))
        if not os.path.isfile(self.fts):
            raise Exception("Not found: " + self.fts)

        pattern = "{}*.dat".format(name)
        self.dat = glob.glob(os.path.join(self.dir, pattern))

        self.line_length = None
        self.metadata = dict()
        self.columns = OrderedDict()
        self.init()
        self.block_size = int(self.metadata["Exact File Record Length (Bytes in Variable Block)"])
        if not year:
            year = name[-4:]
        self.year = year

    def init(self):
        with open(self.fts) as fts:
            lines = [line for line in fts]
            for i in range(0, len(lines)):
                line = lines[i]
                if line.startswith('---') and '------------------' in line:
                    break
                if ':' in line:
                    x = line.split(':', 1)
                    self.metadata[x[0]] = x[1]

            cdef = ColumnDef(line)
            while i < len(lines) - 1:
                i += 1
                line = lines[i]
                if 'End of Document' in line:
                    break
                if not line.strip():
                    break
                if line.startswith("Note:"):
                    break
                column = cdef.read(line)
                self.columns[column.name] = column

    def read_record(self, data, ln):
        exception_count = 0
        pieces = {}
        for name in self.columns:
            column = self.columns[name]
            pieces[name] = data[column.start:column.end]
        record = []
        for name in self.columns:
            column = self.columns[name]
            s = pieces[name]
            try:
                if column.type == "NUM" and not column.d:
                    record.append(int(s))
                elif column.type == "DATE":
                    if s.strip():
                        record.append(date_parser.parse(s))
                    else:
                        record.append(None)
                else:
                    record.append(s.decode("utf-8") )
            except Exception as x:
                log("{:d}: {}[{:d}]: - {}".format(ln, column.name, column.ord, str(x)))
                record.append(s.decode("utf-8"))
                exception_count += 1
                if exception_count > 3:
                    log(data)
                    raise MedparParseException("Too meany exceptions", column.start)
        return record

    def validate(self, record):
        assert record[self.columns["MEDPAR_YR_NUM"].ord - 1] == self.year

    def export(self):
        with gzip.open(self.csv, "wt") as out:
            writer = csv.writer(out, quoting=csv.QUOTE_MINIMAL, delimiter='\t')
            for dat in self.dat:
                print(dat)
                counter = 0
                good = 0
                bad = 0
                bad_lines = 0
                remainder = b''
                with open(dat, "rb") as source:
                    while source.readable():
                        l = self.block_size - len(remainder) + 100
                        block = remainder + source.read(l)
                        if len(block) < self.block_size:
                            break
                        idx = self.block_size
                        try:
                            record = self.read_record(block[:idx], counter)
                            self.validate(record)
                            writer.writerow(record)
                            good += 1
                        except MedparParseException as x:
                            bad += 1
                            log("Line = " + str(counter) + ':' + str(x.pos))
                            bad_lines += 1
                            log(x)
                            for idx in range(x.pos, self.block_size):
                                if block[idx] in [10, 13]:
                                    break
                        except AssertionError as x:
                            log("Line = " + str(counter))
                            bad_lines += 1
                            log(x)
                        while block[idx] in [10, 13]:
                            idx += 1
                        remainder = block[idx:]
                        block = None
                        counter += 1
                        if (counter%10000) == 0:
                            print("{:d}/{:d}/{:d}".format(counter, good, bad))
                print("File processed. Bad lines: " + str(bad_lines))




#def dat2csv(dat_path, csv_path)


if __name__ == '__main__':
    m = Medpar(os.curdir, "medpar_all_file_res000017155_req007087_2015")
    for s in ["Columns in File", "Exact File Record Length (Bytes in Variable Block)"]:
        print("{}: {}".format(s, m.metadata[s]))

    for name in m.columns:
        c = m.columns[name]
        print("{:d} - {} - {} - {:d}".format(c.ord, c.name, c.type, c.start))

    m.export()
