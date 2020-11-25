import codecs
import gzip
import io
import tarfile
import datetime
import os


class SpecialValues:
    NA = "NA"
    NaN = "NaN"

    @classmethod
    def is_missing(cls, v) -> bool:
        return v in [cls.NA, cls.NaN]

    @classmethod
    def is_untyped(cls, v) -> bool:
        if not v:
            return True
        return cls.is_missing(v) or v in ['0']


def fopen(path):
    if isinstance(path, io.BufferedReader):
        return codecs.getreader("utf-8")(path)
    if not isinstance(path, str):
        return path
    if path.endswith(".gz"):
        return gzip.open(path, "rt")
    return open(path)


def name(path):
    if isinstance(path, tarfile.TarInfo):
        full_name =  path.name
    else:
        full_name = str(path)
    name, _ = os.path.splitext(os.path.basename(full_name))
    return name


def get_entries(path: str):
    entries = []
    f = lambda e: e
    if path.endswith(".tar") or path.endswith(".tgz") or path.endswith(
            ".tar.gz"):
        tfile = tarfile.open(path)
        entries = [e for e in tfile.getmembers() if e.isfile()]
        f = lambda e: tfile.extractfile(e)
    elif os.path.isdir(path):
        pass
    else:
        entries.append(path)
    return entries, f


class CSVFileWrapper():
    def __init__(self, file_like_object, sep = ',', null_replacement = SpecialValues.NA):
        self.file_like_object = file_like_object
        self.sep = sep
        self.null_replacement = null_replacement
        self.empty_string = self.sep + self.sep
        self.null_string = self.sep + self.null_replacement + sep
        self.empty_string_eol = self.sep + '\n'
        self.null_string_eol = self.sep + self.null_replacement + '\n'
        self.l = len(sep)
        self.remainder = ""
        self.line_number = 0
        self.last_printed_line_number = 0
        self.chars = 0

    def __getattr__(self, called_method):
        if called_method == "readline":
            return self._readline
        if called_method == "read":
            return self._read
        return getattr(self.file_like_object, called_method)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file_like_object.close()

    def _replace_empty(self, s: str):
        while self.empty_string in s:
            s = s.replace(self.empty_string, self.null_string)
        s = s.replace(self.empty_string_eol, self.null_string_eol)
        return s

    def _readline(self):
        line = self.file_like_object.readline()
        self.line_number += 1
        self.chars += len(line)
        return self._replace_empty(line)

    def _read(self, size, *args, **keyargs):
        if (len(self.remainder) < size):
            raw_buffer = self.file_like_object.read(size, *args, **keyargs)
            buffer = raw_buffer
            while buffer[-self.l:] == self.sep:
                next_char = self.file_like_object.read(self.l)
                buffer += next_char
            buffer = self._replace_empty(buffer)
        else:
            raw_buffer = ""
            buffer = raw_buffer
        if self.remainder:
            buffer = self.remainder + buffer
            self.remainder = ""

        if len(buffer) > size:
            self.remainder = buffer[size - len(buffer):]
            result = buffer[0:size]
        else:
            result = buffer

        self.chars += len(result)
        nl = result.count('\n')
        self.line_number += nl
        t = datetime.datetime.now()
        if (self.line_number - self.last_printed_line_number) > 1000000:
            if self.chars > 1000000000:
                c = "{:7.2f}G".format(self.chars/1000000000.0)
            elif self.chars > 1000000:
                c = "{:6.2f}M".format(self.chars/1000000.0)
            else:
                c = str(self.chars)
            dt = datetime.datetime.now() - t
            t = datetime.datetime.now()
            print("{}: Processed {:d}/{} lines/chars [{}]"
                  .format(str(t), self.line_number, c, str(dt)))
            self.last_printed_line_number = self.line_number
        return result


