import csv
import gzip
import io
import tempfile
import zipfile
from enum import IntEnum, Enum
from typing import List, Dict
import requests
import os
from datetime import datetime, timezone
from dateutil.parser import parse
from typing.io import IO

BASE_AQS_EPA_URL = "https://aqs.epa.gov/aqsweb/airdata/"
ANNUAL_URI = "annual_conc_by_monitor_{year}.zip"
DAILY_URI = "daily_{parameter}_{year}.zip"

MONITOR_FORMAT = "{state}{county:03d}-{site:04d}"


STATE_CODE = "State Code"
COUNTY_CODE = "County Code"
SITE_NUM = "Site Num"
PARAMETER_CODE = "Parameter Code"
MONITOR = "Monitor"


class Parameter(IntEnum):
    NO2 = 42602
    OZONE = 44201
    PM25 = 88101
    MAX_TEMP = 68104
    MIN_TEMP = 68103

    def __str__(self):
        return str(self.name)


class Aggregation(Enum):
    ANNUAL = "annual"
    DAILY = "daily"


def download(url: str, to: IO):
    response = requests.get(url, stream=True)
    for chunk in response.iter_content(chunk_size=1048576):
        to.write(chunk)
        print('#', end='')
    print()


def as_stream(url: str, extension: str = ".csv"):
    response = requests.get(url, stream=True)
    raw = response.raw
    if url.lower().endswith(".zip"):
        #_, tfile = tempfile.mkstemp()
        tfile = tempfile.TemporaryFile()
        download(url, tfile)
        tfile.seek(0)
        zfile = zipfile.ZipFile(tfile)
        entries = [
            e for e in zfile.namelist() if e.endswith(extension)
        ]
        assert len(entries) == 1
        stream = io.TextIOWrapper(zfile.open(entries[0]))
    else:
        stream = raw
    return stream


def as_csv_reader(url: str):
    stream = as_stream(url)
    reader = csv.DictReader(stream, quotechar='"', delimiter=',',
        quoting=csv.QUOTE_NONNUMERIC, skipinitialspace=True)
    return reader


def write_csv(reader: csv.DictReader, writer: csv.DictWriter, flt=None):
    counter = 0
    for row in reader:
        add_monitor_key(row)
        if (not flt) or flt(row):
            writer.writerow(row)
        counter += 1
        if (counter % 10000) == 0:
            print("*", end="")


def add_monitor_key(row: Dict):
    monitor = MONITOR_FORMAT.format(state = row[STATE_CODE],
                                    county = int(row[COUNTY_CODE]),
                                    site = int(row[SITE_NUM]))
    row[MONITOR] = monitor


def fopen(target: str, mode: str):
    if target.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(target, mode))
    return open(target, mode)


def download_data(url: str, target: str, parameters: List):
    with fopen(target, "w") as ostream:
        reader = as_csv_reader(url)
        fieldnames = list(reader.fieldnames)
        fieldnames.append(MONITOR)
        writer = csv.DictWriter(ostream, fieldnames, quotechar='"',
                                delimiter=',',
                                quoting=csv.QUOTE_NONNUMERIC)
        if parameters:
            flt = lambda row: int(row[PARAMETER_CODE]) in parameters
        else:
            flt = None
        write_csv(reader, writer, flt)


def destination_path(destination: str,path: str) -> str:
    return os.path.join(destination, path.replace(".zip", ".csv.gz"))

def is_downloaded(url: str, target: str) -> bool:
    if os.path.isfile(target):
        response = requests.head(url, allow_redirects=True)
        headers = response.headers
        #remote_size = int(headers.get('content-length', 0))
        remote_date = parse(headers.get('Last-Modified', 0))
        stat = os.stat(target)
        local_size = stat.st_size
        local_date = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        return local_date > remote_date and local_size > 1000


def download_aqs_data (aggregation: Aggregation,
                       years: List,
                       destination: str,
                       parameters: List):
    if aggregation == Aggregation.DAILY:
        uri = DAILY_URI
        assert len(parameters) > 0
    else:
        raise Exception ("Invalid aggregation")

    downloads = []
    for year in years:
        if aggregation == Aggregation.ANNUAL:
            path = ANNUAL_URI.format(year=year)
            if not parameters:
                target = destination_path(destination, path)
            else:
                f = path[:-4] + '_' + '_'.join(map(str,parameters)) + ".csv.gz"
                target = os.path.join(destination, f)
            url = BASE_AQS_EPA_URL + path
            downloads.append((url, target, parameters))
        elif aggregation == Aggregation.DAILY:
            assert len(parameters) > 0
            for parameter in parameters:
                path = DAILY_URI.format(parameter=parameter.value, year=year)
                url = BASE_AQS_EPA_URL + path
                target = destination_path(destination, path)
                downloads.append((url, target, None))

    for (url, target, parameters) in downloads:
        if not is_downloaded(url, target):
            download_data(url, target, parameters)





if __name__ == '__main__':
    download_aqs_data(Aggregation.DAILY, [2012], "/tmp", [Parameter.PM25])

