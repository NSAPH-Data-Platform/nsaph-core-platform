"""
Python module to download [EPA AQS Data](https://www.epa.gov/aqs)

The module can be used as a standalone tool or as a library of functions
to be called from other python scripts.

If used as a tool, it expects user to specify a list of years,
a list of EPA Parameter Codes
<https://www.epa.gov/aqs/aqs-memos-technical-note-reporting-pm25-continuous-monitoring-and-speciation-data-air-quality>
and some instructions how to format output

The data is downloaded from https://aqs.epa.gov/aqsweb/airdata/download_files.html

"""

import argparse
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
from requests import Response
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


def check_http_response(r: Response):
    if not r.ok:
        msg = "HTTP Response: {:d}; Reason: {}".format(r.status_code, r.reason)
        raise Exception(msg)


def enum(cls, s: str):
    d = {e.name: e for e in cls}
    return d[s]


class Parameter(IntEnum):
    NO2 = 42602
    OZONE = 44201
    PM25 = 88101
    MAX_TEMP = 68104
    MIN_TEMP = 68103

    def __str__(self):
        return str(self.name)

    @classmethod
    def values(cls):
        return {p.value for p in Parameter}


class Aggregation(Enum):
    ANNUAL = "annual"
    DAILY = "daily"


def download(url: str, to: IO):
    response = requests.get(url, stream=True)
    for chunk in response.iter_content(chunk_size=1048576):
        to.write(chunk)
        print('#', end='')
    print('.', end=' ')
    return


def as_stream(url: str, extension: str = ".csv"):
    response = requests.get(url, stream=True)
    check_http_response(response)
    raw = response.raw
    if url.lower().endswith(".zip"):
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


def write_csv(reader: csv.DictReader, writer: csv.DictWriter, flt=None,
              header: bool = True):
    counter = 0
    if header:
        writer.writeheader()
    for row in reader:
        add_monitor_key(row)
        if (not flt) or flt(row):
            writer.writerow(row)
        counter += 1
        if (counter % 10000) == 0:
            print("*", end="")
    print()


def add_monitor_key(row: Dict):
    monitor = MONITOR_FORMAT.format(state = row[STATE_CODE],
                                    county = int(row[COUNTY_CODE]),
                                    site = int(row[SITE_NUM]))
    row[MONITOR] = monitor


def fopen(target: str, mode: str):
    if target.lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(target, mode))
    return open(target, mode)


def download_data(url: str, target: str, parameters: List, append: bool):
    print("{} => {}".format(url, target))
    with fopen(target, "a") as ostream:
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
        write_csv(reader, writer, flt, header=not append)


def destination_path(destination: str,path: str) -> str:
    return os.path.join(destination, path.replace(".zip", ".csv.gz"))

def is_downloaded(url: str, target: str) -> bool:
    if os.path.isfile(target):
        response = requests.head(url, allow_redirects=True)
        check_http_response(response)
        headers = response.headers
        #remote_size = int(headers.get('content-length', 0))
        remote_date = parse(headers.get('Last-Modified', 0))
        stat = os.stat(target)
        local_size = stat.st_size
        local_date = datetime.fromtimestamp(stat.st_mtime, timezone.utc)
        return local_date > remote_date and local_size > 1000


def collect_annual_downloads(destination: str, path: str,
                             contiguous_year_segment: List, parameters: List,
                             downloads: Dict):
    if not parameters:
        target = destination_path(destination, path)
    else:
        f = path[:-4] + '_' + '_'.join(map(str, parameters)) + ".csv.gz"
        target = os.path.join(destination, f)

    downloads[target] = {}
    for year in contiguous_year_segment:
        url = BASE_AQS_EPA_URL + ANNUAL_URI.format(year=year)
        pp = [int(p) for p in parameters]
        downloads[target][url] = pp


def collect_daily_downloads(destination: str, ylabel: str,
                             contiguous_year_segment: List, parameter,
                             downloads: Dict):
    if isinstance(parameter, Parameter) or parameter in Parameter.values():
        p = Parameter(parameter)
    else:
        p = int(parameter)
    path = DAILY_URI.format(parameter=p, year=ylabel)
    target = destination_path(destination, path)

    downloads[target] = {}
    for year in contiguous_year_segment:
        url = BASE_AQS_EPA_URL + DAILY_URI.format(parameter=int(parameter),
                                                  year=year)
        downloads[target][url] = None


def download_aqs_data (aggregation: Aggregation,
                       years: List,
                       destination: str,
                       parameters: List,
                       merge_years: bool = False):
    if aggregation == Aggregation.DAILY:
        assert len(parameters) > 0

    years = sorted(years)
    segment = [years[0]]
    contiguous_years = [segment]
    for i in range(1, len(years)):
        if merge_years and years[i-1] == years[i] - 1:
            segment.append(years[i])
        else:
            segment = [years[i]]
            contiguous_years.append(segment)

    if parameters:
        parameters = sorted(parameters)

    downloads = {}
    for segment in contiguous_years:
        if len(segment) == 1:
            y = str(segment[0])
        else:
            y = "{}-{}".format(segment[0], segment[-1])

        if aggregation == Aggregation.ANNUAL:
            path = ANNUAL_URI.format(year=y)
            collect_annual_downloads(destination, path, segment, parameters,
                                     downloads)
        elif aggregation == Aggregation.DAILY:
            for parameter in parameters:
                collect_daily_downloads(destination, y, segment, parameter,
                                        downloads)

    for target in downloads:
        urls = downloads[target]
        cached = True
        for url in urls:
            if not is_downloaded(url, target):
                cached = False
                break
        if cached:
            continue
        append = False
        for url in urls:
            p = urls[url]
            download_data(url, target, p, append)
            append = True
    return


def main(args):
    years = []
    for y in args.years:
        if ':' in y:
            x = y.split(':')
            y1 = int(x[0])
            y2 = int(x[1])
            years += range(y1, y2+1)
        else:
            years.append(int(y))
    aggregation = Aggregation(args.aggregation)
    parameters = []
    if args.parameters:
        for p in args.parameters:
            if p.isnumeric():
                parameters.append(int(p))
            else:
                parameters.append(enum(Parameter, p))

    print("Will download EPA AQS data for: ")
    print("\tYears: [" + ", ".join(map(str, years)) + "]")
    if parameters:
        print("\tParameters: [" + ", ".join(map(str, parameters)) + "]")
    else:
        print("\tAll parameters")
    print("\taggregated: " + aggregation.value)
    print("To: " + args.dest)

    download_aqs_data(aggregation, years, args.dest, parameters, args.merge_years)
#    download_aqs_data(Aggregation.DAILY, [2012], "/tmp", [Parameter.PM25])


if __name__ == '__main__':
    a_choices = [a.value for a in Aggregation]
    parser = argparse.ArgumentParser (description=__doc__)
    parser.add_argument("--years", "-y",
                        default="1990:2020",
                        nargs="*",
                        help="Year or list of years to download, " +
                             "default: 1990:2020",
                        required=False)
    parser.add_argument("--aggregation", "-a",
                        choices=a_choices,
                        default="annual",
                        help="Whether to use annual or daily aggregation," +
                             " default: annual",
                        required=False)
    parser.add_argument("--parameters", "-p",
                        nargs = "*",
                        help = "Parameter(s) to download, allowed values: " +
                            ",".join(list(map(str, Parameter))) +
                            " or integer codes. " +
                            "Required for daily data, for annual data " +
                            "defaults to all.",
                        required = False)
    parser.add_argument("--dest", "-d",
                        help="Directory to place the downloaded files",
                        required=True)
    parser.add_argument("--merge_years", action='store_true',
                        help="concatenate consecutive years in one file",
                        required=False)
    main(parser.parse_args())
