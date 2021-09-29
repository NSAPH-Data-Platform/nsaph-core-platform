"""
In this module we attempt to analyze duplicate records in CMS medicaid data

Original R function to remove duplicates:
[de_duplicate](https://github.com/NSAPH/NSAPHplatform/blob/master/R/intake.R#L29-L45)

selects a random record from a set of duplicate records

Original R code that has been used to create Demographics files:
https://github.com/NSAPH/data_model/blob/master/scripts/medicaid_scripts/processed_data/1_create_demographics_data.R

calls de_duplicate function:

    de_duplicate(demographics, "BENE_ID", seed = 987987)


If I understand this code correctly, it:

* Removes all beneficiaries that have multiple records with inconsistent death dates (el_dod)
    (https://github.com/NSAPH/data_model/blob/master/scripts/medicaid_scripts/processed_data/1_create_demographics_data.R#L50)
    See also https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd#varying-death-dates
* For remaining beneficiaries with multiple records, randomly selects a single record
    (https://github.com/NSAPH/data_model/blob/master/scripts/medicaid_scripts/processed_data/1_create_demographics_data.R#L44)
    as stated here: https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd#variation-in-demographic-information-across-years

More details about duplicates:

* Within states: https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd#duplicates-within-states
* Across states: https://github.com/NSAPH/data_requests/tree/master/request_projects/dec2019_medicaid_platform_cvd#duplicates-across-states

Official documentation
https://www2.ccwdata.org/documents/10280/19002246/ccw-max-user-guide.pdf

Section Assignment of a Beneficiary Identifier

To construct the CCW BENE_ID, the CMS CCW team developed an internal cross-reference file
consisting of historical Medicaid and Medicare enrollment information using CMS data sources
such as the Enterprise Cross Reference (ECR) file. When a new MAX PS file is received, the
MSIS_ID, STATE_CD, SSN, DOB, Gender and other beneficiary identifying information is compared
against the historical enrollment file. If there is a single record in the historical enrollment file
that “best matches” the information in the MAX PS record, then the BENE_ID on that historical
record is assigned to the MAX PS record. If there is no match or no “best match” after CCW has
exhausted a stringent matching process, a null (or missing) BENE_ID is assigned to the MAX PS
record. For any given year, approximately 7% to 8% of MAX PS records have a BENE_ID that is
null. Once a BENE_ID is assigned to a MAX PS record for a particular year (with the exception of
those assigned to a null value), it will not change. When a new MAX PS file is received, CCW
attempts to reassign those with missing BENE_IDs.

Also, see: https://resdac.org/cms-data/variables/encrypted-723-ccw-beneficiary-id

"""
import gzip
import json
import os.path
from argparse import ArgumentParser
from datetime import date, timedelta
from typing import Dict

from nsaph_utils.utils.io_utils import fopen

from nsaph.db import Connection

FIND_DUPLICATES = '''
SELECT
    bene_id,
    FILE,
    COUNT(*)
FROM
    cms.ps
WHERE  bene_id IS NOT NULL   
GROUP BY FILE, bene_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) desc
'''


EXPLORE_BENE = '''
SELECT
    *
FROM 
    cms.ps
WHERE bene_id = '{id}'    
'''


class DuplicatesExplorer:
    def __init__(self, arguments):
        self.arguments = arguments
        self.duplicates = None
        self.reset = arguments.reset
        self.duplicate_deaths = None
        self.duplicate_births = None
        self.change = False

    def init (self):
        if self.duplicates is not None:
            return
        if not self.reset and os.path.isfile(self.arguments.report):
            self.load()
            return
        with Connection(self.arguments.db, self.arguments.connection) as connection:
            with (connection.cursor()) as cursor:
                print("Examining database {}".format(self.arguments.connection))
                cursor.execute(FIND_DUPLICATES)
                self.duplicates = {row[0]: None for row in cursor}
            self.change = True
        self.save()
        return

    def explore_one(self, id, cursor):
        sql = EXPLORE_BENE.format(id=id)
        cursor.execute(sql)
        all_columns = [desc[0] for desc in cursor.description]
        data = [row for row in cursor]
        diff_columns = []
        for j in range(len(all_columns)):
            column = {row[j] for row in data}
            if len(column) > 1:
                diff_columns.append(j)
        report = {
            all_columns[j]:
                [str(row[j]) for row in data]
            for j in diff_columns
        }
        self.duplicates[id] = report
        self.change = True

    def explore_all(self):
        with Connection(self.arguments.db, self.arguments.connection) as connection:
            with (connection.cursor()) as cursor:
                n = 0
                for bene_id in self.duplicates:
                    if not self.duplicates[bene_id]:
                        self.explore_one(bene_id, cursor)
                        n += 1
                        if (n % 100) == 0:
                            print(n)

    def is_loaded(self):
        if self.duplicates is None:
            return False
        for bene_id in self.duplicates:
            if not self.duplicates[bene_id]:
                return False
        return True

    def load(self):
        with fopen(self.arguments.report, "rt") as f:
            self.duplicates = json.load(f)

    def save(self):
        fname = self.arguments.report
        if not fname.endswith(".gz"):
            fname += ".gz"
        if self.change:
            with gzip.open(fname, "wt") as f:
                json.dump(self.duplicates, f, indent=2)
        name = '.'.join(fname.split('.')[:-2])
        if self.duplicate_births is not None:
            with open(name + "_births.json", "wt") as f:
                json.dump(self.duplicate_births, f, indent=2)
        if self.duplicate_deaths is not None:
            with open(name + "_deaths.json", "wt") as f:
                json.dump(self.duplicate_deaths, f, indent=2)

    def report(self):
        self.init()
        print("Found {:d} duplicates.".format(len(self.duplicates)))
        if self.reset or not self.is_loaded():
            self.explore_all()
        self.duplicate_deaths = self.find_duplicate_dates("el_dod")
        self.duplicate_births = self.find_duplicate_dates("el_dob")
        self.save()

    def find_duplicate_dates(self, date_type) -> Dict:
        if date_type == "el_dod":
            keep_none = False
        else:
            keep_none = True
        report = dict()
        for bene_id in self.duplicates:
            columns = self.duplicates[bene_id]
            if date_type in columns:
                dates = columns[date_type]
                date_range = sorted({d for d in dates if keep_none or d != "None"})
                if len(date_range) < 2:
                    continue
                report[bene_id] = dict()
                report[bene_id]["range"] = [d for d in date_range]
                report[bene_id]["MSIS"] = {
                    columns["msis_id"][i]: dates[i]
                    for i in range(len(dates))
                }
        return report

    def analyze_inconsistent_age(self):
        self.init()
        report = self.find_duplicate_dates("el_dob")
        max_delta = timedelta()
        max_bene = None
        num_age = 0
        for bene_id in report:
            dates = sorted([date.fromisoformat(d) for d in report[bene_id]["range"] if d != "None"])
            if len(dates) < 2:
                continue
            delta = dates[-1] - dates[0]
            if delta.days > 365:
                num_age += 1
            if delta > max_delta:
                max_delta = delta
                max_bene = bene_id
        print("Max delta is {} for bene_id {}".format(str(max_delta), max_bene))
        print("Beneficiaries with difference in age more than 1 year: {:d}".format(num_age))



def run():
    arguments = args()
    explorer = DuplicatesExplorer(arguments)
    if arguments.action == "age":
        explorer.analyze_inconsistent_age()
    else:
        explorer.report()


def args():
    parser = ArgumentParser ("Utility to explore duplicates in CMS medicaid data")
    parser.add_argument("--db",
                        help="Path to a database connection parameters file",
                        default="database.ini",
                        required=False)
    parser.add_argument("--connection",
                        help="Section in the database connection parameters file",
                        default="nsaph2",
                        required=False)
    parser.add_argument("--report",
                        help="Path to a duplicates report file",
                        default="cms_ps_duplicates.json.gz",
                        required=False)
    parser.add_argument("--reset", action='store_true',
                        help="Force recreating duplicate report if it already exists")
    parser.add_argument("--action", default="report",
                        help="Force recreating duplicate report if it already exists")


    arguments = parser.parse_args()
    return arguments



if __name__ == '__main__':
    run()


