"""
US State FIPS codes, represented as Python dictionary

Source: https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/technical/nra/nri/results/?cid=nrcs143_013696

Can be also used from command line to create JSON array of state FIPS codes
"""

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

import json

QUERY="""
SELECT
    state_id,
    MIN(county_fips)/1000 AS fips
FROM
    public.us_iso
GROUP BY state_id
ORDER BY 2
"""

fips_list = [
    {"state_id":"AL", "fips":1},
    {"state_id":"AK", "fips":2},
    {"state_id":"AZ", "fips":4},
    {"state_id":"AR", "fips":5},
    {"state_id":"CA", "fips":6},
    {"state_id":"CO", "fips":8},
    {"state_id":"CT", "fips":9},
    {"state_id":"DE", "fips":10},
    {"state_id":"DC", "fips":11},
    {"state_id":"FL", "fips":12},
    {"state_id":"GA", "fips":13},
    {"state_id":"HI", "fips":15},
    {"state_id":"ID", "fips":16},
    {"state_id":"IL", "fips":17},
    {"state_id":"IN", "fips":18},
    {"state_id":"IA", "fips":19},
    {"state_id":"KS", "fips":20},
    {"state_id":"KY", "fips":21},
    {"state_id":"LA", "fips":22},
    {"state_id":"ME", "fips":23},
    {"state_id":"MD", "fips":24},
    {"state_id":"MA", "fips":25},
    {"state_id":"MI", "fips":26},
    {"state_id":"MN", "fips":27},
    {"state_id":"MS", "fips":28},
    {"state_id":"MO", "fips":29},
    {"state_id":"MT", "fips":30},
    {"state_id":"NE", "fips":31},
    {"state_id":"NV", "fips":32},
    {"state_id":"NH", "fips":33},
    {"state_id":"NJ", "fips":34},
    {"state_id":"NM", "fips":35},
    {"state_id":"NY", "fips":36},
    {"state_id":"NC", "fips":37},
    {"state_id":"ND", "fips":38},
    {"state_id":"OH", "fips":39},
    {"state_id":"OK", "fips":40},
    {"state_id":"OR", "fips":41},
    {"state_id":"PA", "fips":42},
    {"state_id":"RI", "fips":44},
    {"state_id":"SC", "fips":45},
    {"state_id":"SD", "fips":46},
    {"state_id":"TN", "fips":47},
    {"state_id":"TX", "fips":48},
    {"state_id":"UT", "fips":49},
    {"state_id":"VT", "fips":50},
    {"state_id":"VA", "fips":51},
    {"state_id":"WA", "fips":53},
    {"state_id":"WV", "fips":54},
    {"state_id":"WI", "fips":55},
    {"state_id":"WY", "fips":56},
    {"state_id":"AS", "fips":60}, #American Samoa
    {"state_id":"GU", "fips":66}, # Guam
    {"state_id":"MP", "fips":69}, # Northern Mariana Islands
    {"state_id":"PR", "fips":72},
    {"state_id":"VI", "fips":78} #  Virgin Islands
]


fips_dict = {
    x["state_id"]: x["fips"] for x in fips_list
}


if __name__ == '__main__':
    print(json.dumps(fips_dict, indent=2))
