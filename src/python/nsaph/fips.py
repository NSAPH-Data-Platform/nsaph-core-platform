'''
SELECT
    state_id,
    MIN(county_fips)/1000 AS fips
FROM
    public.us_iso
GROUP BY state_id
ORDER BY 2
'''
import json

fips_list = [
    {"state_id":"AL", "fips":1},
    {"state_id":"AK", "fips":2},
    {"state_id":"AZ", "fips":4},
    {"state_id":"AR", "fips":5},
    {"state_id":"CA", "fips":6},
    {"state_id":"OR", "fips":6},
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
    {"state_id":"PR", "fips":72}
]


fips_dict = {
    x["state_id"]: x["fips"] for x in fips_list
}


if __name__ == '__main__':
    print(json.dumps(fips_dict, indent=2))
