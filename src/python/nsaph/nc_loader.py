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

import netCDF4 as nc
import random

if __name__ == '__main__':
    fn = '/Users/misha/harvard/projects/data_server/nsaph/local_data/V4NA03_PM25_NA_200001_200012-RH35.nc'
    ds = nc.Dataset(fn)
    print(ds)

    pm25 = ds["PM25"]
    lat = ds["LAT"]
    lon = ds["LON"]
    random.seed(0)
    for i in range(0, 20):
        lo = random.randrange(0, len(lon))
        la = random.randrange(0, len(lat))
        p = pm25[la, lo]
        print("[{:d},{:d}]: ({:f}, {:f}) pm25={:f}".format(lo, la, lat[la], lon[lo], p))
