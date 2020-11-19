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
