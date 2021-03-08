import numpy as np
import pandas as pd
import os
import wget
import zipfile
import gdal as gdal
import glob
import multiprocessing
from dask.delayed import delayed
import dask.dataframe as ddf
from dask import compute
from dask.distributed import Client, progress
import dask.dataframe as dd
import glob
import subprocess
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate haversine distance between two points
    
    Args:
        lon1: longitude point 1
        lat1: latitude point 1
        lon2: lonitude point 2
        lat2: latitude point 2
    
    Returns:
        Calculate the great circle distance between two points 
        on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    return km


# Get PRISM data
def proc_PRISMbil(ThisCol, ThisRow, bil_path):
    """
    Args:
        ThisCol: column from raster file to parse
        ThisRow: row from raster file to parse
        bil_path: path to raster file
    
    Returns:
        Parsed grid_id, X, Y, PRISM data
    """
    # Raster file setup
    ncol = 1405
    nrow = 621

    ds       = gdal.OpenShared(bil_path)
    GeoTrans = ds.GetGeoTransform()
    ColRange = range(ds.RasterXSize)
    RowRange = range(ds.RasterYSize)
    rBand    = ds.GetRasterBand(1) # first band
    nData    = rBand.GetNoDataValue()
    if nData == None:
        nData = -9999 # set it to something if not set

    # specify the centre offset
    HalfX    = GeoTrans[1] / 2
    HalfY    = GeoTrans[5] / 2

    # Get parsed data
    RowData = rBand.ReadAsArray(0, ThisRow, ds.RasterXSize, 1)[0]
    if RowData[ThisCol] >= -100:   # Greater than minTemp -100 C
        X = GeoTrans[0] + ( ThisCol * GeoTrans[1] )
        Y = GeoTrans[3] + ( ThisRow * GeoTrans[5] )
        X += HalfX
        Y += HalfY
        grid_id = ((ThisRow-1)*ncol)+ThisCol
        return (grid_id, X, Y, RowData[ThisCol])
    else:
        return (-9999, -9999, -9999, -9999)


def proc_prism(dat, scri_farm_grids):
    prism_var = dat['var'].iat[0]
    prism_year = dat['year'].iat[0]
    
    print(f"Processing: {prism_year} - {prism_var}")
    
    file_path = sorted(glob.glob(f"tmp/prism.nacse.org/daily/{prism_var}/{prism_year}/*_bil.zip"))
    
    for file_ in file_path:
        subprocess.call(['unzip', '-o', file_, '-d', 'tmp/dump/'], stdout=subprocess.DEVNULL) 
    
    files_ = glob.glob(f"tmp/dump/PRISM_{prism_var}_stable_4kmD2_{prism_year}*_bil.bil")

    for bil_path in files_:
        # bil_path = files_[0]
        # Process raster
        ds       = gdal.OpenShared(bil_path)
        ColRange = range(ds.RasterXSize)
        RowRange = range(ds.RasterYSize)

        # List compress grids and data
        lst = [proc_PRISMbil(x, y, bil_path) for x in ColRange for y in RowRange]

        # Build df of lst compression
        gridNumber = [x[0] for x in lst]
        lon = [x[1] for x in lst]
        lat = [x[2] for x in lst]
        value = [x[3] for x in lst]
        prism_date =  bil_path[-16:-8]        

        outdat = pd.DataFrame({'date': prism_date, 'gridNumber': gridNumber, 'var': prism_var, 'value': value, 'lon': lon, 'lat': lat})
        
        # Remove NA
        outdat = outdat[outdat['gridNumber'] != -9999].sort_values(['lon'], ascending=[False])
        outdat = outdat[outdat['gridNumber'] != -9999].sort_values(['lat'], ascending=[False])
        outdat = outdat[['date', 'gridNumber', 'var', 'value', 'lon', 'lat']].reset_index(drop=True)
        
        # Filter nearest farms lat and long and save
        print(f"Saving: tmp/prism_processed/prism_{prism_date}_{prism_var}.csv")
        outdat = outdat[outdat.gridNumber.isin(scri_farm_grids)].reset_index(drop=True)
        outdat.to_csv(f"tmp/prism_processed/prism_{prism_date}_{prism_var}.csv", index=False)

    # Trash collecting
    print("Trashing cleaning")
    del_files = glob.glob(f"tmp/dump/PRISM_{prism_var}_stable_4kmD2_{prism_year}*")
    for filename in del_files:
        os.remove(filename)
    return 0




def get_prism_lookup():
    # File to get grids lat/lon
    print("[1/5] Download file PRISM_ppt_stable_4kmD2_20100101_bil.zip")
    filename = 'PRISM_ppt_stable_4kmD2_20100101_bil.zip'
    file_path = f"tmp/{filename}"
    bil_path = 'tmp/PRISM_ppt_stable_4kmD2_20100101_bil.bil'

    # Check if file exists and download if not
    link = 'ftp://prism.nacse.org/daily/ppt/2010/'
    url = f"{link}{filename}"
    wget.download(url, 'tmp/')  

    print("[2/5] Unzippin PRISM_ppt_stable_4kmD2_20100101_bil.zip")
    # Unzip files
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        print(f"[2/5] Unzipping: {filename}")
        zip_ref.extractall('tmp/')

    print("[3/5] Processing PRISM grids")
    ds = gdal.OpenShared(bil_path)
    ColRange = range(ds.RasterXSize)
    RowRange = range(ds.RasterYSize)

    # List compress grids
    lst = [proc_PRISMbil(x, y, bil_path) for x in ColRange for y in RowRange]

    gridNumber = [x[0] for x in lst]
    lon = [x[1] for x in lst]
    lat = [x[2] for x in lst]
    outdat = pd.DataFrame({'gridNumber': gridNumber, 'lon': lon, 'lat': lat})
    outdat = outdat[outdat['gridNumber'] != -9999].sort_values(['lon'], ascending=[False])
    outdat = outdat[outdat['gridNumber'] != -9999].sort_values(['lat'], ascending=[False])
    outdat = outdat[['gridNumber', 'lon', 'lat']].reset_index(drop=True)

    # Save
    print(f"[4/5] Saving to data/prism-grids.csv")
    outdat.to_csv("data/prism-grids.csv", index=False)

    print(f"[5/5] Cleaning up dir data/prism_temp")
    # Trash collecting
    files = glob.glob('tmp/PRISM_ppt_stable_4kmD2_20100101*')
    for filename in files:
        os.remove(filename)




def download_prism():
    os.system('sh download_prism.sh')




def get_prism_scri_lookup(lat, lon, prism_data):    
    lat = float(lat)
    lon=float(lon)
    
    prism_data = prism_data[(prism_data['lon'] >= lon - 1) & (prism_data['lon'] <= lon + 1)]
    prism_data = prism_data[(prism_data['lat'] >= lat - 1) & (prism_data['lat'] <= lat + 1)]
    
    dist = prism_data.groupby('gridNumber').apply(lambda x: haversine(lon, lat, x['lon'], x['lat'])).reset_index()
    dist.columns = ['gridNumber', 'dist']
    dist = dist.sort_values('dist').reset_index()
    return pd.DataFrame({'farm_lat': [lat], 'farm_lon': [lon], 'gridNumber': [dist.gridNumber.iat[0]], 'dist': [dist.dist.iat[0]]})