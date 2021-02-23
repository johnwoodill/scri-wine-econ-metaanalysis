import numpy as np
import pandas as pd
import os
import wget
import zipfile
import gdal as gdal
import glob
import multiprocessing
from dask.delayed import delayed
from dask import compute
from dask.distributed import Client, progress

def order_dist(x1, y1):
    '''
    Args:
        x1: 
        y1: 

    Returns:
        Distance to constant point for consistent ordering
    '''
    x2 = -67.582908
    y2 = 47.577537
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)



# Get PRISM data
def proc_PRISMbil(ThisCol, ThisRow, bil_path):
    '''
    Args:
        ThisCol: column from raster file to parse
        ThisRow: row from raster file to parse
        bil_path: path to raster file
    
    Returns:
        Parsed grid_id, X, Y, PRISM data
    '''
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



def get_PRISMbil(dat, force=True):
    '''
    Args:
        dat: input data
        force: force download even if file exists

    Returns:
        0 if processing occured
        1 if error "Failed: {date} - {var}"
    '''
    # print(dat)
    date = dat.date.iat[0]
    var = dat['var'].iat[0]
    try: 
        # Get date info
        day_of_year = date.timetuple().tm_yday
        week_of_year = int(round(day_of_year/7, 0) + 1)
        date = date.strftime("%Y%m%d")
        
        # File to get grids lat/lon and value
        zip_filename = f"PRISM_{var}_stable_4kmD2_{date}_bil.zip"
        file_path = f"tmp/{zip_filename}"
        bil_path = f"tmp/PRISM_{var}_stable_4kmD2_{date}_bil.bil"
        save_filename = f"PRISM_{var}_{date}.csv"
        year = date[0:4]

        # Check if file exists and download if not
        if not os.path.exists(f"dat/proc_prism/{save_filename}") or force==True:
            url = f"ftp://prism.nacse.org/daily/{var}/{year}/{zip_filename}"
            wget.download(url, 'tmp/', bar=False)  

            # Unzip files
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall('tmp/')

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
            outdat = pd.DataFrame({'date': date, 'year': year, 'day_of_year': day_of_year, 'week_of_year': week_of_year, 
                                'gridNumber': gridNumber, 'var': var, 'value': value, 'lon': lon, 'lat': lat})
            
            # Sort grids based on north-east data point (consistency from cyano)
            outdat = outdat[outdat['gridNumber'] != -9999].sort_values(['lon'], ascending=[False])
            outdat = outdat[outdat['gridNumber'] != -9999].sort_values(['lat'], ascending=[False])
            outdat.loc[:, 'dist'] = outdat.apply(lambda x: order_dist(x['lon'], x['lat']), axis = 1)
            outdat = outdat.sort_values('dist')
            outdat.loc[:, 'gridNumber'] = np.linspace(1, len(outdat), len(outdat))
            outdat = outdat[['date', 'year', 'gridNumber', 'var', 'value', 'lon', 'lat']].reset_index(drop=True)
            
            # Save
            print(f"Saving: {save_filename}")
            
            outdat.to_csv(f"dat/proc_prism/{save_filename}", index=False)

            # Trash collecting
            directory='tmp/'
            os.chdir(directory)
            files = glob.glob(f"PRISM_{var}_stable_4kmD2_{date}*")
            for filename in files:
                os.unlink(filename)
            os.chdir('../')    
        return 0
    except:
        print(f"Failed: {date} - {var}")
        return 1



def post_PRISM(filename):
    '''
    Args:
        filename: PRISM filename to process
        
    Returns:
        Saves processed file 
    '''
    out_filename = os.path.basename(filename)
    # os.system("taskset -p 0xfffff %d" % os.getpid())
    if not os.path.exists(f"data/prism_temp/post_processed/{out_filename}"):
        try:
            dat = pd.read_csv(filename, index_col=False)
            dat = dat[dat['gridNumber'].isin(cyano_grids)]
            dat = dat.reset_index(drop=True)
            dat.to_csv(f"data/prism_temp/post_processed/{out_filename}", index=False)
            print(f"Saving: {out_filename}")
        except:
            print(f"Failed: {out_filename}")
            with open("../log.txt", "a") as f:
                f.write(f"\n Failed: {filename}")



if __name__ == "__main__":

    # NCORES = multiprocessing.cpu_count() - 1
    NCORES = 1
    
    ### [1] Download, process, and save all PRISM data
    beg_dat = "2008-01-01"
    end_dat = "2008-01-03"
    
    date_range = pd.date_range(beg_dat, end_dat)
    
    var = ["tmax", "tmin", "ppt", "tdmean", "vpdmax", "vpdmin"]

    lst_ = [(x, y) for x in date_range for y in var]
    date = [x[0] for x in lst_]
    var = [x[1] for x in lst_]

    indf = pd.DataFrame({'date': date, 'var': var}).reset_index()
    
    gb = indf.groupby('index')
    gb_i = [gb.get_group(x) for x in gb.groups]

    # dask.config.set({'temporary_directory': 'dask_tmp/'})
    client = Client(n_workers=NCORES, threads_per_worker=1)
    
    # gb_i = gb_i[0:50]

    # Dash implementation
    compute([delayed(get_PRISMbil)(thisRow) for thisRow in gb_i])

    # Standard list compressions
    # [get_PRISMbil(thisRow) for thisRow in gb_i]

    ### [2] Data check

    ## Get each file and remove gridNumbers not needed
    cyano_dat = pd.read_csv("../data/full_lake_info.csv")
    cyano_grids = cyano_dat['gridNumbers'].unique()
    len(cyano_grids)   #10064

    del cyano_dat

    # ### Get files in dir
    files = glob.glob(f"data/prism_temp/processed/PRISM_*.csv")

    client = Client(n_workers=NCORES, threads_per_worker=1)
    
    compute([delayed(post_PRISM)(file_) for file_ in files])

    ### Parallel Post-process with multipleprocessing (alt to Dask)
    # pool = multiprocessing.Pool(NCORES)
    # pool.map(post_PRISM, files)
    # pool.close()

    ### [3] Data check
    # ### Check all files process
    print("Checking all files were processed")
    infiles = glob.glob(f"data/prism_temp/processed/PRISM_*.csv")
    outfiles = glob.glob(f"data/prism_temp/post_processed/*.csv")

    infilenames = [os.path.basename(x) for x in infiles]
    outfilenames = [os.path.basename(x) for x in outfiles]

    missing = set(infilenames).difference(outfilenames)
    print("Missing....")
    print(missing)

    lst_ = []
    ### Bind all PRISM data
    for file_ in outfiles:
        df = pd.read_csv(file_, index_col=False)
        lst_.append(df)

    full_prism = pd.concat(lst_, axis=0, ignore_index=True)

    ### Revert date
    dates = pd.to_datetime(full_prism['date'], format='%Y%m%d')
    full_prism = full_prism.assign(date = dates)

    ### Reorder columns
    full_prism = full_prism[['date', 'day_of_year', 'week_of_year',
        'gridNumber', 'var', 'value', 'lon', 'lat']]

    full_prism.to_csv("../data/full_prism.csv", index=False)

    print("Saving: ../data/full_prism.csv")

    ### [4] Get degree days
    full_prism = pd.read_csv("../data/full_prism.csv", index_col=False)