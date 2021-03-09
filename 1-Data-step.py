from lib.prism import download_prism, get_prism_lookup, get_prism_scri_lookup, proc_prism
from dask import compute
# from dask.distributed import Client, progress
import dask.dataframe as dd
import pandas as pd
import nonlineartemppy.calculations as nltemp
import glob


if __name__ == "__main__":

    print("[1/] Building PRISM lookup table and download all grids")
    # Build prism lookup grids
    # get_prism_lookup()

    # Download prism data
    # download_prism()

    print("[2/] Get SCRI Farms and PRISM Lookup table")
    # Get scri farms and prism lookup
    # scri_farm_data = pd.read_csv("data/scri_farms_lat_lon.csv")
    # prism_data = pd.read_csv("data/prism-grids.csv")
    # farm_lookup = scri_farm_data.groupby(['Company', 'Vineyard']).apply(lambda x: get_prism_scri_lookup(x['Lat'], x['Lon'], prism_data)).reset_index()
    # farm_lookup = farm_lookup.drop(columns={'level_2'})
    # # save farm_lookup table
    # farm_lookup.to_csv('data/scri_farm_prism_lookup.csv', index=False)    

    print("[3/] Process PRISM grids")

    # Parse prism data 
    farm_lookup = pd.read_csv('data/scri_farm_prism_lookup.csv') 

    year_ = ["2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"]
    var_ = ["tmax", "tmin", "ppt", "tdmean", "vpdmax", "vpdmin"]

    # year_ = ["2018"]      
    # var_ = ["ppt"]

    lst_ = [(x, y) for x in year_ for y in var_]
    year = [x[0] for x in lst_]
    var = [x[1] for x in lst_]
    indf = pd.DataFrame({'year': year, 'var': var}).reset_index()

    indf_dd = dd.from_pandas(indf, npartitions=10)
    res = indf_dd.groupby('index').apply(lambda x: proc_prism(x, farm_lookup['gridNumber']), meta='f8').compute(scheduler='processes')
    
    print("[4/] Bind PRISM Data")
    
    prism_files = glob.glob("tmp/prism_processed/*.csv")
    
    lst_ = []
    for file_ in prism_files:
        df = pd.read_csv(file_)
        lst_.append(df)
    
    prism_dat = pd.concat(lst_)    
    prism_dat = prism_dat.sort_values('date')
    prism_dat = prism_dat.set_index(['date', 'gridNumber', 'lat', 'lon', 'var']).unstack('var').reset_index()
    prism_dat.columns = ['date', 'gridNumber', 'lat', 'lon', 'ppt', 'tmax', 'tmin', 'vpdmax', 'vpdmin']
    prism_dat
    
    # Get degree days from 0-40C
    prism_dat_dd = nltemp.degree_days(prism_dat, range(0, 40)).reset_index(drop=True)
    prism_dat_dd.to_csv('data/full_prism_degree_days.csv', index=False)
    
    # Get degree days from 0-40C
    prism_dat_td = nltemp.degree_time(prism_dat, range(0, 40)).reset_index(drop=True)
    prism_dat_td.to_csv('data/full_prism_degree_time.csv', index=False)