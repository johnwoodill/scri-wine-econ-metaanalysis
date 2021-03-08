from lib.prism import download_prism, get_prism_lookup, get_prism_scri_lookup, proc_prism
from dask import compute
# from dask.distributed import Client, progress
import dask.dataframe as dd
import pandas as pd



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