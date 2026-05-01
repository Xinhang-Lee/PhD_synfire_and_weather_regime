'''
This script standardizes CERRA-derived variables for each month empirically, from 2001-2020, in a pixel-wise manner.
The empirical standardization takes two steps:
(1) Calculate the percentile ranks of a 1D array along the time axis. Equal values are given the same percentile ranks.
(2) Transform these percentile ranks to a standard normal distribution N(0, 1), using the inverse cumulative distribution function of a standard normal distribution N(0, 1).
For variables left-tied to zeros (e.g., total precipitation), a small random permutations (N(0, 1e-27)) are given to these zeros.
'''


import xarray as xr
import numpy as np
from scipy.stats import norm, rankdata
from datetime import datetime
import multiprocessing as mp
import os
import glob


#--------------------------------------------
#specify cerra variables
#var_list = ['2t', '2r', '10si', 'mx2t', 'fwi', 'tp']
var_list = ['tp']
var_tied = ['tp']

#--------------------------------------------
#input directories
indir = {
    '2t': '/net/atmos/data/cerra/processed/v1/2t/day/native',
    '2r': '/net/atmos/data/cerra/processed/v1/2r/day/native',
    '10si': '/net/atmos/data/cerra/processed/v1/10si/day/native',
    'mx2t': '/net/atmos/data/cerra/processed/v1/mx2t/day/native',
    'tp': '/net/atmos/data/cerra-land/original/tp', #cerra-land
    'fwi': '/net/rain/hyclimm/data/projects/fofix/derive_FWI_CERRA_Europe_original_grid/fwi_with_mx2t_cerra_original_grid_2001-01-01_2020-12-31_europe.nc'
}


#--------------------------------------------
#standardization functions for one grid cell

#without 0 ties
def empirically_standardize(ts):

    '''
    parameter
    ts: 1D numpy array, values of a grid cell along the time axis
    '''

    if np.isnan(ts).all():
        return ts
    else:
        length = len(ts[~np.isnan(ts)])
        ranks = rankdata(ts, method='average', nan_policy = 'omit')
        pcts = ranks/(length+1) #avoid inf and -inf
        
        return norm.ppf(pcts, loc=0, scale=1)

#tied to 0
def empirically_standardize_with_random_zeros(ts):

    '''
    parameter
    ts: 1D numpy array, values of a grid cell along the time axis
    '''

    ts = ts.copy()
    
    if np.isnan(ts).all(): 
        return ts  
    else:    
        length = len(ts[~np.isnan(ts)])
        
        #remove 0 ties by adding random noise
        mu, sigma = 0, 1e-27
        random_small_numbers = np.random.normal(mu, sigma, 1000)
        ts[ts == 0] = np.random.choice(random_small_numbers, np.sum(ts == 0))
    
        ranks = rankdata(ts, method = 'average', nan_policy = 'omit')
        pcts = ranks/(length + 1)
    
        return norm.ppf(pcts, loc = 0, scale = 1)


#--------------------------------------------
def std_mon_cerra(mon, varname):
    
    '''
    standardize daily cerra data for each month separately
    
    parameters:
    mon: month indicator 1(Jan), 2(Feb), ... 12(Dec)
    varname: variable name
    '''

    #load monthly values
    if varname == 'fwi':
        cerra = xr.open_dataset(indir[varname])
        cerra_mon = cerra.sel(time = cerra['time'].dt.month == mon)
    else:
        cerra_mon = xr.open_mfdataset(paths = [os.path.abspath(f) for y in range(2001, 2021) for f in glob.glob(f"{indir[varname]}/*{y}.nc")], preprocess = lambda ds: ds.sel(time = ds['time'].dt.month == mon))
    

    #chunk into 16 subsets (with 'time' dimension intact), and only keep dimensions (time, x, y)
    if 'height' in cerra_mon.dims:
        cerra_mon = cerra_mon.squeeze('height')

    cerra_mon_xyt_16 = cerra_mon.chunk({'x': 268, 'y': 268, 'time': -1})

    result = xr.apply_ufunc(
        empirically_standardize_with_random_zeros if varname in var_tied else empirically_standardize,          # function to apply (adjusted based on variables)
        cerra_mon_xyt_16[varname],       # xarray DataArray
        input_core_dims=[['time']],      # function takes 1D along 'time'
        output_core_dims=[['time']],     # function returns 1D along 'time'
        vectorize=True,                  # apply to every (y, x) location
        dask='parallelized',         
        keep_attrs = True,
        output_dtypes=[float],           # required for Dask
    )
    
    result = result.to_dataset()
    
    return result


#--------------------------------------------
#parallelize over 12 months, loop over variables
for var in var_list:

    print(var)
    print('start:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    with mp.Pool(12) as pool:
        
        nc_list = pool.starmap(std_mon_cerra, [(m, var) for m in range(1, 13)])
        nc_stacked = xr.concat(nc_list, dim = 'time')
        nc_stacked_sorted = nc_stacked.sortby('time')
        
        #save
        nc_stacked_sorted.to_netcdf(f'/net/rain/hyclimm/data/projects/SynFire/WP1/standardize_monthly_cerra_2001_2020/monthly_standardized_cerra_original_grid_{var}_2001-01-01_2020-12-31.nc')
        
    print('end:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
