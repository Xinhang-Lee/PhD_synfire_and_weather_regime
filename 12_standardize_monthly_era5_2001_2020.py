'''
This script standardizes ERA5-derived variables for each month empirically, from 2001-2020, in a pixel-wise manner.
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

#--------------------------------------------
#specify era5 variables
var_list = ['tasmax', 'hurs', 'sfcWind', 'pr', 'fwi']
var_tied = ['pr']

#--------------------------------------------
#input directories
indir = {
    'tasmax': '/net/rain/hyclimm/data/projects/SynFire/WP1/era5_meteo_vars/',
    'hurs': '/net/rain/hyclimm/data/projects/SynFire/WP1/era5_meteo_vars/',
    'sfcWind': '/net/rain/hyclimm/data/projects/SynFire/WP1/era5_meteo_vars/',
    'pr': '/net/rain/hyclimm/data/projects/SynFire/WP1/era5_meteo_vars/',
    'fwi': '/net/rain/hyclimm/data/projects/SynFire/WP1/era5_meteo_vars/era5/fwi_daily_era5_variables_hurs-tasmax_dryingfactor_original_daylength_continuous_overwintering_original_2001-01-01_2020-12-31.nc'
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
    for daily cumulated precipitation
    parameter
    ts: 1D numpy array, values of a grid cell along the time axis
    '''

    ts = ts.copy()
    
    if np.isnan(ts).all(): 
        return ts  
    else:    
        length = len(ts[~np.isnan(ts)])

        #handle negative precipitation
        ts[ts < 0] = 0
        
        #remove 0 ties by adding random noise
        mu, sigma = 0, 1e-27
        random_small_numbers = np.random.normal(mu, sigma, 1000)
        ts[ts == 0] = np.random.choice(random_small_numbers, np.sum(ts == 0))
    
        ranks = rankdata(ts, method = 'average', nan_policy = 'omit')
        pcts = ranks/(length + 1)
    
        return norm.ppf(pcts, loc = 0, scale = 1)


#--------------------------------------------
def std_mon_era5(mon, varname):
    
    '''
    standardize daily era5 data for each month separately
    
    parameters:
    mon: month indicator 1(Jan), 2(Feb), ... 12(Dec)
    varname: variable name
    '''

    #load data
    if varname == 'fwi':
        era5_mon = xr.open_mfdataset(paths = indir[varname],
                                     preprocess = lambda ds: ds.sel(time = ds['time'].dt.month == mon))
    else:
        era5_mon = xr.open_mfdataset(paths = os.path.join(indir[varname], f'era5_{varname}_day_Europe_2001-01-01_2020-12-31.nc'),
                                     preprocess = lambda ds: ds.sel(time = ds['time'].dt.month == mon))
    
    
    era5_mon = era5_mon.compute()

    result = xr.apply_ufunc(
        empirically_standardize_with_random_zeros if varname in var_tied else empirically_standardize,          # function to apply (adjusted based on variables)
        era5_mon[varname],       # xarray DataArray
        input_core_dims=[['time']],      # function takes 1D along 'time'
        output_core_dims=[['time']],     # function returns 1D along 'time'
        vectorize=True,                  # apply to every (y, x) location
        dask='forbidden',         
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
        
        nc_list = pool.starmap(std_mon_era5, [(m, var) for m in range(1, 13)])
        nc_stacked = xr.concat(nc_list, dim = 'time')
        nc_stacked_sorted = nc_stacked.sortby('time')
        
        #save
        nc_stacked_sorted.to_netcdf(f'/net/rain/hyclimm/data/projects/SynFire/WP1/standardize_monthly_era5_2001_2020/monthly_standardized_era5_original_grid_{var}_2001-01-01_2020-12-31.nc')
        
    print('end:', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
