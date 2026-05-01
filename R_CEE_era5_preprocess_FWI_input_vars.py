'''
preprocess era5 FWI input variables 2001-2020, CEE revision R1
!!!
Outputs of this scripts are copied to /net/rain/hyclimm/data/projects/SynFire/WP1/era5_meteo_vars/
'''

import xarray as xr
import numpy as np
import pandas as pd
import os
import multiprocessing as mp

#------------------------------------------
def preprocess_era5_FWI_input_vars(var):

    '''
    prepare era5 data for fire weather index calculation
    convert longitude from (0-360) to (-180, 180)
    crop to European domain

    parameters
    var: str
    'tasmax', 'hurs', 'pr', 'sfcWind'

    returns
    ds: xarray.dataset
    one dataset per variable from 2001 to 2020.
    precipitation flux converted to amount
    '''

    flist = []

    for yr in range(2001, 2021):

        if var == 'pr':
            
            indir = '/net/atmos/data/era5_cds/processed/v1/tp/day/native/'   #use this directory to keep consistency with Raul
            
            for r, d, f in os.walk(indir):
                for fn in f:
                    if f'_{yr}.nc' in fn:
                        flist.append(os.path.join(indir, fn))
            
            
        else:
            
            indir = f'/net/atmos/data/era5_cds/processed/v2/{var}/day/native/{yr}/'  #pr data under /v2/ has unit issues.
    
            for r, d, f in os.walk(indir):
                for fn in f:
                    if '.nc' in fn:
                        flist.append(os.path.join(indir, fn))

    if var == 'pr':
        assert len(flist) == 20
    else:
        assert len(flist) == 240

    var_stack = xr.open_mfdataset(paths = flist)
    var_stack = var_stack.compute()

    #make sure time is ordered
    var_stack = var_stack.sortby('time')
    

    #crop to analysis domain
    var_stack = var_stack.assign_coords(lon=(((var_stack.lon + 180) % 360) - 180)).sortby('lon')
    var_stack = var_stack.sel(lat = slice(72, 34), lon = slice(-11, 33))

    #drop time_bnds:
    if 'time_bnds' in var_stack.data_vars:
        var_stack = var_stack.drop_vars(['time_bnds'])

    #unit conversion for precipitation
    if 'tp' in var_stack.data_vars:
        var_stack['tp'] = var_stack['tp'] * 1000 #daily accumulated precipitation [mm] (the original unit should be m/day, to get mm/day we need to * 1000, confirmed by Raul)
        var_stack = var_stack.rename({'tp': 'pr'})
        var_stack['pr'].attrs['units'] = 'kg m-2'
        var_stack['pr'].attrs['long_name'] = 'total precipitation'

    #save
    var_stack.to_netcdf(f'/net/rain/hyclimm_nobackup/lixinh/era5_FWI_input_vars_2001_2020/era5_{var}_day_Europe_2001-01-01_2020-12-31.nc',
                        encoding = {v: {'zlib': True, 'complevel': 3} for v in var_stack.variables})

#------------------------------------------
'''
with mp.Pool(4) as pool:
    pool.map(preprocess_era5_FWI_input_vars, ['tasmax', 'hurs', 'pr', 'sfcWind'])
'''

# rerun precipitation
preprocess_era5_FWI_input_vars(var = 'pr')
