'''
Check meteorological conditions (maximum temperature, mean relative humidity, mean wind speed, sum precipitation and fwi) for days with no fire, regional fire(s), and synchronous fires, and for each region.
ERA5-based.
'''

import pandas as pd
import geopandas as gpd
import rioxarray
import xarray as xr
import multiprocessing as mp

def nofire_regfire_synfire_standardized_anomaly(reg, varname):

    
    #-----------------------------------------------------
    # load fire observations
    fire_obs = gpd.read_file('/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations_w_region.shp')
    fire_obs["start_date"] = pd.to_datetime(fire_obs["start_date"])

    # get fire_days and nofire_days as a list
    all_days = pd.date_range(start = "2001-01-01", end = "2020-12-31") 
    fire_days = list(set(fire_obs[fire_obs["region"] == reg]["start_date"]))  #unique days with fires
    nofire_days = [date for date in all_days if date not in fire_days]

    #stratify fire_days into regfire and synfire days
    regfire_days = [date for date in fire_days if set({reg}) == set(fire_obs[fire_obs["start_date"] == date]['region'])]
    synfire_days = [date for date in fire_days if date not in regfire_days]

    #format
    nofire_days = [date.strftime("%Y-%m-%d") for date in nofire_days]
    regfire_days = [date.strftime("%Y-%m-%d") for date in regfire_days]
    synfire_days = [date.strftime("%Y-%m-%d") for date in synfire_days]

    #-----------------------------------------------------
    # load region mask
    region_mask = xr.open_dataarray("/net/rain/hyclimm/data/projects/SynFire/Study_Area/land_mask_ERA5_EPSG4326_study_area_32E.nc")
    region_mask_key = dict(zip(["BI", "IP", "FR", "ME", "AL", "SEA", "NEA", "SC", "WMD", "EMD"], [i for i in range(10)]))
    
    #-----------------------------------------------------
    #load standardized CERRA variables
    var = xr.open_dataset(f"/net/rain/hyclimm/data/projects/SynFire/WP1/standardize_monthly_era5_2001_2020/monthly_standardized_era5_original_grid_{varname}_2001-01-01_2020-12-31.nc")

    #clip to region
    region_mask = region_mask.interp(lon = var.lon, lat = var.lat, method = "nearest") 
    var_reg = var.where(region_mask == region_mask_key[reg], drop = True)


    #stratify
    var_reg["time"] = pd.to_datetime(pd.to_datetime(var.time.values).strftime("%Y-%m-%d")) #format the time dimension (keep date only, leave out the hour information)
    var_nofire = var_reg.sel(time = nofire_days)
    var_regfire = var_reg.sel(time = regfire_days)
    var_synfire = var_reg.sel(time = synfire_days)
    

    #average
    var_nofire_mean = var_nofire.mean(dim = ['time'], skipna = True, keep_attrs = True)
    var_regfire_mean = var_regfire.mean(dim = ['time'], skipna = True, keep_attrs = True)
    var_synfire_mean = var_synfire.mean(dim = ['time'], skipna = True, keep_attrs = True)
    

    #save
    var_nofire_mean.to_netcdf(f'/net/rain/hyclimm/data/projects/SynFire/WP1/No_vs_Reg_vs_Syn_fires_ERA5/{reg}_{varname}_mean_anomaly_nofire.nc')
    var_regfire_mean.to_netcdf(f'/net/rain/hyclimm/data/projects/SynFire/WP1/No_vs_Reg_vs_Syn_fires_ERA5/{reg}_{varname}_mean_anomaly_regfire.nc')
    var_synfire_mean.to_netcdf(f'/net/rain/hyclimm/data/projects/SynFire/WP1/No_vs_Reg_vs_Syn_fires_ERA5/{reg}_{varname}_mean_anomaly_synfire.nc')


#-----------------------------------------------------
with mp.Pool(20) as pool:
    pool.starmap(nofire_regfire_synfire_standardized_anomaly, 
                 [(reg, varname) for reg in ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"] for varname in ['tasmax', 'hurs', 'sfcWind', 'pr', 'fwi']])

