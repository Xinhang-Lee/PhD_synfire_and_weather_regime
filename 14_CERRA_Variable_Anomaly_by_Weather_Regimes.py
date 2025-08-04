'''
Calculate mean anomaly fields over time for each weather regime, including no regime.

'''

import pandas as pd
import xarray as xr
import multiprocessing as mp


def cerra_var_anomaly_by_wr_season(season, varname):


    '''
    parameters
    season: one of "MAM", "JJA", "SON", "DJF"
    
    varname:
    air temperature              "2t"     daily mean
    total precipitation          "tp"     daily sum
    10 m wind speed              "10si"   daily mean
    relative humidity            "2r"     daily mean
    Canadian fire weather index  "fwi"    daily 
    maximum air temperature      "mx2t"   daily
    '''
    
    #------------------------------
    # load data
    #load weather regimes
    wr = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/WR/wr_daily_2001-2020.csv")

    #load standardized CERRA variables
    var = xr.open_dataset(f"/net/rain/hyclimm/data/projects/SynFire/WP1/standardize_monthly_cerra_2001_2020/monthly_standardized_cerra_original_grid_{varname}_2001-01-01_2020-12-31.nc")

    #------------------------------
    #format time
    var["time"] = pd.to_datetime(pd.to_datetime(var.time.values).strftime("%Y-%m-%d"))

    #get timesteps
    var_season = var.sel(time = var['time'].dt.season == season)

    timestep = pd.to_datetime(pd.to_datetime(var_season.time.values).strftime("%Y-%m-%d"))

    #------------------------------
    for wrname in ["AR", "AT", "EuBL", "GL", "ScBL", "ScTr", "ZO", "no"]:
        
        # get the dates for the weather regime
        wrdates  = list(wr[wr["wrname"] == wrname]["time"])
    
        # get common dates
        wr_season_dates = [date for date in wrdates if date in timestep]
    
        # slice var nc
        var_wr_season_dates = var.sel(time = wr_season_dates)
    
        # calculate mean field of anomaly
        var_wr_season_dates_mean = var_wr_season_dates.mean(dim = "time", skipna = True, keep_attrs = True)

        # save
        var_wr_season_dates_mean.to_netcdf(f"/net/rain/hyclimm/data/projects/SynFire/WP1/CERRA_Variable_Anomaly_by_Weather_Regime/Anomaly_{varname}_{season}_{wrname}.nc")


#------------------------------
with mp.Pool(6) as pool:
    pool.starmap(cerra_var_anomaly_by_wr_season, [(season, varname) for season in ["MAM", "JJA", "SON", "DJF"] for varname in ["2t", "tp", "10si", "2r", "fwi", "mx2t"]])
