'''
Calculate mean anomaly fields over time for each weather regime, including no regime.

'''

import pandas as pd
import xarray as xr
import multiprocessing as mp


def era5_var_anomaly_by_wr_season(season, varname):


    '''
    parameters
    season: one of "MAM", "JJA", "SON", "DJF"
    
    varname:
    total precipitation          "pr"      daily sum
    surface wind speed           "sfcWind" daily mean
    relative humidity            "hurs"    daily mean
    Canadian fire weather index  "fwi"     daily 
    maximum air temperature      "tasmax"  daily maximum
    '''
    
    #------------------------------
    # load data
    #load weather regimes
    wr = pd.read_csv('/net/rain/hyclimm/data/projects/SynFire/WP1/Weather_Regime_Dependency/wr_daily_classification_2001-2020.csv')

    #load standardized ERA5 variables
    var = xr.open_dataset(f"/net/rain/hyclimm/data/projects/SynFire/WP1/standardize_monthly_era5_2001_2020/monthly_standardized_era5_original_grid_{varname}_2001-01-01_2020-12-31.nc")

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
        var_wr_season_dates_mean.to_netcdf(f"/net/rain/hyclimm/data/projects/SynFire/WP1/ERA5_Variable_Anomaly_by_Weather_Regime/Anomaly_{varname}_{season}_{wrname}.nc")


#------------------------------
with mp.Pool(10) as pool:
    pool.starmap(era5_var_anomaly_by_wr_season, [(season, varname) for season in ["MAM", "JJA", "SON", "DJF"] for varname in ["tasmax", "hurs", "pr", "sfcWind", "fwi"]])
