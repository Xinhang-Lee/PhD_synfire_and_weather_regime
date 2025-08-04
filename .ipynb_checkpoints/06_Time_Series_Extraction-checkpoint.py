'''
Extract regional wildfire time series.
'''

import pandas as pd
import geopandas as gpd
from datetime import datetime



# Load fire obs
fire_obs = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations_w_region.shp")
fire_obs["start_date"] = pd.to_datetime(fire_obs["start_date"])


# Time Series (All Seasons)
timestep = pd.date_range(start = "2001-01-01", end = "2020-12-31")
region_list = ["BI", "IP", "FR", "ME", "AL", "SEA", "NEA", "SC", "WMD", "EMD"]

fire_time_series_all = pd.DataFrame({"Time": timestep})

for reg in region_list:
    
    # regional fire time series
    fire_time_series_reg = pd.DataFrame(fire_obs.loc[fire_obs["region"] == reg, "start_date"].value_counts()).reset_index()
    fire_time_series_reg["start_date"] = pd.to_datetime(fire_time_series_reg["start_date"])
    fire_time_series_reg = fire_time_series_reg.rename(columns = {"start_date": "Time", "count": reg})

    # merge
    fire_time_series_all = pd.merge(fire_time_series_all, fire_time_series_reg, how = "left", on = "Time")

fire_time_series_all = fire_time_series_all.fillna(0)   #NAN means no fire happening, fill with 0

# convert to int
fire_time_series_all[fire_time_series_all.select_dtypes(include = ['number']).columns] = fire_time_series_all.select_dtypes(include = ['number']).astype(int)

# check total fire number (correct)
fire_time_series_all[fire_time_series_all.select_dtypes(include = ['number']).columns].sum()



fire_time_series_all.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_all_seasons.csv", index = False)


# Time Series (MAM, JJA, SON, DJF)

#MAM
fire_time_series_MAM = fire_time_series_all[fire_time_series_all["Time"].dt.month.isin([3,4,5])]
fire_time_series_MAM.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_MAM.csv", index = False)

#JJA
fire_time_series_JJA = fire_time_series_all[fire_time_series_all["Time"].dt.month.isin([6,7,8])]
fire_time_series_JJA.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_JJA.csv", index = False)

#SON
fire_time_series_SON = fire_time_series_all[fire_time_series_all["Time"].dt.month.isin([9,10,11])]
fire_time_series_SON.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_SON.csv", index = False)

#DJF
fire_time_series_DJF = fire_time_series_all[fire_time_series_all["Time"].dt.month.isin([12,1,2])]
fire_time_series_DJF.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_DJF.csv", index = False)

