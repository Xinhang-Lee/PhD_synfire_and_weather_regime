'''
Create a block permutation ensemble.
'''

import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import random
import pickle

yr_list = [yr for yr in range(2001, 2021)]


unique_perms = set()   

while len(unique_perms) < 20000:
    
    perm = tuple(random.sample(yr_list, k = len(yr_list)))   
    
    '''
    random.sample(population, k, *, counts=None)
    Return a k length list of unique elements chosen from the population sequence. 
    Used for random sampling without replacement.
    '''
    
    unique_perms.add(perm)

unique_perms = list(unique_perms)


#save seeds
with open("/net/rain/hyclimm/data/projects/SynFire/WP1/Create_Permutation_Ensemble/Permutation_Seeds.pkl", 'wb') as file:
    pickle.dump(unique_perms, file)


# Permutation Function

def time_series_block_permutation(season, n):

    #season: 'all_seasons', 'MAM', 'JJA', 'SON', 'DJF'
    #n: number of permutation (2000)
    
    #load seeds
    with open("/net/rain/hyclimm/data/projects/SynFire/WP1/Create_Permutation_Ensemble/Permutation_Seeds.pkl", 'rb') as file:
        unique_perms = pickle.load(file)

    #load time series
    fire_time_series = pd.read_csv(f'/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_{season}.csv')
    fire_time_series['Time'] = pd.to_datetime(fire_time_series['Time'])

    region_list = ["BI", "IP", "FR", "ME", "AL", "SEA", "NEA", "SC", "WMD", "EMD"]

    fire_time_series_bp_em = []

    ind = 0
    
    for i in range(n):

        # -----------------------------------------
        timestep = pd.date_range(start = '2001-01-01', end = '2020-12-31')
        
        if season == "MAM":
            timestep = pd.DatetimeIndex([date for date in timestep if date.month in [3,4,5]])
        elif season == "JJA":
            timestep = pd.DatetimeIndex([date for date in timestep if date.month in [6,7,8]])
        elif season == "SON":
            timestep = pd.DatetimeIndex([date for date in timestep if date.month in [9,10,11]])
        elif season == "DJF":
            timestep = pd.DatetimeIndex([date for date in timestep if date.month in [12,1,2]])
        # -----------------------------------------  
        
        
        fire_time_series_bp = pd.DataFrame({'Time': timestep})
        
        for reg in region_list:

            fire_time_series_reg = fire_time_series[["Time", reg]].copy()
            fire_time_series_reg['yr'] = fire_time_series_reg['Time'].dt.year
            seeds = unique_perms[ind]
            
        
            fire_time_series_bp_reg = pd.concat([fire_time_series_reg[fire_time_series_reg["yr"] == seed] for seed in seeds], ignore_index = True)
            fire_time_series_bp_reg = fire_time_series_bp_reg.drop(columns = ['yr'])
            
            #reset time
            fire_time_series_bp_reg["Time"] = timestep
            fire_time_series_bp = pd.merge(fire_time_series_bp, fire_time_series_bp_reg, how = "left", on = "Time")
            ind += 1

        # add label for parallelization
        fire_time_series_bp['label'] = i
        fire_time_series_bp_em.append(fire_time_series_bp)
        
    fire_time_series_bp_em = pd.concat(fire_time_series_bp_em, ignore_index = True)


    #save
    fire_time_series_bp_em.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Create_Permutation_Ensemble/fire_time_series_Bpermutation_{n}_{season}.csv", index = False)


time_series_block_permutation(season = "all_seasons", n = 2000)
time_series_block_permutation(season = "MAM", n = 2000)
time_series_block_permutation(season = "JJA", n = 2000)
time_series_block_permutation(season = "SON", n = 2000)
time_series_block_permutation(season = "DJF", n = 2000)

