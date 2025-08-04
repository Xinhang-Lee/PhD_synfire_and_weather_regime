'''
Analyze the dependency between pair-wise synchronicity and weather regimes.
'''

import pandas as pd
import geopandas as gpd
import numpy as np
from itertools import combinations
from random import sample
import pickle
import multiprocessing as mp
import networkx as nx


#load fire observations
fire_obs = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations_w_region.shp")
fire_obs["start_date"] = pd.to_datetime(fire_obs["start_date"])


#load weather regime
wr = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/WR/wr_daily_2001-2020.csv")
wr["time"] = pd.to_datetime(wr["time"])


#Identify synchronized event pairs (all seasons, MAM, JJA, SON, DJF)

def syn_event_pair(season):

    #load time series
    time_series = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_{season}.csv")
    
    tau_max = 6
    reg_list = list(time_series.columns[1:])
    
    #Identify event date and stack for all regions
    event_date_list = []
    
    for reg in reg_list:
        reg_event_date = pd.DataFrame(columns = ["Region", "Date"])
        reg_event_date["Date"] = time_series[time_series[reg] > 0]["Time"]
        reg_event_date["Region"] = reg 
        event_date_list.append(reg_event_date)
    
    #concat and convert to time object
    event_date = pd.concat(event_date_list, ignore_index = True)
    event_date["Date"] = pd.to_datetime(event_date["Date"])
    
    #calculate time difference between two fire events
    def time_diff(date_list, ind1, ind2):
        '''
        Function
        calculate the absolute time difference (in days) between two fire event indices, return np.nan if indexing out of the boundary for either of the fire event.
        '''
        try:
            
            date1 = date_list[ind1]
            date2 = date_list[ind2]
            return abs((date1 - date2).days)
            
        except (IndexError, KeyError): 
            return np.nan
    
    rows = []
    
    for (reg1, reg2) in [(combo[0], combo[1]) for combo in combinations(reg_list, 2)]:
        
        #filter event date for two regions
        event_date_reg1 = event_date[event_date["Region"] == reg1]
        event_date_reg2 = event_date[event_date["Region"] == reg2]
        
        for index1, row1 in event_date_reg1.iterrows():
        
            for index2, row2 in event_date_reg2.iterrows():
                del_t = (row2["Date"] - row1["Date"]).days                 
                tau = np.nanmin([time_diff(event_date_reg1["Date"], index1+1, index1), 
                                 time_diff(event_date_reg1["Date"], index1, index1-1), 
                                 time_diff(event_date_reg2["Date"], index2+1, index2), 
                                 time_diff(event_date_reg2["Date"], index2, index2-1)])
                
                tau = min(tau/2, tau_max)
            
                if abs(del_t) <= tau:   #synchronized
                    
                    ptch_id_reg1 = list(fire_obs.loc[(fire_obs["start_date"] == row1["Date"]) & (fire_obs["region"] == reg1)]["ptch_id"])
                    ptch_id_reg2 = list(fire_obs.loc[(fire_obs["start_date"] == row2["Date"]) & (fire_obs["region"] == reg2)]["ptch_id"])
                    new_row = {"reg1": reg1, "id1": ptch_id_reg1, "date1": row1["Date"], "num1":len(ptch_id_reg1), "reg2":reg2, "id2":ptch_id_reg2, "date2":row2["Date"], "num2":len(ptch_id_reg2)}
                    rows.append(new_row)

    #---------------------------
    synpair = pd.DataFrame(rows)

    # Assign weather regime: for synchronized event pairs, we assign a unique weather regime to this event pair based on the date of the earlier event.
    synpair["time"] = np.where(synpair["date1"] <= synpair["date2"], synpair["date1"], synpair["date2"])
    synpair["time"] = pd.to_datetime(synpair["time"])
    synpair = synpair.merge(pd.DataFrame(wr[["time", "wrname"]]), how = "left")

    #save
    synpair.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Synchronized_event_pair_between_region_pair_{season}.csv", index = False)


pool = mp.Pool(5)
pool.map(syn_event_pair, ["all_seasons", "MAM", "JJA", "SON", "DJF"])
pool.close()



# calculate weather regime dependency including no regime
def event_pair_wr_dependency(season):

    #weather regimes
    wr = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/WR/wr_daily_2001-2020.csv")
    wr["time"] = pd.to_datetime(wr["time"])

    #calculate weather regime frequency based on season
    if season == "MAM":
        wr = wr[wr["time"].dt.month.isin([3,4,5])]
    elif season == "JJA":
        wr = wr[wr["time"].dt.month.isin([6,7,8])]
    elif season == "SON":
        wr = wr[wr["time"].dt.month.isin([9,10,11])]
    elif season == "DJF":
        wr = wr[wr["time"].dt.month.isin([12,1,2])]
    
    wr_freq = dict(zip(["EuBL", "GL", "ScBL", "ScTr", "AR", "AT", "ZO", "no"], [len(wr[wr["wrname"] == wrname])/len(wr) for wrname in ["EuBL", "GL", "ScBL", "ScTr", "AR", "AT", "ZO", "no"]]))

    #----------------------------------------------
    rows = []
    n = len(wr)  #total number of days
    synpair = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Synchronized_event_pair_between_region_pair_{season}.csv")

    for (reg1, reg2) in [(combo[0], combo[1]) for combo in combinations(['BI', 'IP', 'FR', 'ME', 'AL', 'SEA', 'NEA', 'SC', 'WMD', 'EMD'], 2)]:

        #filter synchronized event pairs
        synpair_reg = synpair[(synpair["reg1"] == reg1) & (synpair["reg2"] == reg2)]
    
        #calculate dependency
        row = {"reg1":reg1, "reg2":reg2, 
               "EuBL": ((len(synpair_reg[synpair_reg["wrname"] == "EuBL"])/n)/wr_freq['EuBL'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA, 
               "GL": ((len(synpair_reg[synpair_reg["wrname"] == "GL"])/n)/wr_freq['GL'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA, 
               "ScBL": ((len(synpair_reg[synpair_reg["wrname"] == "ScBL"])/n)/wr_freq['ScBL'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA, 
               "ScTr": ((len(synpair_reg[synpair_reg["wrname"] == "ScTr"])/n)/wr_freq['ScTr'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA, 
               "AR": ((len(synpair_reg[synpair_reg["wrname"] == "AR"])/n)/wr_freq['AR'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA, 
               "AT": ((len(synpair_reg[synpair_reg["wrname"] == "AT"])/n)/wr_freq['AT'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA, 
               "ZO": ((len(synpair_reg[synpair_reg["wrname"] == "ZO"])/n)/wr_freq['ZO'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA,
               "no":((len(synpair_reg[synpair_reg["wrname"] == "no"])/n)/wr_freq['no'])/(len(synpair_reg)/n) if len(synpair_reg) !=0 else pd.NA,
               "n": len(synpair_reg)}
        rows.append(row)

    
    dependency_synpair = pd.DataFrame(rows)
    
    #save
    dependency_synpair.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Dependency_wr_for_region_pair_{season}_incl_noregime.csv", index = False)


for season in ["all_seasons", "MAM", "JJA", "SON", "DJF"]:
    event_pair_wr_dependency(season)


# Statistical Test

## Create weather regime permutation ensemble (n = 2000)

#load seeds
with open("/net/rain/hyclimm/data/projects/SynFire/WP1/Create_Permutation_Ensemble/Permutation_Seeds.pkl", 'rb') as file:
    seeds = pickle.load(file)


wr_ens = pd.DataFrame(columns = ["time"] + [f"ens_{i}" for i in range(2000)])
wr_ens["time"] = pd.date_range(start = "2001-01-01", end = "2020-12-31")
time_wrname = pd.DataFrame(wr[["time", "wrname"]])

for i in range(2000):
    
    seed  = seeds[i]
    wrname_permute = pd.concat([time_wrname[time_wrname["time"].dt.year == s] for s in seed], ignore_index = True)
    wr_ens[f"ens_{i}"] = wrname_permute["wrname"]


# check NA
wr_ens.isna().any().any()


#save
wr_ens.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Block_permutation_ens2000_weather_regime.csv", index = False)


# Calculate ensemble dependency
def event_pair_wr_dependency_ens(season):

    #load weather regime ensemble
    wr_ens = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Block_permutation_ens2000_weather_regime.csv")
    wr_ens["time"] = pd.to_datetime(wr_ens["time"])

    #subset for four seasons
    if season == "MAM":
        wr_ens = wr_ens[wr_ens["time"].dt.month.isin([3,4,5])]
    elif season == "JJA":
        wr_ens = wr_ens[wr_ens["time"].dt.month.isin([6,7,8])]
    elif season == "SON":
        wr_ens = wr_ens[wr_ens["time"].dt.month.isin([9,10,11])]
    elif season == "DJF":
        wr_ens = wr_ens[wr_ens["time"].dt.month.isin([12,1,2])]

    #total number of days
    n = len(wr_ens)

    #load synchronous event pair
    synpair = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Synchronized_event_pair_between_region_pair_{season}.csv")
    synpair["time"] = pd.to_datetime(synpair["time"])
    synpair = synpair.drop(columns = ["wrname"])

    rows = []

    for (reg1, reg2) in [(combo[0], combo[1]) for combo in combinations(['BI', 'IP', 'FR', 'ME', 'AL', 'SEA', 'NEA', 'SC', 'WMD', 'EMD'], 2)]:
    
        synpair_reg = synpair[(synpair["reg1"] == reg1) & (synpair["reg2"] == reg2)]
    
        for i in range(2000):
            
            wr_rand = pd.DataFrame(wr_ens[["time", f'ens_{i}']])
            wr_rand.rename(columns = {f'ens_{i}': "wrname"}, inplace = True)

            #weather regime frequency
            wr_freq = dict(zip(["EuBL", "GL", "ScBL", "ScTr", "AR", "AT", "ZO", "no"], [len(wr_rand[wr_rand["wrname"] == wrname])/len(wr_rand) for wrname in ["EuBL", "GL", "ScBL", "ScTr", "AR", "AT", "ZO", "no"]]))
            
            synpair_reg_wr = synpair_reg.merge(pd.DataFrame(wr_rand[["time", "wrname"]]), how = "left")
    
            #calculate dependency
            row = {"reg1":reg1, "reg2":reg2, 
                   "EuBL": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "EuBL"])/n)/wr_freq['EuBL'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA, 
                   "GL": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "GL"])/n)/wr_freq['GL'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA, 
                   "ScBL": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "ScBL"])/n)/wr_freq['ScBL'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA, 
                   "ScTr": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "ScTr"])/n)/wr_freq['ScTr'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA, 
                   "AR": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "AR"])/n)/wr_freq['AR'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA, 
                   "AT": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "AT"])/n)/wr_freq['AT'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA, 
                   "ZO": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "ZO"])/n)/wr_freq['ZO'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA,
                   "no": ((len(synpair_reg_wr[synpair_reg_wr["wrname"] == "no"])/n)/wr_freq['no'])/(len(synpair_reg_wr)/n) if len(synpair_reg_wr) != 0 else pd.NA,
                   "n": len(synpair_reg_wr),
                   "label": i}
            
            rows.append(row)
            
    dependency_ens = pd.DataFrame(rows)
    
    dependency_ens.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Dependency_ens2000_wr_for_region_pair_{season}_incl_noregime.csv", index = False)


pool = mp.Pool(5)
pool.map(event_pair_wr_dependency_ens, ["all_seasons", "MAM", "JJA", "SON", "DJF"])
pool.close()


# check significant dependency at p < 0.05 level

for season in ["MAM", "JJA", "SON"]:
    
    print(season)
    
    dependency = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Dependency_wr_for_region_pair_{season}_incl_noregime.csv")
    dependency_ens = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Analyze_synchronized_event_pair/Dependency_ens2000_wr_for_region_pair_{season}_incl_noregime.csv")

    for (reg1, reg2) in [(combo[0], combo[1]) for combo in combinations(['BI', 'IP', 'FR', 'ME', 'AL', 'SEA', 'NEA', 'SC', 'WMD', 'EMD'], 2)]:

        n = dependency.loc[(dependency["reg1"] == reg1) & (dependency["reg2"] == reg2), "n"].item()

        if n <= 20:
            continue
        else:
            
            dependency_ens_reg = dependency_ens[(dependency_ens["reg1"] == reg1) & (dependency_ens["reg2"] == reg2)]
                
            for wrname in ["EuBL", "GL", "ScBL", "ScTr", "AR", "AT", "ZO", "no"]:
                
                dp = dependency.loc[(dependency["reg1"] == reg1) & (dependency["reg2"] == reg2), wrname].item()
                sig_thd = dependency_ens_reg[wrname].quantile(0.95)
                
                if dp > sig_thd:
                    print(reg1, reg2, wrname, dp, sig_thd, n)

