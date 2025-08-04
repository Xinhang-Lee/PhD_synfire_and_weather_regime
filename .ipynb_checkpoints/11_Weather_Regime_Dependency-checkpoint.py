'''
calculate the relative conditional probability of synchronous wildfire events with different spatial scales co-occurring with weather regimes.
'''

import pandas as pd
import numpy as np
import geopandas as gpd
from itertools import combinations


# Load Fire Observations

fire_obs = gpd.read_file('/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations_w_region.shp')
fire_obs["start_date"] = pd.to_datetime(fire_obs["start_date"])


# Make SynFire 7d dataframe

synfire_7d = pd.DataFrame(columns = ["date", "regsyn", "consyn", "WR", "BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD", "id_list", "WR_list", "type"])
synfire_7d["date"] = pd.date_range(start = "2001-01-01", end = "2020-12-25")



for ind, row in synfire_7d.iterrows():
    
    t = row["date"]

    # WR
    wr_7d = list(wr[(wr["time"] >= t) & (wr["time"] <= t + pd.Timedelta(days=6))]["wrname"])
    synfire_7d.at[ind, "WR_list"] = wr_7d

    #identify the most frequent WR, if not unique, assign all 
    wr_dict = dict(pd.Series(wr_7d).value_counts())
    max_freq = max(wr_dict.values())
    max_wr = [wr for wr, freq in wr_dict.items() if freq == max_freq]
    synfire_7d.at[ind, "WR"] = max_wr

    # filter fire obs
    fire_obs_7d = fire_obs[(fire_obs["start_date"] >= t) & (fire_obs["start_date"] <= t + pd.Timedelta(days = 6))]

    
    #-------------------------------------------
    if len(fire_obs_7d) <= 1: #no synchronicity
        
        synfire_7d.loc[ind, "regsyn"] = 0
        synfire_7d.loc[ind, "consyn"] = 0

        if len(fire_obs_7d) == 0:  #no fire
            synfire_7d.loc[ind, "type"] = "no"
            
        else: # one fire in any region
            
            synfire_7d.loc[ind, "type"] = "reg"
            
            #record id_list
            synfire_7d.at[ind, "id_list"] = list(fire_obs_7d["ptch_id"])
            reg_unique = fire_obs_7d["region"].item()
            synfire_7d.loc[ind, ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"]] = [1 if reg == reg_unique else 0 for reg in ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"]]
            
    #-------------------------------------------  
    else: #synchronicity
        
        #record id_list
        synfire_7d.at[ind, "id_list"] = list(fire_obs_7d["ptch_id"])
        regs_list = list(fire_obs_7d["region"])
        regs_set = list(set(fire_obs_7d["region"])) #unique list of affected regions

        if len(regs_set) == 1: #regional synchronicity
            synfire_7d.loc[ind, "regsyn"] = 1
            synfire_7d.loc[ind, "consyn"] = 0

            #record the number of fires for the region
            synfire_7d.loc[ind, ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"]] = [regs_list.count(reg) if reg in regs_set else 0 for reg in ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"]]
            synfire_7d.loc[ind, "type"] = "reg"
            
        else: # continental synchronicity
            synfire_7d.loc[ind, "regsyn"] = 0
            synfire_7d.loc[ind, "consyn"] = len(regs_set)

            #record the number of fires for each region
            synfire_7d.loc[ind, ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"]] = [regs_list.count(reg) if reg in regs_set else 0 for reg in ["BI", "SC", "ME", "NEA", "FR", "AL", "SEA", "IP", "WMD", "EMD"]]

            #assign event type
            synfire_7d.loc[ind, "type"] = "low" if len(regs_set) in [2, 3] else "medium" if len(regs_set) in [4, 5] else "high"



synfire_7d_nona = synfire_7d.dropna()
synfire_7d_nona = synfire_7d_nona.drop_duplicates(subset = ["id_list"], keep = "first")
synfire_7d_na = synfire_7d[synfire_7d.isna().any(axis=1)]
synfire_7d_uni_id_list = pd.concat([synfire_7d_nona, synfire_7d_na], ignore_index = True)
synfire_7d_uni_id_list = synfire_7d_uni_id_list.sort_values(by = "date")
synfire_7d_uni_id_list = synfire_7d_uni_id_list.reset_index(drop = True)




#save
synfire_7d_uni_id_list.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Weather_Regime_Dependency/SynFire_7d_unique_id_list_five_levels.csv", index = False)


# Calculate Dependency
def dependency_calculator(season):

    #----------------------------
    #month index
    mon_ind = [3, 4, 5] if season == "MAM" else [6, 7, 8] if season == "JJA" else [9, 10, 11]
    
    #load wr daily classification
    wr = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Weather_Regime_Dependency/wr_daily_classification_{season}_2001-2020.csv")
    
    #calculate wr frequency
    p_wr = dict(zip(["AT", "ZO", "ScTr", "AR", "EuBL", "ScBL", "GL", "no"], [len(wr[wr["wrname"] == wrt])/len(wr) for wrt in ["AT", "ZO", "ScTr", "AR", "EuBL", "ScBL", "GL", "no"]]))
    
    #load synfire dataset with unique fire patch id list
    synfire_7d = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Weather_Regime_Dependency/SynFire_7d_unique_id_list_five_levels.csv")
    
    #subset synfire_7d to each season
    synfire_7d["date"] = pd.to_datetime(synfire_7d["date"])
    synfire_7d = synfire_7d[synfire_7d["date"].dt.month.isin(mon_ind)]
    
    #total number of day
    dates = pd.date_range(start = "2001-01-01", end = "2020-12-31")
    n = len([date for date in dates if date.month in mon_ind]) 
    
    #----------------------------
    #probability of events
    
    #no (no fire within the entire continent)
    p_no = len(synfire_7d[synfire_7d["type"] == "no"])/n
    
    #reg (fire(s) within a single region)
    p_reg = len(synfire_7d[synfire_7d["type"] == "reg"])/n
    
    #lowconsyn (2-3 regions affected)
    p_lowconsyn = len(synfire_7d[synfire_7d["type"] == "low"])/n
    
    #mediumconsyn (4-5 regions affected)
    p_mediumconsyn = len(synfire_7d[synfire_7d["type"] == "medium"])/n 
    
    #highconsyn (more than 5 regions affected)
    p_highconsyn = len(synfire_7d[synfire_7d["type"] == "high"])/n 
    
    #----------------------------
    #initialize dependency table
    dp = pd.DataFrame(columns = ["AT", "ZO", "ScTr", "AR", "EuBL", "ScBL", "GL", "no"], index = ["no", "reg", "lowconsyn", "mediumconsyn", "highconsyn"])
    
    #calculate dependency
    for wrt in ["AT", "ZO", "ScTr", "AR", "EuBL", "ScBL", "GL", "no"]:
        
        p_no_wr = len(synfire_7d[(synfire_7d["type"] == "no") & (synfire_7d["WR"].apply(lambda x: wrt in x))])/n
        
        p_reg_wr = len(synfire_7d[(synfire_7d["type"] == "reg")  & (synfire_7d["WR"].apply(lambda x: wrt in x))])/n
        
        p_lowconsyn_wr = len(synfire_7d[(synfire_7d["type"] == "low") & (synfire_7d["WR"].apply(lambda x: wrt in x))])/n
        
        p_mediumconsyn_wr = len(synfire_7d[(synfire_7d["type"] == "medium") & (synfire_7d["WR"].apply(lambda x: wrt in x))])/n
        
        p_highconsyn_wr = len(synfire_7d[(synfire_7d["type"] == "high") & (synfire_7d["WR"].apply(lambda x: wrt in x))])/n

        #-----------------------------------
        dp_no_wr = (p_no_wr/p_wr[wrt])/p_no
        
        dp_reg_wr = (p_reg_wr/p_wr[wrt])/p_reg
        
        dp_lowconsyn_wr = (p_lowconsyn_wr/p_wr[wrt])/p_lowconsyn
        
        dp_mediumconsyn_wr = (p_mediumconsyn_wr/p_wr[wrt])/p_mediumconsyn
        
        dp_highconsyn_wr = (p_highconsyn_wr/p_wr[wrt])/p_highconsyn

        #-----------------------------------
        dp[wrt] = [dp_no_wr, dp_reg_wr, dp_lowconsyn_wr, dp_mediumconsyn_wr, dp_highconsyn_wr]
    
    #----------------------------
    #record sample size
    dp["n"] = [len(synfire_7d[synfire_7d["type"] == "no"]), #no
               len(synfire_7d[synfire_7d["type"] == "reg"]), #regional fire(s)
               len(synfire_7d[synfire_7d["type"] == "low"]), #low
               len(synfire_7d[synfire_7d["type"] == "medium"]), #medium
               len(synfire_7d[synfire_7d["type"] == "high"]) #high
              ]

    
    dp.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Weather_Regime_Dependency/Weather_Regime_Dependency_five_levels_{season}_incl_noregime.csv", index = True, index_label = "synlevel")


dependency_calculator("MAM")
dependency_calculator("JJA")
dependency_calculator("SON")
