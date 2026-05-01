import pandas as pd
import multiprocessing as mp
from itertools import permutations, combinations
import numpy as np


# Please adapt before run !!!
#----------------------------------------------------------------------------
#enter the season indicator: could be "all_seasons", "MAM", "JJA", "SON", "DJF"
#season = "all_seasons"
#season = "MAM"
#season = "JJA"
#season = "SON"
season = "DJF"

#enter the fire indicator: could be "All_Fires", "Large_Fires"
fire = "All_Fires"
#----------------------------------------------------------------------------



#----------------------------------------------------------------------------
# 1. calculate ES for the permutation ensemble
#----------------------------------------------------------------------------


# load time series
time_series_rand_ens = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Create_Permutation_Ensemble/fire_time_series_Bpermutation_2000_{season}.csv")

def SynEvent_Rand(time_series_rand, tau_max):
    
    '''
    Parameters
    time_series_rand: randomized dataframe for time series in the format ["Time", "reg1", "reg2", ..., "regX", "label"], record the initial date of fire in each region.
    tau_max: maximal time lag in event synchronicity definition according to Boers et al., 2019.
    --------------------------------------------
    Returns
    ES_df
    Event Synchronicity (ES) dataframe, lead-lag events counted as 1 for the lead region, fully synchronised events counted as 0.5 for both regions.
    ---------------------------------------------
    Function
    calculate event synchronicity (ES) for region pairs based on Boers et al., 2019.
    '''
    
    #get region names
    reg_list = list(time_series_rand.columns[1:-1])
    
    
    #Identify event date and stack for all regions
    event_date_list = []
    
    for reg in reg_list:
        reg_event_date = pd.DataFrame(columns = ["Region", "Date"])
        reg_event_date["Date"] = time_series_rand[time_series_rand[reg] > 0]["Time"]
        reg_event_date["Region"] = reg 
        event_date_list.append(reg_event_date)
    
    #concat and convert to time object
    event_date = pd.concat(event_date_list, ignore_index = True)
    event_date["Date"] = pd.to_datetime(event_date["Date"])

    #get the label
    label = np.unique(time_series_rand["label"]).item()
    print(label)
    
    #initialize event synchronicity matrix
    ES_df = pd.DataFrame(columns = ["_".join(combo) for combo in permutations(reg_list, 2)],
                         index = [label])  #return each possible 2-element permutations (90 in total)

    #------------------------
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
            
        except (IndexError, KeyError): #IndexError---handle out-of-bound errors, KeyError--- handle index = -1
            return np.nan
    #-----------------------

    for (reg1, reg2) in [(combo[0], combo[1]) for combo in combinations(reg_list, 2)]:
        
        
        #filter event date for two regions
        event_date_reg1 = event_date[event_date["Region"] == reg1]
        event_date_reg2 = event_date[event_date["Region"] == reg2]

        ES_1_2 = 0   #reg1 leads reg2
        ES_2_1 = 0   #reg2 leads reg1
        
        for index1, row1 in event_date_reg1.iterrows():

            for index2, row2 in event_date_reg2.iterrows():
                
                del_t = (row2["Date"] - row1["Date"]).days  # actual time difference   
                
                tau = np.nanmin([time_diff(event_date_reg1["Date"], index1+1, index1), 
                                 time_diff(event_date_reg1["Date"], index1, index1-1), 
                                 time_diff(event_date_reg2["Date"], index2+1, index2), 
                                 time_diff(event_date_reg2["Date"], index2, index2-1)])
                
                tau = min(tau/2, tau_max)

                if abs(del_t) <= tau:
                    if del_t < 0: #reg2 leads reg1
                        ES_2_1 += 1
                    elif del_t > 0: #reg1 leads reg2
                        ES_1_2 += 1
                    else: # fully synchronized
                        ES_2_1 += 0.5
                        ES_1_2 += 0.5

        #fill in the matrix
        ES_df.loc[label, rf"{reg1}_{reg2}"] = ES_1_2
        ES_df.loc[label, rf"{reg2}_{reg1}"] = ES_2_1

    #return
    return ES_df

#parallelization
time_series_rand_grouped = time_series_rand_ens.groupby('label')
time_series_rand_list = [group for _, group in time_series_rand_grouped]

# Parallelization
def Parallel_EventSyn_Rand(tau_max):
    pool = mp.Pool(80)
    results = [pool.apply_async(SynEvent_Rand, args=(time_series, tau_max)) for time_series in time_series_rand_list]
    pool.close()
    
    results = [result.get() for result in results]
    results = pd.concat(results, ignore_index = True)
    results.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Event_Sync_Sig_Sns/{fire}/Sigtest/ES_Bpermutation_2000_taumax_{tau_max}_{season}.csv", index = False)

#adapt here
for tau_max in [6, 9, 12, 15, 18, 21]:
    Parallel_EventSyn_Rand(tau_max)

#----------------------------------------------------------------------------
# 2. calculate ES of true fire time series
#----------------------------------------------------------------------------

#read true fire time series
fire_time_series = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_{season}.csv")


def SynEvent(time_series, tau_max):
    
    '''
    Parameters
    time_series: dataframe for time series in the format ["Time", "reg1", "reg2", ..., "regX"], record the initial date of fire in each region.
    tau_max: maximal time lag in event synchronicity definition according to Boers et al., 2019.
    --------------------------------------------
    Returns
    Event Synchronicity (ES) matrix, lead-lag events counted as 1 for the lead region, fully synchronised event counted as 0.5 for both regions.
    ---------------------------------------------
    Functionality
    Identify event synchronicity based on Boers et al., 2019.
    '''
    print(tau_max)
    
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
    
    #initialize event synchronicity matrix
    ES_matrix = pd.DataFrame(index=reg_list, columns=reg_list)

    #------------------------
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
            
        except (IndexError, KeyError): #IndexError---handle out-of-bound errors, KeyError--- handle index = -1
            return np.nan
    #-----------------------

    for (reg1, reg2) in [(combo[0], combo[1]) for combo in combinations(reg_list, 2)]:
        
        
        #filter event date for two regions
        event_date_reg1 = event_date[event_date["Region"] == reg1]
        event_date_reg2 = event_date[event_date["Region"] == reg2]

        #initialize undirected and directed Event Eynchronicity Values
        ES_1_2 = 0   #reg1 leads reg2
        ES_2_1 = 0   #reg2 leads reg1
        
        for index1, row1 in event_date_reg1.iterrows():

            for index2, row2 in event_date_reg2.iterrows():
                del_t = (row2["Date"] - row1["Date"]).days                 
                tau = np.nanmin([time_diff(event_date_reg1["Date"], index1+1, index1), 
                                 time_diff(event_date_reg1["Date"], index1, index1-1), 
                                 time_diff(event_date_reg2["Date"], index2+1, index2), 
                                 time_diff(event_date_reg2["Date"], index2, index2-1)])
                tau = min(tau/2, tau_max)

                if abs(del_t) <= tau:
                    if del_t < 0: #reg2 leads reg1
                        ES_2_1 +=1
                    elif del_t > 0: #reg1 leads reg2
                        ES_1_2 +=1
                    else: # fully synchronized
                        ES_2_1 += 0.5
                        ES_1_2 += 0.5

        #fill in the matrix
        ES_matrix.loc[reg1, reg2] = ES_1_2
        ES_matrix.loc[reg2, reg1] = ES_2_1

    #save
    ES_matrix.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Event_Sync_Sig_Sns/{fire}/Snstest/ES_matrix_taumax_{tau_max}_{season}.csv", index = True)



#------------------------------------------------
taumax_list = [6, 9, 12, 15, 18, 21]
pool = mp.Pool(6)
pool.starmap(SynEvent, [(fire_time_series, tau_max) for tau_max in taumax_list])
pool.close()



