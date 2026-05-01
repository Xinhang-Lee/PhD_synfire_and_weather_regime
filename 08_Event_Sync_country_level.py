import pandas as pd
import multiprocessing as mp
from itertools import permutations, combinations
import numpy as np



#----------------------------------------------------------------------------
# calculate ES at country level
#----------------------------------------------------------------------------


def SynEvent_ctr(season, tau_max):
    
    '''
    "reg" means country in this function.
    Parameters
    season: one of ["all_seasons", "MAM", "JJA", "SON", "DJF"]
    tau_max: maximal time lag in event synchronicity definition according to Boers et al., 2019.
    --------------------------------------------
    Returns
    Event Synchronicity (ES) matrix, lead-lag events counted as 1 for the lead region, fully synchronised event counted as 0.5 for both regions.
    ---------------------------------------------
    Functionality
    Identify event synchronicity based on Boers et al., 2019 at country level.
    '''
    
    time_series = pd.read_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Time_Series_Extraction/fire_time_series_country-level_{season}.csv")
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

    #identify countries with fires
    ctr_w_fires = event_date['Region'].value_counts().index.tolist()
    
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

        if (reg1 not in ctr_w_fires) or (reg2 not in ctr_w_fires):  #Either side without fires leads to 0 synchronicity
            ES_matrix.loc[reg1, reg2] = 0
            ES_matrix.loc[reg2, reg1] = 0
            continue
        
        #filter event date for two regions
        event_date_reg1 = event_date[event_date["Region"] == reg1]
        event_date_reg2 = event_date[event_date["Region"] == reg2]

        #initialize undirected and directed Event Eynchronicity Values
        ES_1_2 = 0   #reg1 leads reg2
        ES_2_1 = 0   #reg2 leads reg1
        
        for index1, row1 in event_date_reg1.iterrows():

            for index2, row2 in event_date_reg2.iterrows():
                del_t = (row2["Date"] - row1["Date"]).days
                
                '''
                tau = np.nanmin([time_diff(event_date_reg1["Date"], index1+1, index1), 
                                 time_diff(event_date_reg1["Date"], index1, index1-1), 
                                 time_diff(event_date_reg2["Date"], index2+1, index2), 
                                 time_diff(event_date_reg2["Date"], index2, index2-1)]) #will get run-time warnings if both series only have one event --> not the case for PRUDENCE region analysis
                tau = min(tau/2, tau_max)
                '''
                
                arr = [time_diff(event_date_reg1["Date"], index1+1, index1), 
                       time_diff(event_date_reg1["Date"], index1, index1-1), 
                       time_diff(event_date_reg2["Date"], index2+1, index2), 
                       time_diff(event_date_reg2["Date"], index2, index2-1)]
                if np.all(np.isnan(arr)):
                    tau = np.nan
                else:
                    tau = np.nanmin(arr)

                tau = np.nanmin([tau/2, tau_max])

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
    ES_matrix.to_csv(f"/net/rain/hyclimm/data/projects/SynFire/WP1/Event_Sync_country_level/ES_matrix_country-level_taumax_{tau_max}_{season}.csv", index = True)



#------------------------------------------------
pool = mp.Pool(5)
pool.starmap(SynEvent_ctr, [(s, 6) for s in ["all_seasons", "MAM", "JJA", "SON", "DJF"]])
pool.close()



