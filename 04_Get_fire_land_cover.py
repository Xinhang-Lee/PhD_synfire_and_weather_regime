import xarray as xr
import pandas as pd
import geopandas as gpd
import rioxarray
import multiprocessing as mp
import numpy as np

#suppress the warning message related to rio.clip
import warnings
warnings.filterwarnings("ignore", message="invalid value encountered in cast", category=RuntimeWarning)

#load fire obs
fry_study_area = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/FRYv2.0_FireCCI51_6D_2001-2020_study_area_final_incl_border.shp")

def get_land_cover(year):

    print(rf"{year} started!")

    #filter fire observation of the year
    fire_of_year = fry_study_area[fry_study_area["YR"] == year]
    
    #initialize dataframe to store land cover distribution
    land_cover = pd.DataFrame(columns = ["ptch_id", "lc_unique", "lc_count"])
    land_cover["ptch_id"] = fire_of_year["ptch_id"].copy()
    
    #set ptch_id as index
    fire_of_year.set_index("ptch_id", inplace = True)
    land_cover.set_index("ptch_id", inplace = True)

    #get CCI land cover map of the previous year
    lc_year = year -1
    if lc_year < 2016:
        lc_map = xr.open_dataset(f"/net/rain/hyclimm/data/projects/SynFire/WP1/CCI_Land_Cover_Maps_EU/EU_ESACCI-LC-L4-LCCS-Map-300m-P1Y-{lc_year}-v2.0.7cds.nc")
    else:
        lc_map = xr.open_dataset(f"/net/rain/hyclimm/data/projects/SynFire/WP1/CCI_Land_Cover_Maps_EU/EU_C3S-LC-L4-LCCS-Map-300m-P1Y-{lc_year}-v2.1.1.nc")

    
    #get land cover code based on land cover map, set crs and spatial dimensions
    lc = lc_map["lccs_class"]
    lc = lc.rio.write_crs("EPSG:4326")
    lc = lc.rio.set_spatial_dims(x_dim='lon', y_dim='lat')

    #iterate over fire obs in the fire_of_year
    for index, row in land_cover.iterrows():

        #get id
        fire_id = index

        #get fire obs according to id -- make sure this is a geodataframe but not a geoseries!
        fire = fire_of_year.loc[[fire_id]]

        #clip
        fire_lc = lc.rio.clip(fire.geometry, fire.crs, drop = True, all_touched = True)

        #get and record lc distribution
        unique, count = np.unique(fire_lc, return_counts = True)
        land_cover.at[fire_id, "lc_unique"] = list(unique)
        land_cover.at[fire_id, "lc_count"] = list(count)

    #make the patch_id a normal column
    land_cover = land_cover.reset_index()
    print(rf"{year} finished!")
        
    return land_cover

#parallelize over 2001-2020
pool = mp.Pool(20)
results = pool.map(get_land_cover, range(2001, 2021))
pool.close()
results = pd.concat(results, ignore_index = True)
results.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover.csv", index = False)