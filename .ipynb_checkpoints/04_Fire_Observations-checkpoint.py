'''
Process satellite fire observations.
This is a combined script. 
'''

# clip
#---------------------------------------------------------
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import numpy as np
import rioxarray as rio
import xarray as xr
import multiprocessing as mp
import ast
import warnings

#load study area 
study_area = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Study_Area_Final.shp")

#unify 
study_area_unify = study_area.geometry.unary_union

#load fires in the EU bbox
fry = gpd.read_file("/net/rain/hyclimm/data/public/hazard/obs/fire_observations/FRYv2.0_FireCCI51/SHP_6D/FRYv2.0_FireCCI51_6D_2001-2020_EUbbox.shp")

#intersection
fry_study_area_border_incl = fry[fry.intersects(study_area_unify)]

#save
fry_study_area_border_incl.to_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/FRYv2.0_FireCCI51_6D_2001-2020_study_area_final_incl_border.shp")


#get fire land cover
#---------------------------------------------------------
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

        #get fire obs according to id
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


#get Cyprus shapefile
study_area = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Study_Area_Final.shp")
cyprus = study_area[study_area["NAME_ENGL"] == "Cyprus"]

#get the id list for fires in Cyprus
fry_study_area_border_incl = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/FRYv2.0_FireCCI51_6D_2001-2020_study_area_final_incl_border.shp")
fry_cyprus = fry_study_area_border_incl[fry_study_area_border_incl.intersects(cyprus.geometry.unary_union)]
fry_cyprus_id = list(fry_cyprus["ptch_id"]) # n = 99


#exclude from fire polygon shapefiles
fry_study_area_32e = fry_study_area_border_incl[~fry_study_area_border_incl["ptch_id"].isin(fry_cyprus_id)]
fry_study_area_32e.to_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/FRYv2.0_FireCCI51_6D_2001-2020_study_area_32e_incl_border.shp")


#exclude from fire land cover dataframe
fire_land_cover = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover.csv")
fire_land_cover_32e = fire_land_cover[~fire_land_cover["ptch_id"].isin(fry_cyprus_id)]
fire_land_cover_32e.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover_32e.csv", index = False)


# Calculate coverage of natural vegetation based on level 1 land cover code
#---------------------------------------------------------
fire_land_cover_32e = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover_32e.csv")
#check NA
print(fire_land_cover_32e["lc_unique"].apply(lambda x: any(pd.isna(num) for num in ast.literal_eval(x))).any())
print(fire_land_cover_32e["lc_count"].apply(lambda x: any(pd.isna(num) for num in ast.literal_eval(x))).any())
# further checks
fire_land_cover_32e["lc_unique"].apply(lambda x: len(x) == 0).any() #no empty lc_unique
fire_land_cover_32e["lc_count"].apply(lambda x: len(x) == 0).any()  #no empty lc_count
fire_land_cover_32e["lc_unique"].apply(lambda x: len(x) == 1 and x[0] == 0).any()   #no fire patch with only 0 (No Data) land cover codes

# calculate natural vegetation coverage
fire_land_cover_32e = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover_32e.csv")
fire_land_cover_32e["lc_unique_L1"] = pd.NA
fire_land_cover_32e["lc_count_L1"] = pd.NA
fire_land_cover_32e["coverage_50_150"] = pd.NA
fire_land_cover_32e["coverage_50_180"] = pd.NA

#set ptch_id as index
fire_land_cover_32e.set_index("ptch_id", inplace = True)

for index, row in fire_land_cover_32e.iterrows():

    lc_unique = ast.literal_eval(fire_land_cover_32e.loc[index, "lc_unique"])
    lc_count = ast.literal_eval(fire_land_cover_32e.loc[index, "lc_count"])

    #get rid of 0 (mostly come from border in the cropping process, could also come from No Data in the CCI LC maps (see user guide for more details), don't consider them in the vegetation coverage calculation)
    if lc_unique[0] == 0:
        lc_unique = lc_unique[1:]
        lc_count = lc_count[1:]

    #two empty lists to store unique level 1 land cover codes and counts
    lc_unique_L1 = []
    lc_count_L1 = []
    
    #--------------------------------------------------------------
    #calculate unique and count based on level1(L1) land cover code
    if 10 in lc_unique or 11 in lc_unique or 12 in lc_unique:
        lc_unique_L1.append(10)
        count = 0
        for lc in [10, 11, 12]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 20 in lc_unique:
        lc_unique_L1.append(20)
        lc_count_L1.append(lc_count[lc_unique.index(20)])
    if 30 in lc_unique:
        lc_unique_L1.append(30)
        lc_count_L1.append(lc_count[lc_unique.index(30)])
    if 40 in lc_unique:
        lc_unique_L1.append(40)
        lc_count_L1.append(lc_count[lc_unique.index(40)])
    if 50 in lc_unique:
        lc_unique_L1.append(50)
        lc_count_L1.append(lc_count[lc_unique.index(50)])
    if 60 in lc_unique or 61 in lc_unique or 62 in lc_unique:
        lc_unique_L1.append(60)
        count = 0
        for lc in [60, 61, 62]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 70 in lc_unique or 71 in lc_unique or 72 in lc_unique:
        lc_unique_L1.append(70)
        count = 0
        for lc in [70, 71, 72]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 80 in lc_unique or 81 in lc_unique or 82 in lc_unique:
        lc_unique_L1.append(80)
        count = 0
        for lc in [80, 81, 82]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 90 in lc_unique:
        lc_unique_L1.append(90)
        lc_count_L1.append(lc_count[lc_unique.index(90)])
    if 100 in lc_unique:
        lc_unique_L1.append(100)
        lc_count_L1.append(lc_count[lc_unique.index(100)])
    if 110 in lc_unique:
        lc_unique_L1.append(110)
        lc_count_L1.append(lc_count[lc_unique.index(110)])
    if 120 in lc_unique or 121 in lc_unique or 122 in lc_unique:
        lc_unique_L1.append(120)
        count = 0
        for lc in [120, 121, 122]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 130 in lc_unique:
        lc_unique_L1.append(130)
        lc_count_L1.append(lc_count[lc_unique.index(130)])
    if 140 in lc_unique:
        lc_unique_L1.append(140)
        lc_count_L1.append(lc_count[lc_unique.index(140)])
    if 150 in lc_unique or 151 in lc_unique or 152 in lc_unique or 153 in lc_unique:
        lc_unique_L1.append(150)
        count = 0
        for lc in [150, 151, 152, 153]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 160 in lc_unique:
        lc_unique_L1.append(160)
        lc_count_L1.append(lc_count[lc_unique.index(160)])
    if 170 in lc_unique:
        lc_unique_L1.append(170)
        lc_count_L1.append(lc_count[lc_unique.index(170)])
    if 180 in lc_unique:
        lc_unique_L1.append(180)
        lc_count_L1.append(lc_count[lc_unique.index(180)])
    if 190 in lc_unique:
        lc_unique_L1.append(190)
        lc_count_L1.append(lc_count[lc_unique.index(190)])
    if 200 in lc_unique or 201 in lc_unique or 202 in lc_unique:
        lc_unique_L1.append(200)
        count = 0
        for lc in [200, 201, 202]:
            if lc in lc_unique:
                count += lc_count[lc_unique.index(lc)]
        lc_count_L1.append(count)
    if 210 in lc_unique:
        lc_unique_L1.append(210)
        lc_count_L1.append(lc_count[lc_unique.index(210)])
    if 220 in lc_unique:
        lc_unique_L1.append(220)
        lc_count_L1.append(lc_count[lc_unique.index(220)])
        
    #--------------------------------------------------------------
    #assign level 1 land cover unique values and counts
    fire_land_cover_32e.at[index, "lc_unique_L1"] = list(lc_unique_L1)
    fire_land_cover_32e.at[index, "lc_count_L1"] = list(lc_count_L1)

    #--------------------------------------------------------------
    n_cell = sum(lc_count_L1)

    #--------------------------------------------------------------
    #calculate natural vegetation coverage based on L1 50-150
    n_cell_50_150 = 0
    for lc in lc_unique_L1:
        if lc >= 50 and lc <= 150:
            n_cell_50_150 += lc_count_L1[lc_unique_L1.index(lc)]
    coverage_50_150 = n_cell_50_150/n_cell 
    
    #assign values
    fire_land_cover_32e.at[index, "coverage_50_150"] = coverage_50_150

    #--------------------------------------------------------------
    #caulculate natural vegetation coverage based on L1 50-180
    n_cell_50_180 = 0
    for lc in lc_unique_L1:
        if lc >= 50 and lc <= 180:
            n_cell_50_180 += lc_count_L1[lc_unique_L1.index(lc)]
    coverage_50_180 = n_cell_50_180/n_cell 
    
    #assign values
    fire_land_cover_32e.at[index, "coverage_50_180"] = coverage_50_180



fire_land_cover_32e["lc_unique_L1"].apply(lambda x: len(x) == 0).any()     #no empty lists in the column lc_unique_L1
fire_land_cover_32e["lc_count_L1"].apply(lambda x: len(x) == 0).any()     #no empty lists in the column lc_count_L1





fire_land_cover_32e =fire_land_cover_32e.reset_index()
fire_land_cover_32e.rename(columns={'lc_unique': 'lc_unique_L2', 'lc_count': 'lc_count_L2'}, inplace=True)
fire_land_cover_32e.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover_32e_L1_L2_L1natvegcover.csv", index = False)


#---------------------------------------------------------
# Land Cover Filtration (>= 80% coverage of LC codes 50-150)   after: n = 20117
fire_land_cover_32e = pd.read_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/fire_land_cover_32e_L1_L2_L1natvegcover.csv")
fire_natveg_id = list(fire_land_cover_32e[fire_land_cover_32e["coverage_50_150"] >= 0.8]["ptch_id"])
fry_study_area_32e_natveg = fry_study_area_32e[fry_study_area_32e["ptch_id"].isin(fire_natveg_id)]


# Border Filtration (remove all fire polygons crossing 32E)  after: n = 20103
# 32E
coords = [(32, 90), (32, -90)]
x_32E = LineString(coords)
print(x_32E)
fry_study_area_32e_natveg_borderexcl = fry_study_area_32e_natveg[~fry_study_area_32e_natveg.geometry.crosses(x_32E)]

# Temporal Correction
len(fry_study_area_32e_natveg_borderexcl[fry_study_area_32e_natveg_borderexcl["mindtc_frp"].isna() & fry_study_area_32e_natveg_borderexcl["minBD"].notna()])  # 1021 fires with only BA starting date
len(fry_study_area_32e_natveg_borderexcl[fry_study_area_32e_natveg_borderexcl["mindtc_frp"].notna() & fry_study_area_32e_natveg_borderexcl["minBD"].notna()])  # 19082 fires with both BA and AF starting date (use AF)


fry_study_area_32e_natveg_borderexcl_tcor = fry_study_area_32e_natveg_borderexcl.copy()
fry_study_area_32e_natveg_borderexcl_tcor["start_date"] = np.where(fry_study_area_32e_natveg_borderexcl['mindtc_frp'].notna(), fry_study_area_32e_natveg_borderexcl['mindtc_frp'], fry_study_area_32e_natveg_borderexcl['minBD'])

#reset index
fry_study_area_32e_natveg_borderexcl_tcor = fry_study_area_32e_natveg_borderexcl_tcor.reset_index(drop = True)


# Save
fry_study_area_32e_natveg_borderexcl_tcor.to_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations.shp")

