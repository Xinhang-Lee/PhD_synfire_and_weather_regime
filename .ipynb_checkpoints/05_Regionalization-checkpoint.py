'''
Regionalize fire observations.
'''

import pandas as pd
import geopandas as gpd
import numpy as np
import regionmask
import xarray as xr
import cartopy.crs as ccrs
from shapely.geometry import box
from shapely.geometry import Polygon
from shapely.geometry import Point


# # Load Data
# load fire observations
fire_obs = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Fire_Observations/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations.shp")

# load study area
study_area = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Study_Area_32E.shp")


# # Regionalization (10 regions)
# British Isles (BI)
BI = box(-11, 50, 1.5, 65)

# Iberian Peninsula (IP)
IP = box(-10, 34, 5, 44)

# France (FR)
FR = box(-5, 44, 5, 50)

# Mid Europe (ME)
ME = Polygon([(1.5, 50), (1.5, 55), (19, 55), (19, 50), (15, 50), (15, 48), (5, 48), (5, 50)])

# The alps (AL)
AL = box(5, 44, 15, 48)

# South Eastern Europe (SEA)
SEA = box(15, 44, 32, 50)

# North Eastern Europe (NEA)
NEA = box(19, 50, 32, 60)

# Scandinavia (SC)
SC = Polygon([(4, 55), (4, 71.5), (32, 71.5), (32, 60), (19, 60), (19, 55)])

# West Mediterranean (WMD)
WMD = Polygon([(5, 44), (5, 34), (19, 34), (19, 40), (15, 44)])

# East Mediterranean (EMD)
EMD = Polygon([(15, 44), (32, 44), (32, 34), (19, 34), (19, 40)])


region_ten = gpd.GeoDataFrame({'geometry': [BI, IP, FR, ME, AL, SEA, NEA, SC, WMD, EMD],
                               'region': ["British Isles", "Iberian Peninsula", "France", "Mid Europe", "The alps", "South Eastern Europe", "North Eastern Europe", "Scandinavia", "West Mediterranean", "East Mediterranean"],
                               'abbrev': ["BI", "IP", "FR", "ME", "AL", "SEA", "NEA", "SC", "WMD", "EMD"]
                              }, 
                               crs = "EPSG:4326")


region_ten.to_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/Region_Ten.shp")


# Assign each fire to a region
fire_obs["region"] = pd.Series(np.nan, dtype = "str")

for index, fire in fire_obs.iterrows():
    
    fire_geometry = gpd.GeoSeries(data = [fire.geometry], crs = "EPSG:4326")
    fire_geometry = fire_geometry.to_crs(epsg = 3035)
    
    #get fire centroid in 3035 prj
    fire_centroid = zip(fire_geometry.geometry.centroid.x, fire_geometry.geometry.centroid.y)

    #reproject the centroid to 4326 prj
    gdf_fire_centroid = gpd.GeoDataFrame({'geometry': [Point(fire_centroid)]}, crs = "EPSG:3035")
    gdf_fire_centroid = gdf_fire_centroid.to_crs(epsg = 4326)

    for _, box in region_ten.iterrows():
        bounding_box = box.geometry
        if gdf_fire_centroid.geometry.iloc[0].covered_by(bounding_box):  #both inside and on the border
            fire_obs.loc[index, "region"] = box["abbrev"]
            break


fire_obs.to_file("/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/FRYv2.0_FireCCI51_6D_2001-2020_study_area_fire_observations_w_region.shp")


# Fire Seasonality
region_list = ["BI", "IP", "FR", "ME", "AL", "SEA", "NEA", "SC", "WMD", "EMD"]
fire_seas = pd.DataFrame(columns = ["Region", "Month", "EV", "BA"]) #region, month, number of fire event, cumulative burned area
months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


for reg in region_list:
    
    fire_obs_reg = fire_obs[fire_obs["region"] == reg].copy()
    fire_obs_reg["start_date"] = pd.to_datetime(fire_obs_reg["start_date"])
    fire_seas_reg = pd.DataFrame(columns = ["Region", "Month", "EV", "BA"])
    fire_seas_reg["Region"] = [reg for _ in range(12)]
    fire_seas_reg["Month"] = months
    
    for ind, mon in enumerate(months):
        
        fire_obs_reg_mon = fire_obs_reg[fire_obs_reg['start_date'].dt.month == ind+1]
        
        #avg number of events across the 20 yrs period
        EV = len(fire_obs_reg_mon)/20
        
        #avg cumulative burned area across the 20 yrs period
        BA = fire_obs_reg_mon["area"].sum()/20

        fire_seas_reg.loc[fire_seas_reg["Month"] == mon, "EV"] = EV
        fire_seas_reg.loc[fire_seas_reg["Month"] == mon, "BA"] = BA

    # rescaled to monthly maximal values
    fire_seas_reg["EV"] = fire_seas_reg["EV"]/(fire_seas_reg["EV"].max())
    fire_seas_reg["BA"] = fire_seas_reg["BA"]/(fire_seas_reg["BA"].max())

    fire_seas = pd.concat([fire_seas, fire_seas_reg], ignore_index = True)


fire_seas.to_csv("/net/rain/hyclimm/data/projects/SynFire/WP1/Regionalization/Fire_Seasonality.csv", index = False)



# # Create Region Masks
region_land = gpd.GeoDataFrame({"geometry": [gpd.clip(study_area, geom.geometry).unary_union for _, geom in region_ten.iterrows()],
                                "region": region_ten["region"],
                                "abbrev": region_ten["abbrev"]},
                               crs = "EPSG:4326")

land_mask_binary = xr.open_dataarray("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Land_Mask_Binary_CERRA_reproject_EPSG4326_study_area_32E.nc")




region_mask = regionmask.mask_geopandas(region_land, land_mask_binary.x.values, land_mask_binary.y.values)
region_mask = region_mask.rename({"lat": "y", "lon": "x"})



# save
region_mask.to_netcdf("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Region_Mask_CERRA_reproject_EPSG4326_Ten.nc")
