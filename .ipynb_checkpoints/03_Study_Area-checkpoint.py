'''
Make study area shapefile.
'''

import numpy as np
import xarray as xr
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
from datetime import datetime, timedelta
from shapely.geometry import Polygon
from shapely.geometry import Point
import os
import regionmask
import cartopy.crs as ccrs
pd.set_option("display.max_rows", 100)


#country boundaries
cn_bd = gpd.read_file("/home/lixinh/Study_Area/Country_Boundary_eurostat/CNTR_RG_01M_2020_4326.shp")
cn_bd["EU_STAT"].unique()
cn_bd_eu = cn_bd[cn_bd["EU_STAT"] == "T"]
cn_bd_eu["NAME_ENGL"]  # 27 countries in EU


#add 14 missing regions
#Switzerland(CH), Norway(NO), England(UK), Andorra(AD), Liechtenstein(LI), Vatican City (VA), Faroe Islands(FO), and Jan Mayen(SJ)
#Bosnia and Herzegovina(BA), Montenegro(ME), Serbia(RS), Albania(AL), North Macedonia(MK), San Marino(SM)
# ISO 3166-1 alpha-2 country codes
CH = cn_bd[cn_bd["CNTR_ID"] == "CH"]   
NO = cn_bd[cn_bd["CNTR_ID"] == "NO"]
UK = cn_bd[cn_bd["CNTR_ID"] == "UK"]
AD = cn_bd[cn_bd["CNTR_ID"] == "AD"]
LI = cn_bd[cn_bd["CNTR_ID"] == "LI"]
VA = cn_bd[cn_bd["CNTR_ID"] == "VA"]
FO = cn_bd[cn_bd["CNTR_ID"] == "FO"]
SJ = cn_bd[cn_bd["CNTR_ID"] == "SJ"]
BA = cn_bd[cn_bd["CNTR_ID"] == "BA"]
ME = cn_bd[cn_bd["CNTR_ID"] == "ME"]
RS = cn_bd[cn_bd["CNTR_ID"] == "RS"]  #inlucdes Kosovo
AL = cn_bd[cn_bd["CNTR_ID"] == "AL"]
MK = cn_bd[cn_bd["CNTR_ID"] == "MK"]
SM = cn_bd[cn_bd["CNTR_ID"] == "SM"]




cn_bd_study = pd.concat([cn_bd_eu, CH, NO, UK, AD, LI, VA, FO, SJ, BA, ME, RS, AL, MK, SM], ignore_index=True)

#EnZ
eu_enz = gpd.read_file('/net/krypton/hyclimm/data/public/geospatial/regions/Environmental_Stratification_EU/EnSv8/EnSv8/enz_v8.shp')
eu_enz = eu_enz.to_crs(epsg = 4326) #WGS84 (EPSG: 4326)
eu_enz.plot(figsize = (15,15))


min_lon, min_lat, max_lon, max_lat = eu_enz.total_bounds
bounding_box = box(min_lon, min_lat, max_lon, max_lat)
cn_bd_study = gpd.clip(cn_bd_study, bounding_box)

#save
cn_bd_study.to_file("/home/lixinh/Study_Area/Study_Area.shp")


# Make a Clear-cut Eastern Boundary
# add Russia, Belarus, Ukraine, Moldova, Turkey
cn_bd = gpd.read_file("/home/lixinh/Study_Area/Country_Boundary_eurostat/CNTR_RG_01M_2020_4326.shp")
RU = cn_bd[cn_bd["CNTR_ID"] == "RU"]
BY = cn_bd[cn_bd["CNTR_ID"] == "BY"]
UA = cn_bd[cn_bd["CNTR_ID"] == "UA"]
MD = cn_bd[cn_bd["CNTR_ID"] == "MD"]
TR = cn_bd[cn_bd["CNTR_ID"] == "TR"]

eastern_cn = pd.concat([RU, BY, UA, MD, TR], ignore_index = True)
eastern_bbox = box(19, 33, 32, 72)
eastern_cn_clipped = gpd.clip(eastern_cn, eastern_bbox)

study_area = gpd.read_file("/home/lixinh/Study_Area/Study_Area.shp")
study_area_final = pd.concat([study_area, eastern_cn_clipped], ignore_index = True)
#save
study_area_final.to_file("/home/lixinh/Study_Area/Study_Area_Final.shp")


# Exclude Cyprus
study_area_final = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Study_Area_Final.shp")
study_area_32E = study_area_final[study_area_final["NAME_ENGL"] != "Cyprus"].reset_index(drop = True)
study_area_32E.to_file("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Study_Area_32E.shp")


# Create 2D land mask based on CERRA grid in EPSG:4326 for the study domain (Lon: -12-34, Lat: 72-33)
fwi = xr.open_dataarray(f"/net/rain/hyclimm/data/projects/fofix/standardize_Cerra_empirically_original_res_from_shell/fwi_cerra_grid_2001-01-01_2020-12-31.nc", decode_coords = "all").sel(time = "2001-08-01")   # as a reference for CERRA grid

# define CERRA projection
proj_cerra = ccrs.LambertConformal(central_longitude=8,
                                   central_latitude=50,       
                                   false_easting=2937000.506058639,        
                                   false_northing=2937000.470434457,        
                                   standard_parallels=(50, 50))     # "+proj=lcc +lat_1=50 +lat_2=50 +lon_0=8 +lat_0=50 +x_0=2937000.506058639 +y_0=2937000.470434457 +datum=WGS84"

fwi = fwi.rio.write_crs(proj_cerra)
fwi_lonlat = fwi.rio.reproject("EPSG:4326")
fwi_lonlat = fwi_lonlat.sel(x = slice(-12, 34), y = slice(72, 33))

study_area_32E = gpd.read_file("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Study_Area_32E.shp")
lon = fwi_lonlat.x.values
lat = fwi_lonlat.y.values
land_mask = regionmask.mask_geopandas(study_area_32E, lon, lat)   # create land mask from geodataframe
land_mask = land_mask.rename({"lat": "y", 
                              "lon": "x"})
land_mask.to_netcdf("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Land_Mask_Country_CERRA_reproject_EPSG4326_study_area_32E.nc")


#Binary land mask for the land (1)
land_mask_binary = xr.DataArray(data = np.where(land_mask.isnull(), 0, 1),
                                coords = dict(y = land_mask.y, x = land_mask.x))

#save
land_mask_binary.to_netcdf("/net/rain/hyclimm/data/projects/SynFire/Study_Area/Land_Mask_Binary_CERRA_reproject_EPSG4326_study_area_32E.nc")

