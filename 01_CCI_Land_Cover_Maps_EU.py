'''
Clip CCI land cover maps (from 2000 to 2022) to the MODIS Europe-North Africa continental tile.
'''


import geopandas as gpd
import pandas as pd
import xarray as xr
import multiprocessing as mp
import numpy as np
import rioxarray


def Clip_CCI_LC(yr):
    
    if yr <= 2015:
        ds = xr.open_dataset(f"/net/krypton/hyclimm/lixinh/CCI_LC_maps/nc_world/ESACCI-LC-L4-LCCS-Map-300m-P1Y-{yr}-v2.0.7cds.nc")
    else:
        ds = xr.open_dataset(f"/net/krypton/hyclimm/lixinh/CCI_LC_maps/nc_world/C3S-LC-L4-LCCS-Map-300m-P1Y-{yr}-v2.1.1.nc")

    #assign coordination system WGS84
    ds = ds.rio.write_crs('EPSG:4326')

    #use the MODIS regionalization to clip out Europe (according to FireCCI user guideline)
    clipped_ds = ds.sel(lat=slice(83, 25), lon=slice(-26, 53))
    clipped_ds = clipped_ds.rio.write_crs('EPSG:4326')

    #save
    if yr <= 2015:
        clipped_ds.to_netcdf(f"/net/krypton/hyclimm/data/projects/SynFire/WP1/CCI_Land_Cover_Maps_EU/EU_ESACCI-LC-L4-LCCS-Map-300m-P1Y-{yr}-v2.0.7cds.nc")
    else:
        clipped_ds.to_netcdf(f"/net/krypton/hyclimm/data/projects/SynFire/WP1/CCI_Land_Cover_Maps_EU/EU_C3S-LC-L4-LCCS-Map-300m-P1Y-{yr}-v2.1.1.nc")



#map over 2000-2022
mp.cpu_count() #64
pool = mp.Pool(5)    #specify number of cores
pool.map(Clip_CCI_LC, [year for year in range(2000, 2023)])
pool.close()

