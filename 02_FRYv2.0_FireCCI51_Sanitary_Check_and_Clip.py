'''
Stack FRYv2.0_FireCCI51_6D and FRYv2.0_FireCCI51_12D from 2001 to 2020 and clip to the European domain.
'''

import xarray as xr
import pandas as pd
import geopandas as gpd
import multiprocessing as mp
from shapely.geometry import box



def read_and_clip(filepath, mask):
    data = gpd.read_file(filepath)
    data = data.to_crs(epsg = 4326)
    data_clipped = gpd.clip(data, mask)
    return data_clipped

#bounding box
bbox = box(-11, 33, 35, 72)

#----------------------------
#6D
fry_6d_paths = [rf"/net/krypton/hyclimm/data/public/hazard/obs/fire_observations/FRYv2.0_FireCCI51/SHP_6D/FRY_fire_patches_POLYGON_6D_{yr}.shp" for yr in range(2001, 2021)]

pool = mp.Pool(20)
fry_6d_stack_eubbox = pd.concat([pool.apply(read_and_clip, args=(path, bbox)) for path in fry_6d_paths], ignore_index = True)
pool.close()

#save
fry_6d_stack_eubbox.to_file("/net/krypton/hyclimm/data/public/hazard/obs/fire_observations/FRYv2.0_FireCCI51/SHP_6D/FRYv2.0_FireCCI51_6D_2001-2020_EUbbox.shp")

#----------------------------
#12D
fry_12d_paths = [rf"/net/krypton/hyclimm/data/public/hazard/obs/fire_observations/FRYv2.0_FireCCI51/SHP_12D/FRY_fire_patches_POLYGON_12D_{yr}.shp" for yr in range(2001, 2021)]

pool = mp.Pool(20)
fry_12d_stack_eubbox = pd.concat([pool.apply(read_and_clip, args=(path, bbox)) for path in fry_12d_paths], ignore_index = True)
pool.close()

#save
fry_12d_stack_eubbox.to_file("/net/krypton/hyclimm/data/public/hazard/obs/fire_observations/FRYv2.0_FireCCI51/SHP_12D/FRYv2.0_FireCCI51_12D_2001-2020_EUbbox.shp")

