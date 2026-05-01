import geopandas as gpd

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