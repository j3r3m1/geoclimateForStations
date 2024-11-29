# -*- coding: utf-8 -*-
"""
This script:
    1. Identify zones to study according to station location and buffer size
    2. For each of these buffers, convert OSM or BDTOPO data into GeoClimate standard inputs
    3. Creates buffers of different sizes around stations for each zone to study
    4. Calculates LCZ and other indicators for each of these buffers

@author: Jérémy Bernard, CNRM, CNRS
"""

# Necessary imports in all situations
import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
from geoclimatetool.functions.globalVariables import *
from geoclimatetool.functions.otherFunctions import runProcess

# Define whether or not the QGIS environment should be used
qgis_env = True

##############################################################################
################## DEFINE VARIABLES ##########################################
##############################################################################
# Declare what to do in the code (in case some steps have already been done)
step2 = False
step3 = False
step4 = True

# Location of the BDTOPO data
bdtopo_path = "/cnrm/ville/USERS/bernardj/Data/BDTOPO_3-3_TOUSTHEMES_SHP_LAMB93_R11_2024-03-15/"

# Define the file path where are saved informations about stations, and column names
station_location_path = "/cnrm/ville/USERS/bernardj/Data/PANAME/Liste_stations_LCZs.csv"
station_lat = "LAT"
station_lon = "LON"
station_name = "SITE"
buffer_size_list = [100, 300, 500]

# Where to save GeoClimate outputs
geoclimate_output_loc = "/home/bernardj/Code/geoclimateForStations/Data/GeoClimateData"




##############################################################################
################## ACTIVATE QGIS ENV #########################################
##############################################################################
# Variables for using the QGIS plugins installed on computer (such as GeoClimateTool tool)
path_for_processing = "/usr/share/qgis/python/plugins"
if os.path.exists(path_for_processing):
    sys.path.append(path_for_processing)

# Necessary imports if QGIS environment is used
from qgis.core import QgsApplication

# # Starts the qgis application without the graphical user interface
gui_flag = False
app = QgsApplication([], gui_flag)
app.initQgis()

import processing

from processing.core.Processing import Processing
Processing.initialize()

from geoclimatetool.processing_geoclimate_provider import ProcessingGeoClimateProvider
geoclimate_provider = ProcessingGeoClimateProvider()
QgsApplication.processingRegistry().addProvider(geoclimate_provider)


##############################################################################
################## 1. IDENTIFY ZONES TO DOWNLOAD #############################
##############################################################################
print("1. IDENTIFY ZONES TO DOWNLOAD")
datasets = {"OSM" : 4326, "BDTOPO_V3" : 2154}

df_stations = pd.read_csv(station_location_path,
                          header = 0, 
                          index_col = "SITE",
                          sep = ";",
                          decimal = ",")
gdf_station = gpd.GeoDataFrame(df_stations, geometry=gpd.GeoSeries.from_xy(df_stations[station_lon], 
                                                                           df_stations[station_lat]),
                       crs=4326).to_crs(f"EPSG:{datasets['BDTOPO_V3']}")
# Save stations as GIS file
gdf_station.to_file(".".join(station_location_path.split(".")[0:-1]) + ".fgb")
buff_max = max(buffer_size_list)

# Union the largest buffers together and explode to a limited number of geometries
gdf_zones = gpd.GeoSeries(gdf_station.buffer(buff_max).unary_union, 
                          crs = f"EPSG:{datasets['BDTOPO_V3']}").explode(ignore_index = True)
gdf_zones_epsg = {dt: gdf_zones.to_crs(f"EPSG:{epsg}") \
                  for dt, epsg in datasets.items()}
gdf_zones_bounds = {dt: gdf_zones_epsg[dt].bounds \
                    for dt, epsg in datasets.items()}
gdf_zones_str = {dt : data["miny"].astype(str) + "_" + data["minx"].astype(str)\
                 + "_" + data["maxy"].astype(str) + "_" + data["maxx"].astype(str)\
                     for dt, data in gdf_zones_bounds.items()}
gdf_zones_bbox = {dt : [[float(i) for i in j] for j in list(data.str.split("_").values)]\
                  for dt, data in gdf_zones_str.items()}

#datasets = {"BDTOPO_V3" : 2154}
datasets = {"OSM" : 4326}



##############################################################################
################## 2. FORMAT DATA TO GEOCLIMATE INPUTS #######################
##############################################################################
print("2. FORMAT DATA TO GEOCLIMATE INPUTS")
if step2:
    for dt in datasets:
        if dt == "OSM":
            input_data = None
        else:
            input_data = bdtopo_path 
        processing.run("GeoClimateTool:coolparkstool_process", 
                       {'INPUT_DATASET':list(DATASETS.columns).index(dt),
                        'INPUT_DIRECTORY':input_data,
                        'ESTIMATED_HEIGHT':True,
                        'LCZ_CALC':True,
                        'UTRF_CALC':False,
                        'WRF_INPUTS':False,
                        'TEB_INPUTS':False,
                        'SVF_SIMPLIFIED':True,
                        'LOCATION':str(gdf_zones_bbox[dt])[1:-1],
                        'EXTENT':None,
                        'OUTPUT_DIRECTORY':geoclimate_output_loc,
                        'LOAD_INPUTS':False,
                        'LOAD_OUTPUTS':False,
                        'STYLE_LANGUAGE':0})


##############################################################################
########### 3. CALCULATE BUFFERS AROUND STATIONS FOR EACH ZONE ###############
##############################################################################
print("3. CALCULATE BUFFERS AROUND STATIONS FOR EACH ZONE")
buffer_file_name = "station_buffer.fgb"
# Allocate to each station the corresponding zone
if step3:
    stations_by_zone = {}
    for dt in datasets:
        for i in gdf_zones_epsg[dt].index:
            stations_by_zone[i] = gdf_station[gdf_station.geometry.to_crs(f"EPSG:{datasets[dt]}")\
                                              .intersects(gdf_zones_epsg[dt].loc[i])]
            if dt[0:3].lower() == "bdt":
                dt_name = f"bdtopo_{dt[-1:]}"
            else:
                dt_name = "osm"
            stations_by_zone[i].to_file(os.path.join(geoclimate_output_loc, 
                                                     f"{dt_name}_{gdf_zones_str[dt].loc[i]}",
                                                     buffer_file_name))
            

##############################################################################
################## 4. CALCULATE INDICATORS FOR EACH BUFFER ###################
##############################################################################
print("4. CALCULATE INDICATORS FOR EACH BUFFER")
groovy_cmd = f'groovy {os.path.abspath(os.path.join(os.curdir, "LczForStationBuffer", "src", "main", "groovy", "Main.groovy"))}'

# Execute the GeoClimate workflow and log informations
try: 
    for line in runProcess(groovy_cmd.split()):
        print(line.decode("utf8"))
    executed = True
except:
    executed = False
if not executed:
    print("Groovy script was not run")