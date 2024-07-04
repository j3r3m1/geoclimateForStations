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
import subprocess
import pandas as pd
import geopandas as gpd
import numpy as np
import glob
import logging
from geoclimatetool.functions.globalVariables import *
from geoclimatetool.functions.otherFunctions import runProcess

# Define whether or not the QGIS environment should be used
qgis_env = True

##############################################################################
################## DEFINE VARIABLES ##########################################
##############################################################################
# Declare what to do in the code (in case some steps have already been done)
step2 = True
step3 = False
step4 = False
step5 = False

# Define the folder where you want the results to be saved
output_folder = "/home/bernardj/Code/geoclimateForStations/Data/Results"

# Location of the BDTOPO data
#bdtopo_path = "/cnrm/ville/USERS/bernardj/Data/BDTOPO_3-3_TOUSTHEMES_SHP_LAMB93_R11_2024-03-15/"
bdtopo_path = "/home/bernardj/Data/BDT/V3/BDTOPO_3-3_TOUSTHEMES_SHP_LAMB93_R11_2024-03-15"

# Define the file path where are saved informations about stations, and column names
#station_location_path = "/cnrm/ville/USERS/bernardj/Data/PANAME/Liste_stations_LCZs.csv"
station_location_path = "/home/bernardj/Code/geoclimateForStations/Data/Liste_stations_LCZs.csv"
station_lat = "LAT"
station_lon = "LON"
station_name = "ID"
buffer_size_list = [100, 300, 500]

# Where to save GeoClimate outputs
#geoclimate_output_loc = "/cnrm/ville/USERS/bernardj/Data/PANAME/GeoClimateData"
geoclimate_output_loc = "/home/bernardj/Code/geoclimateForStations/Data/GeoClimateData"

# Need to set where groovy is installed on the computer to add it in the path of the cmd
groovy_loc = "/home/bernardj/.sdkman/candidates/groovy/current/bin"

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
################## ADD GROOVY TO ENV VAR #####################################
##############################################################################
os.environ["PATH"] += ":" + groovy_loc


##############################################################################
################## 1. IDENTIFY ZONES TO DOWNLOAD #############################
##############################################################################
datasets = {"OSM" : 4326, "BDTOPO_V3" : 2154}

df_stations = pd.read_csv(station_location_path,
                          header = 0, 
                          index_col = station_name,
                          sep = ";",
                          decimal = ",")
gdf_station = gpd.GeoDataFrame(df_stations, geometry=gpd.GeoSeries.from_xy(df_stations[station_lon], 
                                                                           df_stations[station_lat]),
                               crs=4326).to_crs(f"EPSG:{datasets['BDTOPO_V3']}")
# Save stations as GIS file
gdf_station.to_file(".".join(station_location_path.split(".")[0:-1]) + ".fgb")
buff_max = max(buffer_size_list)

# # Union the largest buffers together and explode to a limited number of geometries
# gdf_zones = gpd.GeoSeries(gdf_station.buffer(buff_max).unary_union, 
#                           crs = f"EPSG:{datasets['BDTOPO_V3']}").explode(ignore_index = True)
# Union the largest buffers together and explode to a limited number of geometries
gdf_zones = gpd.GeoSeries(gdf_station.buffer(buff_max), 
                          crs = f"EPSG:{datasets['BDTOPO_V3']}")
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
#datasets = {"OSM" : 4326}



##############################################################################
################## 2. FORMAT DATA TO GEOCLIMATE INPUTS #######################
##############################################################################
if step2:
    for dt, epsg in datasets.items():
        # Need to change format to remove some of the bbox based on their index
        gdf_zones_bbox_tmp = pd.Series(gdf_zones_bbox[dt], gdf_zones_str[dt].index)
        # Config file is slightly different between BDTopo and OSM
        if dt == "OSM":
            input_data = None
        else:
            input_data = bdtopo_path 
        
        # Remove from the list the bbox that have already been calculated
        if dt[0:3].lower() == "bdt":
            dt_name = f"bdtopo_{dt[-1:]}"
        else:
            dt_name = "osm"
        for i, zone in gdf_zones_str[dt].items():
            if os.path.exists(os.path.join(geoclimate_output_loc, 
                                           f"{dt_name}_{zone}")):
                gdf_zones_bbox_tmp.drop(i, inplace = True)
        gdf_zones_bbox_tmp = list(gdf_zones_bbox_tmp.values)
        # Calculations are processed
        if len(gdf_zones_bbox_tmp)>0:
            processing.run("GeoClimateTool:coolparkstool_process", 
                           {'INPUT_DATASET':list(DATASETS.columns).index(dt),
                            'INPUT_DIRECTORY':input_data,
                            'ESTIMATED_HEIGHT':True,
                            'LCZ_CALC':True,
                            'UTRF_CALC':False,
                            'WRF_INPUTS':False,
                            'TEB_INPUTS':False,
                            'SVF_SIMPLIFIED':True,
                            'LOCATION':str(gdf_zones_bbox_tmp)[1:-1],
                            'EXTENT':None,
                            'OUTPUT_DIRECTORY':geoclimate_output_loc,
                            'LOAD_INPUTS':False,
                            'LOAD_OUTPUTS':False,
                            'STYLE_LANGUAGE':0})


##############################################################################
########### 3. CALCULATE BUFFERS AROUND STATIONS FOR EACH ZONE ###############
##############################################################################
buffer_file_name = "station_buffers.geojson"
# Allocate to each station the corresponding zone
for dt in datasets:
    list_lcz = {buf: [] for buf in buffer_size_list}
    list_indic = {buf: [] for buf in buffer_size_list}
    for i in gdf_zones_epsg[dt].index:
        # Save the buffer file into the corresponding zone folder for each type of data
        if dt[0:3].lower() == "bdt":
            dt_name = f"bdtopo_{dt[-1:]}"
        else:
            dt_name = "osm"
        station_buff_file = os.path.join(geoclimate_output_loc, 
                                         f"{dt_name}_{gdf_zones_str[dt].loc[i]}",
                                         buffer_file_name)
        if step3:
            list_by_buf = []
            # For each buffer size
            for buf_siz in buffer_size_list:
                # Identify the CRS of the GeoClimate data for this specific location
                epsg = gpd.read_file(os.path.join(geoclimate_output_loc, 
                                                 f"{dt_name}_{gdf_zones_str[dt].loc[i]}",
                                                 "zone.fgb")).crs.to_epsg()
                # Identify which station are in each zone (corresponding to a GeoClimate output folder)
                # stations_by_zone_tmp = gdf_station[gdf_station.geometry.to_crs(f"EPSG:{datasets[dt]}")\
                #                                   .intersects(gdf_zones_epsg[dt].loc[i])]
                stations_by_zone_tmp = gdf_station[gdf_zones_epsg[dt].loc[[i]].to_crs(f"EPSG:{epsg}")\
                                                   .covers(gdf_station.to_crs(f"EPSG:{epsg}"), True)]
                stations_by_zone_tmp = stations_by_zone_tmp.to_crs(f"EPSG:{epsg}").buffer(buf_siz)
                stations_by_zone_tmp.index = stations_by_zone_tmp.index + buf_siz
                list_by_buf.append(stations_by_zone_tmp)
            # Gather all sizes of buffer geometries into a single file
            pd.concat(list_by_buf).to_file(station_buff_file)
##############################################################################
################## 4. CALCULATE INDICATORS FOR EACH BUFFER ###################
##############################################################################
        if step4:        
            try:
                # Get the name of the buffer station ID column
                id_st = gpd.read_file(station_buff_file).columns[0]
            except: 
                raise Exception("There is no buffer file, it seems you should do step 3 before step 4") 
                
            # Name of the output folder
            out_file = os.path.join(output_folder, f"{dt_name}_{gdf_zones_str[dt].loc[i]}")
            if not os.path.exists(out_file):
                os.mkdir(out_file)
            # Test the number of files inside the output dir, rerun if not 2
            nb_files = len(glob.glob(os.path.join(out_file, "*")))
            if nb_files == 1:
                print("Strange that only one output file is present for folder 'out_file'. Delete the folder and rerun")
            elif len(glob.glob(os.path.join(out_file, "*"))) == 2:
                pass
            else:
                # Folder where are saved the GeoClimate results
                geoc_res_fold = os.path.join(geoclimate_output_loc, 
                                             f"{dt_name}_{gdf_zones_str[dt].loc[i]}")
                groovy_cmd = f"""groovy {os.path.abspath(os.path.join(os.curdir, "LczForStationBuffer", "src", "main", "groovy", "Main.groovy"))} {station_buff_file} {out_file} {geoc_res_fold} {id_st}"""
                
                # Execute the GeoClimate workflow and log informations
                try:
                    for line in runProcess(groovy_cmd.split()):
                        print(line.decode("utf8"))
                except:
                    raise Exception("Groovy script was not run")

##############################################################################
################## 5. GATHER RESULTS IN A SINGLE FILE ########################
##############################################################################
        if step5:
            try:
                tmp_lcz = gpd.read_file(os.path.join(out_file, "RSU_LCZ.fgb"))
                idx = tmp_lcz.ID[0] - buffer_size_list[0]
                tmp_lcz.drop("ID", axis = 1, inplace = True)
                for i, buf in enumerate(buffer_size_list):
                    list_lcz[buf].append(tmp_lcz.loc[[i],:].set_index(pd.Index([idx])))
            except:
                raise Exception(f"There is no output RSU_LCZ file for folder {out_file}")

            try:
                tmp_indic = gpd.read_file(os.path.join(out_file, "RSU_INDICATORS.fgb"))
                idx = tmp_indic.ID[0] - buffer_size_list[0]
                tmp_indic.drop("ID", axis = 1, inplace = True)
                for i, buf in enumerate(buffer_size_list):
                    list_indic[buf].append(tmp_indic.loc[[i],:].set_index(pd.Index([idx])))
            except:
                raise Exception(f"There is no output RSU_INDICATOR file for folder {out_file}")

    if step5:
        # Gather all results
        rsu_lcz = {buf : pd.concat(list_lcz[buf]) for i, buf in enumerate(buffer_size_list)}
        rsu_indic = {buf : pd.concat(list_indic[buf]) for i, buf in enumerate(buffer_size_list)}
        
        for buf in buffer_size_list:
            rsu_lcz[buf].to_file(os.path.join(output_folder, f"{dt}_RSU_LCZ_{buf}m.fgb"))
            rsu_indic[buf].to_file(os.path.join(output_folder, f"{dt}_RSU_INDICATORS_{buf}m.fgb"))
                