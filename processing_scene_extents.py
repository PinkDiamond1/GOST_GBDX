####################################################################################################
# Calculate imagery extents
# Benjamin Stewart, May 2018
# Purpose: calculate better used extents for imagery results for Mexico
####################################################################################################

import os, sys

import pandas as pd
import geopandas as gpd

from shapely.wkt import loads
from shapely.ops import cascaded_union

inScenes = r"Q:\WORKINGPROJECTS\Mexico_Poverty\CSV for analysis\Complete list of scene IDs\Scene_List_Mexico_complete.txt"
outputFile = inScenes.replace(".txt", "_newExtent2.csv")
inShapes = r"Q:\WORKINGPROJECTS\Mexico_Poverty\agebs\urban_agebs.shp"
unProcessedFiles = r"Q:\WORKINGPROJECTS\Mexico_Poverty\agebs\UrbanAgebsurban_agebs_unprocessed_2018_05_22.csv"

inPD = pd.read_csv(inScenes)
inD = gpd.GeoDataFrame(inPD, geometry=[loads(x).buffer(0) for x in inPD.Full_scene_WKT])
inD.newExtent = ''
inD.newExtent2 = ''

agebs = gpd.read_file(inShapes)
newAgebs = pd.read_csv(unProcessedFiles)
#Select columns from agebs that are unprocessed
agebs2 = agebs[agebs.concat_id.isin(newAgebs.concat_id)]
agebsShape = cascaded_union(agebs2.geometry)

for index, vals in inD.iterrows():
    print "Processing %s" % index
    #Clip the current scene with the agebs
    extentShape = vals.geometry.intersection(agebsShape)
    ss = extentShape.bounds
    #Create a bounding WKT from the selectedScene
    if len(ss) > 0:
        curEnv = "POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))" % (ss[0], ss[1],
                                                           ss[0], ss[3],
                                                           ss[2], ss[3],
                                                           ss[2], ss[1],
                                                           ss[0], ss[1])
        #Eliminate the selected shape from the AGEBS shape
        agebsShape = agebsShape.difference(extentShape)                            
        inD = inD.set_value(index, 'newExtent2', curEnv)


inD.to_csv(outputFile)
