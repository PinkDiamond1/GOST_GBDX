################################################################################
# Create some Satellite Art
# Benjamin Stewart, May 2018
# Purpose: Create three panel satellite image art based on locations
################################################################################
import sys, os, inspect, logging
import pandas as pd
import geopandas as gpd

from gbdxtools import Interface
from gbdxtools import CatalogImage
'''
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST\GBDx")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc
'''
SENSOR_BAND_DICT={'ASTER':dict(green=1,red=2,nir=3,midir=4,farir=5),
'ASTER-VNIR':dict(green=1,red=2,nir=3),
'GEOEYE01':dict(blue=1,green=2,red=3,nir=4),
'IKONOS':dict(blue=1,green=2,red=3,nir=4),
'QUICKBIRD02':dict(blue=1,green=2,red=3,nir=4),
'WORLDVIEW02':dict(cblue=1,blue=2,green=3,yellow=4,red=5,rededge=6,nir=7,midir=8),
'WORLDVIEW03':dict(cblue=1,blue=2,green=3,yellow=4,red=5,rededge=6,nir=7,midir=8),
'Planet':dict(blue=1,green=2,red=3,nir=4),
'Landsat-sharp':dict(blue=1,green=2,red=3,nir=4),
'Landsat':dict(blue=1,green=2,red=3,nir=4,midir=5,farir=6,pan=8),
'Landsat8':dict(cblue=1,blue=2,green=3,red=4,nir=5,midir=6,farir=7,cirrus=8,thermal1=9,thermal2=10,pan=8),
'Landsat-thermal':dict(blue=1,green=2,red=3,nir=4,midir=5,farir=7),
'MODISc5':dict(blue=3,green=4,red=1,nir=2,midir=6,farir=7),
'SUPPORTED_VIS':dict(blue=3,green=4,red=1,nir=2,midir=6,farir=7),
'RapidEye':dict(blue=1,green=2,red=3,rededge=4,nir=5),
'Sentinel2':dict(cblue=1,blue=2,green=3,red=4,rededge=5,rededge2=6,rededge3=7,niredge=8,nir=9,wv=10,cirrus=11,midir=12,farir=13),
'Sentinel2-10m':dict(blue=1,green=2,red=3,nir=4),
'Sentinel2-20m':dict(rededge=1,niredge=4,midir=5,farir=6),
'RGB':dict(blue=3,green=2,red=1),
'BGR':dict(blue=1,green=2,red=3),
'pan-sharp57':dict(midir=1,farir=2)}

class satellitePanel(object)
    def __init__(self, gbdx, cat_id, shape, shapeType='wkt')
        x = CatalogImage(cat_id, pansharpen=True, acomp=True)
        sensorBands = SENSOR_BAND_DICT[sensor]
        if shapeType='wkt':
            xx = x.aoi(wkt=shape)
        else:
            raise(BaseException("Forgot to add all the shape types"))
    
class artPanel(object)
    def __init__(self, cat_ids, bboxes, outFile, gbdx):
    ###Variable checking should go here
    
        ##Loop through images, extract, 
        for panelIdx in range(0, len(cat_ids)):
            curCat = cat_ids[panelIdx]
            curBox = boxes[panelIdx]
            curPanel = satellitePanel(curCat, curBox, gbdx)

            
class