###################################################################################################
# Download Raw Imagery
# Benjamin P. Stewart, May 2018
# Purpose: Use the AWS CLI for downloading S3 results 
#   - see the curFolder, imageFolder, and resultsFolder for specific folders to be downloaded
###################################################################################################

import sys, os, inspect
import geojson

import geopandas as gpd

from gbdxtools import Interface
from gbdxtools import CatalogImage
from shapely.geometry import shape

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST\GBDx")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc

#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else qill f%*$&ing tell you that)
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)
images = ['1040010037BEE200', '10400100363A2100']
images = ['10400100280CF300', '103001001DAC9700'] #Konna Mali
focalArea = [[[-3.888849020004272,14.934797068255058],[-3.8874703645706172,14.934797068255058],[-3.8874703645706172,14.935719677045839],[-3.888849020004272,14.935719677045839],[-3.888849020004272,14.934797068255058]]]
outFile = "Q:/WORKINGPROJECTS/ImageryDownload/Konna_uint8_RGB_%s.tif"
inPoly = geojson.Polygon(focalArea)
curWKT = shape(inPoly).wkt
'''
inputShapefile = r"Q:\WORKINGPROJECTS\ImageryDownload\HCMC Admin Unit UTM WGS84\Thu Duc District\ThuDuc_District_Dissolved.shp"
inShp = gpd.read_file(inputShapefile)
curWKT = str(inShp.geometry[0])
'''

for cat_id in images:
    print "Processing %s" % cat_id
x = CatalogImage(cat_id,pansharpen=True)
testArea = x.aoi(wkt=curWKT)
testArea.geotiff(path=outFile % cat_id, dtype='uint8', bands=[1,2,4])

#testArea.geotiff(path="C:/Temp/HCMC_default.tif")
#testArea.geotiff(path="C:/Temp/HCMC_uint8.tif",dtype='uint8')

