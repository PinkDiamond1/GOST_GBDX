###################################################################################################
# Download Raw Imagery
# Benjamin P. Stewart, May 2018
# Purpose: Use the AWS CLI for downloading S3 results 
#   - see the curFolder, imageFolder, and resultsFolder for specific folders to be downloaded
###################################################################################################

import sys, os, inspect, logging
import geojson

import geopandas as gpd

from gbdxtools import Interface
from gbdxtools import ordering
from gbdxtools import CatalogImage
from shapely.geometry import shape
from shapely.wkt import loads

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST_GBDx")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc
from gbdxtools import CatalogImage
           
#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else qill f%*$&ing tell you that)
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

    
#There are two basic options for downloading Imagery - tasks and CatalogImage
#   Tasks: Run AOP strip processing and Clip raster to mount new imagery to S3
#   CatalogImage: Creates reference to image on IDAHO that can then be written locally
#
#   Use tasks for larger imagery and CatalogImage for smaller samples

#Download within a kml
from osgeo import ogr
from shapely.wkt import loads
'''
inKML = r"Q:\WORKINGPROJECTS\ImageryDownload\Jamaica\PortlandCottage.kml"
outFolder = r"Q:\WORKINGPROJECTS\ImageryDownload\Jamaica\PortlandCottage\%s"
inImages = ['103001000421D700']
'''
inKML = r"Q:\AFRICA\COD\Projects\NDSV_Urbanization\AOI_Kinshasa.kml"
outFolder = r"Q:\AFRICA\COD\Projects\NDSV_Urbanization\AOI_Kinshasa\%s"
inImages = ['103001007FA97400','1030010051656900','103001002F4EE100','1030010011D73D00','1030010006789E00','10100100047CCE00']
curIndex = "NDSV" #"INDICES"

#get WKT from KML
ds = ogr.Open(inKML)
for lyr in ds: 
    for feat in lyr:
        geom = feat.GetGeometryRef()

geom.CloseRings()
curWKT = geom.ExportToIsoWkt()

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',level=logging.INFO)

for catID in inImages:
    curFolder = outFolder % catID
    try:
        os.mkdir(curFolder)
    except:
        pass
    #Check on the existence of the current images in two ways
    try:
        #Check ot see if the image successfully pulls up a CatalogImage, if it does, proceed
        img = CatalogImage(catID)
        curStatus = 'exists'
    except:
        #Else, order it
        imgStatus = gbdx.ordering.order(catID)
        curStatus = gbdx.ordering.status(imgStatus)[0]['location']
    if curStatus != 'not_delivered':
        print "Processing %s" % catID
        res = curTasks.downloadImage(catID, curFolder, curWKT=loads(curWKT), output=curIndex)  
        print (res)
    else:
        print "Ordering %s " % catID
