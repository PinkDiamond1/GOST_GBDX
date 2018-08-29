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
inKML = r"D:\Jamaica\PortlandCottage.kml"
inSHP = r"D:\Jamaica\pCottage.shp"
outFolder = r"D:\Jamaica\PortlandCottage\%s"
inImages = ['103001002E6E6A00','104001000F642500','1040010010219200',
            '1040010010076B00','1040010017A81B00','10400100367D2200','10400100382F4900']
#get WKT from KML
ds = ogr.Open(inKML)
for lyr in ds:
    for feat in lyr:
        geom = feat.GetGeometryRef()
geom.CloseRings()
curWKT = geom.ExportToIsoWkt()
'''
#get WKT from shapefile
inD = gpd.read_file(inSHP)
'''


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
        res = curTasks.downloadImage(catID, curFolder, curWKT=loads(curWKT), output="INDICES")  
        print (res)
    else:
        print "Ordering %s " % catID

