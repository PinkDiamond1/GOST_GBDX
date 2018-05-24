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
'''
x = CatalogImage('103001001DAC9700')
x.ipe_metadata['image']['sensorPlatformName']
x.ipe_metadata['image']['acquisitionDate']
'''

#There are two basic options for downloading Imagery - tasks and CatalogImage
#   Tasks: Run AOP strip processing and Clip raster to mount new imagery to S3
#   CatalogImage: Creates reference to image on IDAHO that can then be written locally
#
#   Use tasks for larger imagery and CatalogImage for smaller samples
initials = "bps" #This is used to create the output S3 folder 
location = "Mali_3" #This is used to create the output S3 folder 
###Download imagery using tasks

'''
inputImages = ['10400100280CF300', '103001001DAC9700']
#Get the WKT from the geojson.io
inputGeojson = [[[-3.9216041564941406,14.924881365256299],[-3.873538970947265,14.924881365256299],[-3.873538970947265,14.957721167065205],[-3.9216041564941406,14.957721167065205],[-3.9216041564941406,14.924881365256299]]]
inPoly = geojson.Polygon(inputGeojson)
curWKT = shape(inPoly).wkt

###Get the WKT from a Shapefile
'''
inShp = gpd.read_file(r"Q:\WORKINGPROJECTS\ImageryDownload\HCMC Admin Unit UTM WGS84\HCMC_province.shp")
if not inShp.crs == {'init': u'epsg:4326'}:
    inShp = inShp.to_crs({'init': 'epsg:4326'})

curWKT = str(inShp.geometry[0])

inputImages = ['1040010037BEE200','10400100363A2100','1030010063809D00','1040010027575000','10300100651C4B00']
allTasks = []
for cat_id in inputImages:
    data = gbdx.catalog.get_data_location(cat_id)    
    print data
    x = curTasks.downloadAOP(cat_id, "%s/%s/%s" % (initials, location, cat_id), curWKT,
        aopBands="PAN", aopDra=False, panSharpen=False)
    allTasks.append(x)   
    
for x in allTasks[0]:
    x.execute()
xx = gbdxUrl.monitorWorkflows(sleepTime=120)   
gbdxUrl.descWorkflow("4867245509786686531")


'''
#Download imagery the new, hip way using Catalog Image
outputFolder = r"Q:\AFRICA\LBR\IMAGERY"
inputShapefile = r"Q:\AFRICA\LBR\ADMIN\Greater Monrovia admin boundary\Greater_Monrovia_admin_boundary.shp"
inShp = gpd.read_file(inputShapefile)    
cbbox = [inShp.bounds.minx[0], inShp.bounds.maxy[0], inShp.bounds.maxx[0], inShp.bounds.miny[0]]
for tID in ['1030010051A7CD00', '105041001207E600']:
    curTasks.downloadImage(tID, os.path.join(outputFolder, "%s.tif" % tID), boundingBox=cbbox, panSharpen=False)

#Download Imagery the old fashioned way
allTasks = []
cbbox = "%s,%s,%s,%s" % (inShp.bounds.minx[0], inShp.bounds.maxy[0], inShp.bounds.maxx[0], inShp.bounds.miny[0])
print cbbox

for tID in [['1030010051A7CD00','WORLDVIEW3_VNIR'],['105041001207E600','WORLDVIEW3_VNIR']]: 
    x = curTasks.createWorkflow(tID[0], cbbox, str(inShp.geometry[0]), tID[1], "bps/Monrovia/%s" % tID[0],
                    runCarFinder = 0, runSpfeas = 0, downloadImages = 1,
                    aopPan=True, aopDra=True, aopAcomp=True, aopBands='MS',
                    spfeasParams={"triggers":'dmp fourier gabor grad hog lac mean ndvi pantex saliency sfs', 
                                  "scales":'8 16 32 64', "block":'4'})
    allTasks.append(x)    

print allTasks

#for t in allTasks:
#    t.execute()
xy = allTasks[0].execute()
print xy

xx = gbdxUrl.monitorWorkflows(sleepTime=300)    
for x in xx['FAILED']:
    print x
'''