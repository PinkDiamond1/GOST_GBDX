###################################################################################################
# Search for imagery on GBDx 
# Benjamin P. Stewart, May 2018
# Purpose: Use script developed by Charles Fox to search for imagery within polygons
#   https://notebooks.geobigdata.io/juno/notebook/notebooks/CharlesRocks_4.ipynb
###################################################################################################
import sys, os, inspect, json
import geojson

import geopandas as gpd

from gbdxtools import Interface
from shapely.geometry import shape

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

#manualCurFolder = r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST\GBDx"
#sys.path.insert(0, manualCurFolder)

from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc
from GOST_GBDx_Tools import imagery_search

#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else will f%*$&ing tell you that)
#   You can find the folder by opening Python and entering os.path.expanduser('~')
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

'''
#Search for imagery within defined geojson shape
curShape = [[[-3.9216041564941406,14.924881365256299],[-3.873538970947265,14.924881365256299],[-3.873538970947265,14.957721167065205],[-3.9216041564941406,14.957721167065205],[-3.9216041564941406,14.924881365256299]]]
inPoly = geojson.Polygon(curShape)
curWKT = shape(inPoly)
curRes = imagery_search.searchForImages(gbdx, curWKT, "C:/Temp", "Konna", cutoff_date='1-Jan-08', optimal_date='01-May-18')
curRes.to_csv("C:/Temp/Konna_City_Mali.csv")
               
'''
### Search for imagery within defined shapefile
inShape = r"Q:\WORKINGPROJECTS\Mexico_Poverty\agebs\urban_agebs_Buffer_200m_Diss.shp"
sceneFolder = r"Q:\WORKINGPROJECTS\ImageryDownload\HCMC Admin Unit UTM WGS84"
outFolder = os.path.dirname(inShape)
inD = gpd.read_file(inShape)
if not inD.crs == {'init': u'epsg:4326'}:
    inD = inD.to_crs({'init': 'epsg:4326'})
cnt = 0
allRes = []
nrows = inD.shape[0]

for shp in inD.iterrows():      
    aoi = shp[1]['geometry']
    outputFile = os.path.join(sceneFolder, "%s_sceneList.csv" % cnt)
    if not os.path.exists(outputFile):
        try:
            curRes = imagery_search.searchForImages(gbdx, aoi, sceneFolder, str(cnt), cutoff_date='1-Jan-17', optimal_date='01-May-18')
            curRes['shpID'] = cnt
            curRes.to_csv(outputFile)
        except:
            print "Something errored with %s" % cnt
    else:
        curRes = pd.read_csv(outputFile)
    if cnt == 0:
        finalRes = curRes
    else:
        finalRes = pd.concat([finalRes, curRes], axis=0)
    print "Processed %s of %s: %s images" % (cnt, nrows, curRes.shape[0])
    cnt = cnt + 1

finalRes.to_csv(inShape.replace(".shp", "_imagerySearch.csv"))
