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
from osgeo import ogr

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

manualCurFolder = r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST_GBDx"
sys.path.insert(0, manualCurFolder)

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

### Search for imagery within defined shapefile
inShape = r"Q:/WORKINGPROJECTS/ImageryDownload/Mali_Keith/Konna_Koanna_1kmBuffer.shp"
inD = gpd.read_file(inShape)
if not inD.crs == {'init': u'epsg:4326'}:
    inD = inD.to_crs({'init': 'epsg:4326'})
cnt = 0
allRes = []
nrows = inD.shape[0]
'''
inKML = r"Q:\WORKINGPROJECTS\ImageryDownload\Mali_Keith\Konna-Koana.kml"
#get WKT from KML
ds = ogr.Open(inKML)
for lyr in ds: 
    for feat in lyr:
        geom = feat.GetGeometryRef()
        print geom
geom.CloseRings()
curWKT = geom.ExportToIsoWkt()
'''

curRes = imagery_search.searchForImages(gbdx, inD.geometry.unary_union, os.path.dirname(inKML), 
                    str(cnt), cutoff_date='1-Jan-12', optimal_date='01-Sep-18')

curRes.to_csv(inShape.replace(".shp", "_imagerySearch.csv"))


'''
#Search for imagery within defined geojson shape
curShape = [[[-5.55908203125,16.762467717941604],[-5.6689453125,15.559544421458103],[-10.30517578125,15.47485740268724],[-11.44775390625,15.686509572551435],[-11.689453125,15.496032414238634],[-12.216796875,14.647368383896632],[-12.01904296875,13.603278132528756],[-11.689453125,13.047372256948787],[-11.62353515625,12.08229583736359],[-10.78857421875,11.996338401936226],[-9.2724609375,12.254127737657381],[-8.37158203125,10.31491928581316],[-7.55859375,10.228437266155943],[-6.2841796875,10.206813072484595],[-5.515136718749999,10.422988388338242],[-5.185546875,11.480024648555816],[-4.74609375,12.146745814539685],[-4.1748046875,12.91890657418042],[-2.96630859375,13.603278132528756],[-1.86767578125,14.28567730018259],[-0.98876953125,14.923554399044052],[-0.1318359375,14.966013251567164],[1.47216796875,15.284185114076433],[-0.10986328125,17.11979250078707],[-2.43896484375,17.20376982191752],[-5.55908203125,16.762467717941604]]]
inPoly = geojson.Polygon(curShape)
curWKT = shape(inPoly)
curRes = imagery_search.searchForImages(gbdx, curWKT, "C:/Temp", "Balikpanana", cutoff_date='1-Jan-15', optimal_date='17-July-18',
                    cutoff_cloud_cover = 100, cutoff_nadir = 90)
curRes.to_csv("C:/Temp/Balikpapan.csv")

#Search for imagery within KML file



### Search for imagery within defined shapefile
inShape = r"Q:\WORKINGPROJECTS\DRC_Road_To_Konna\Konna-Koana_1km.shp"
sceneFolder = r"Q:\WORKINGPROJECTS\DRC_Road_To_Konna"
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




#Search for imagery within defined geojson shape
curShape = [[[-5.55908203125,16.762467717941604],[-5.6689453125,15.559544421458103],[-10.30517578125,15.47485740268724],[-11.44775390625,15.686509572551435],[-11.689453125,15.496032414238634],[-12.216796875,14.647368383896632],[-12.01904296875,13.603278132528756],[-11.689453125,13.047372256948787],[-11.62353515625,12.08229583736359],[-10.78857421875,11.996338401936226],[-9.2724609375,12.254127737657381],[-8.37158203125,10.31491928581316],[-7.55859375,10.228437266155943],[-6.2841796875,10.206813072484595],[-5.515136718749999,10.422988388338242],[-5.185546875,11.480024648555816],[-4.74609375,12.146745814539685],[-4.1748046875,12.91890657418042],[-2.96630859375,13.603278132528756],[-1.86767578125,14.28567730018259],[-0.98876953125,14.923554399044052],[-0.1318359375,14.966013251567164],[1.47216796875,15.284185114076433],[-0.10986328125,17.11979250078707],[-2.43896484375,17.20376982191752],[-5.55908203125,16.762467717941604]]]
inPoly = geojson.Polygon(curShape)
curWKT = shape(inPoly)
curRes = imagery_search.searchForImages(gbdx, curWKT, "C:/Temp", "Balikpanana", cutoff_date='1-Jan-00', optimal_date='05-June-18',
                    cutoff_cloud_cover = 100, cutoff_nadir = 90)
curRes.to_csv("C:/Temp/Balikpapan.csv")
               
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

'''