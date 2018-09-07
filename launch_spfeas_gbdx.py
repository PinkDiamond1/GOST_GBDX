###################################################################################################
# Launch GBDx Tasks
# Benjamin P. Stewart, May 2018
# Purpose: use the GBDX tasks library to launch various GBDx tasks - this focuses on spfeas
###################################################################################################
import sys, os, inspect
import pandas as pd
import geopandas as gpd

from gbdxtools import Interface
from shapely.wkt import loads

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST_GBDx")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc

#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else qill f%*$&ing tell you that)
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

#Run spfeas on each feature in the input image, depending on its intersection with the defined image boundaries
#Get image boundaries
inShape = r"Q:\AFRICA\COD\Projects\DRC_Census_Franck\Kinshasai_WM\Kinshasai_1000\Kinshasai_GRID_for_SPFEAS.shp"
inFile = r"Q:\AFRICA\COD\Projects\DRC_Census_Franck\Kinshasai_WM\Kinshasai\Kinshasai_GRID_imagery_search_selected.csv"
inD = pd.read_csv(inFile)
inD = inD.loc[inD.Selected == 1]
inD = inD.reset_index()
inDGeom = [loads(x) for x in inD.Full_scene_WKT]
inDG = gpd.GeoDataFrame(inD.drop(['Full_scene_WKT'], axis=1), geometry=inDGeom)

inS = gpd.read_file(inShape)
inS.Id = inS.index
inS = inS.to_crs({'init': u'epsg:4326'})
catIDtoProcess = ['1030010050523000','1030010050523000','102001004C6A7F00','10200100432B8600','10200100432B8600','102001004C6A7F00','10200100432B8600','1020010052D3D000','1020010052D3D000','102001004C6A7F00','10200100432B8600','1020010052D3D000','10200100432B8600','102001004C6A7F00','1020010052D3D000','102001004C6A7F00','10200100432B8600','102001004C6A7F00','10200100432B8600','102001004C6A7F00']
shpToProcess = [21,2,0,11,13,14,15,16,17,19,22,24,26,28,31,33,5,6,8,9]
inS = inS.loc[[x in shpToProcess for x in inS.Id]]
allIDs = []
for idx, row in inS.iterrows():
    curImage = inDG.loc[inDG.intersects(row.geometry)]
    for imageIdx, imageRow in curImage.iterrows():
        catID = imageRow.ID
        curSensor = imageRow.Sensor
        outFolder = "bps/cityAnalysis/Kinshasa/%s_%s" % (row.Id, catID)
        curRasterFolder = os.path.join('s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/%s/clippedRaster' % outFolder)
        x = curTasks.createWorkflow(catID, str(row.geometry), curSensor, outFolder,
                        runCarFinder = 0, runSpfeas = 1, runLC = 0, downloadImages = 0,
                        aopPan=False, aopDra=False, aopAcomp=True, aopBands='AUTO',
                        spfeasParams={"triggers":'orb seg dmp fourier gabor grad hog lac mean pantex saliency sfs ndvi', 
                            "scales":'8 16 32', "block":'8', "gdal_cache":'1024', "section_size":'2000', "n_jobs":'1'}, 
                            inRaster = curRasterFolder)
    allIDs.append(x.execute())

xx = gbdxUrl.monitorWorkflows(sleepTime=300, focalWorkflows=allIDs)    




'''
gbdxUrl.descWorkflow('4940389794301909082')

# Run spfeas and car counting on defined imagery
inS = gpd.read_file(r"Q:\WORKINGPROJECTS\Indonesia_GBDx\BalikPapan_AOI.shp")
#inImages = [['104001003E70C400',"WORLDVIEW03_VNIR"],['103001007E75D600','WORLDVIEW02']]
for catID in ["104001003E70C400"]:#, "10400100315E4200"]:
    inS3Folder = r"s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/bps/cityAnalysis/Balikpapan/%s/clippedRaster" % catID
    outFolder = "bps/cityAnalysis/Balikpapan/%s" % catID
    sensor = "WORLDVIEW03_VNIR"
    x = curTasks.createWorkflow(catID, str(inS.geometry[0]), sensor, outFolder,
                    runCarFinder = 0, runSpfeas = 0, spfeasLoop = 0, downloadImages = 0, runLC=1,
                    aopPan=False, aopDra=False, aopAcomp=False, aopBands='PAN',
                    spfeasParams={"triggers":'orb seg dmp fourier gabor grad hog lac mean pantex saliency sfs ndvi', 
                        "scales":'8 16 32', "block":'8', "gdal_cache":'1024', "section_size":'2000', "n_jobs":'1'}, 
                        inRaster = inS3Folder)
    id1 = x.execute()

xx = gbdxUrl.monitorWorkflows(sleepTime=300)
#
for imageSets in inImages:
    catID = imageSets[0]
    sensor = imageSets[1]
    outFolder = "bps/cityAnalysis/Balikpapan/%s" % catID
    x = curTasks.createWorkflow(catID, str(inS.geometry[0]), sensor, outFolder,
                    runCarFinder = 1, runSpfeas = 1, spfeasLoop = 0, downloadImages = 1, runLC=1,
                    aopPan=True, aopDra=False, aopAcomp=True, aopBands='Auto',
                    spfeasParams={"triggers":'orb seg dmp fourier gabor grad hog lac mean pantex saliency sfs ndvi', 
                        "scales":'8 16 32', "block":'8', "gdal_cache":'1024', "section_size":'5000', "n_jobs":'1'})
    id1 = x.execute()

#x.tasks[0].generate_task_workflow_json()

xx = gbdxUrl.monitorWorkflows(sleepTime=60)
xx['FAILED'][id1]
for k in xx['FAILED'].keys():
    print xx['FAILED'][k]['tasks'][0]['note']

for xIdx in xx['FAILED'][id1]['tasks'][0]['inputs']:
    print "%s - %s" % (xIdx['name'], xIdx['value'])


#Run spfeas on saved clippedRaster
inS = gpd.read_file(r"Q:\AFRICA\COD\Projects\DRC_Census_Franck\Lubumbashi_WM\Lubumbashi_WM.shp")
inS = inS.to_crs({'init': u'epsg:4326'})


#x.tasks[0].generate_task_workflow_json()

xx = gbdxUrl.monitorWorkflows(sleepTime=60)


id2 = '4934643809165411250'
xx['FAILED'][id2]

print xx
#Order images
inputImages = ['1030010066932800','103001005AD57300','10300100414D2E00','10504100104D5D00']
for img in inputImages:
    curID = gbdx.ordering.order(img)
    print gbdx.ordering.status(curID)

#Run LULC on image
inShpFile = r"Q:\AFRICA\LSO\LCVR\GBDx_LULC_Dunstan\Sample_wards.shp"
inS = gpd.read_file(inShpFile)
inS = inS.to_crs({'init': u'epsg:4326'})
for catID in inputImages:
    outFolder = "bps/Lesotho/%s" % catID
    x = curTasks.createWorkflow(catID, str(inS.geometry.unary_union), '', outFolder,
                    runCarFinder = 0, runSpfeas = 0, runLC = 1, downloadImages = 0,
                    aopPan=False, aopDra=False, aopAcomp=True, aopBands='MS')
    x.execute()
#xx = gbdxUrl.monitorWorkflows(sleepTime=300)
'''

'''
#get information from shapefile
inShpFile = "Q:\WORKINGPROJECTS\Indonesia_GBDx\Balikpapan_selected.csv"
inImages = pd.read_csv(inShpFile)
#Set column names properly
#inImages['useful_area_WKT'] = [str(x) for x in inImages.geometry]

imageryFiles = r"H:\SriLanka\IMAGE_CSV\Final_Scene_List_LKA.csv"
inImages = pd.read_csv(imageryFiles)
allTasks = []
outFolder = "H:/SriLanka/SpFeasRes"
alreadyProcessed = os.listdir(outFolder)
alreadyProcessed = ['1030010021854200', '1030010030470700', '1030010041200300', '1030010041531900', '1030010041707200', '10300100417B4100', '1030010044968600', '1030010045079300', '1030010045946100', '1030010046557800', '1030010046655700', '1030010047664E00', '1030010047872800', '1030010048190900', '1030010051599300', '103001005215A400', '10300100533E0000', '1030010055568000', '1030010056469000', '1030010060471400', '1030010061415200', '1030010064612600', '1040010004487900', '1040010004857900', '1040010004957900', '1040010006485300', '1040010007959E00', '1040010008266400', '1040010008379400', '1040010009803800', '1040010013738600', '1040010014508100', '1040010018306200']
currentlyProcessing = []

#Get list of processing workflows
xx = gbdxUrl.listWorkflows()
for x in xx:
    descX = gbdxUrl.descWorkflow(x)
    for idx in [0,1,2]:
        try:
            currentlyProcessing.append(descX['tasks'][0]['outputs'][0]['persistLocation'].split("/")[3])
        except:
            pass

currentlyProcessing = [x for x in set(currentlyProcessing)]

failedIDs = []
orderedIDs = []

for tID in inImages.iterrows():
    if not tID[1].ID in alreadyProcessed and not tID[1].ID in currentlyProcessing:
        x = curTasks.createWorkflow(tID[1].ID, tID[1].useful_area_WKT, tID[1].Sensor, "bps/IDN_Balikpapan/%s" % tID[1].ID,
                runCarFinder = 0, runSpfeas = 0, downloadImages = 1,
                aopPan=False, aopDra=False, aopAcomp=False, aopBands='PAN',
                spfeasParams={"triggers":'orb seg dmp fourier gabor grad hog lac mean pantex saliency sfs', 
                    "scales":'8 16 32', "block":'4'})
        if not x is None:
            x.execute()
        else:
            failedIDs.append(tID[0])

xx = gbdxUrl.monitorWorkflows(sleepTime=300)    
for x in xx['FAILED']:
    print x
for x in xx['SUCCEEDED']:
    print x['tasks'][0]['outputs'][0]['persistLocation'].split("/")[3]
'''
