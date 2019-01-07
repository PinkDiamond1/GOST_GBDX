###################################################################################################
# Launch GBDx Tasks
# Benjamin P. Stewart, May 2018
# Purpose: use the GBDX tasks library to launch various GBDx tasks - this focuses on spfeas
###################################################################################################
import sys, os, inspect
import pandas as pd
import geopandas as gpd

from gbdxtools import Interface
from gbdxtools import CatalogImage

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

#Run spfeas on saved clippedRaster
inS = gpd.read_file(r"Z:\SPFEAS_Results\Kinshasa\Shohei_Poverty\AOI.shp")
inS = inS.to_crs({'init': u'epsg:4326'})

for catID in ['1030010080070D00','1030010080555B00','103001007FA97400']:#'104001002B65CE00']:#
    inS3Folder = r"s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/bps/cityAnalysis/Lubumbashi_WM/%s/clippedRaster" % catID
    sensor = "WORLDVIEW03_VNIR"
    #Get the intersecting area with the current image
    cImg = CatalogImage(catID)
    b = cImg.bounds
    bGeom = box(b[0], b[1], b[2], b[3])
    inGeom = bGeom.intersection(inS.geometry[0])   
    for cJob in ['saliency','seg','fourier','pantex']:          #'orb','seg','dmp','fourier','gabor','lac','mean','pantex','saliency','sfs','ndvi']:
        outFolder = "bps/cityAnalysis/Kinshasa/Shohei_Poverty/%s/spfeas/%s" % (catID, cJob)
        imageFolder = "bps/cityAnalysis/Kinshasa/Shohei_Poverty/%s/%s" % (catID, "clippedRaster")
        x = curTasks.createWorkflow(catID, str(inGeom.wkt), sensor, outFolder,
                    runCarFinder = 0, runSpfeas = 1, spfeasLoop = 0, downloadImages = 0,
                    aopPan=False, aopDra=False, aopAcomp=False, aopBands='PAN',
                    spfeasParams={"triggers":'%s' % cJob, 
                        "scales":'8 16 32', "block":'8', "gdal_cache":'1024', "section_size":'5000', "n_jobs":'1'}, 
                        inRaster = '')
        id1 = x.execute()

#x.tasks[0].generate_task_workflow_json()

xx = gbdxUrl.monitorWorkflows(sleepTime=60)
xx['FAILED'][id1]

for k in xx['FAILED'].keys():
    print(xx['FAILED'][k]['tasks'][0]['note'])


for xIdx in xx['FAILED'][id1]['tasks'][0]['inputs']:
    print("%s - %s" % (xIdx['name'], xIdx['value']))


'''
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
