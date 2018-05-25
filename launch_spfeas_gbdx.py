###################################################################################################
# Launch GBDx Tasks
# Benjamin P. Stewart, May 2018
# Purpose: use the GBDX tasks library to launch various GBDx tasks - this focuses on spfeas
###################################################################################################
import sys, os, inspect
import pandas as pd
import geopandas as gpd

from gbdxtools import Interface

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

#Run spfeas on input imagery
#get information from shapefile
inShpFile = "Q:/WORKINGPROJECTS/Mexico_Poverty/CSV for analysis/Complete list of scene IDs/scenes_forUnprocessed_agebs.shp"
inImages = gpd.read_file(inShpFile)
#Set column names properly
inImages['useful_area_WKT'] = [str(x) for x in inImages.geometry]

'''
imageryFiles = r"H:\SriLanka\IMAGE_CSV\Final_Scene_List_LKA.csv"
inImages = pd.read_csv(imageryFiles)
'''
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
    if not tID[1].ID in alreadyProcessed and not tID[1].ID in currentlyProcessing and not tID[1].geometry is None:
        x = curTasks.createWorkflow(tID[1].ID, tID[1].useful_area_WKT, tID[1].Sensor, "bps/MexicoPoverty_Training_FollowUp/%s" % tID[1].ID,
                runCarFinder = 0, runSpfeas = 1, downloadImages = 1,
                aopPan=False, aopDra=False, aopAcomp=True, aopBands='MS',
                spfeasParams={"triggers":'orb seg ndvi dmp fourier gabor grad hog lac mean pantex saliency sfs', 
                    "scales":'8 16 32', "block":'4'})
        if not x is None:
            x.execute()
        else:
            failedIDs.append(tID[0])

'''
        try:
        except:
            failedIDs.append(tID[1].ID)
            curOrder = gbdx.ordering.order(tID[1].ID)
            orderedIDs.append(curOrder)
            print gbdx.ordering.status(curOrder)
'''

xx = gbdxUrl.monitorWorkflows(sleepTime=300)    
for x in xx['FAILED']:
    print x
for x in xx['SUCCEEDED']:
    print x['tasks'][0]['outputs'][0]['persistLocation'].split("/")[3]
    
'''
10300100533E0000,1040010007959E00,1030010051599300,1030010045946100,1030010041200300,1030010060471400,1030010044968600,1030010056469000,1030010047872800,1030010048190900,1030010045079300,1030010041707200,1040010008379400,1040010013738600,
'''