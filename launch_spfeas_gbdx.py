###################################################################################################
# Launch GBDx Tasks
# Benjamin P. Stewart, May 2018
# Purpose: use the GBDX tasks library to launch various GBDx tasks - this focuses on spfeas
###################################################################################################
import sys, os, inspect
import pandas as pd

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
gbdxUrl = gbdxURL_misc.gbdxURL(inputParams)

#Run spfeas on input imagery
imageryFiles = r"Q:\WORKINGPROJECTS\LKA_Poverty\CharlesRocks_csv\Final_Scene_List_LKA.csv"
inImages = pd.read_csv(imageryFiles)

allTasks = []
outFolder = "D:/SriLanka/SpFeasRes"
alreadyProcessed = os.listdir(outFolder)

failedIDs = []
orderedIDs = []

for tID in inImages.iterrows():
#    if not tID[1].ID in alreadyProcessed:
    x = curTasks.createWorkflow(tID[1].ID, tID[1].useful_area_WKT, tID[1].Sensor, "bps/MexicoPoverty_Training/%s" % tID[1].ID,
                    runCarFinder = 0, runSpfeas = 1, downloadImages = 1,
                    aopPan=False, aopDra=False, aopAcomp=True, aopBands='MS',
                    spfeasParams={"triggers":'orb seg ndvi dmp fourier gabor grad hog lac mean pantex saliency sfs', 
                        "scales":'8 16 32', "block":'4'})
    try:
        x.execute()
    except:
        failedIDs.append(tID[1].ID)
        curOrder = gbdx.ordering.order(tID[1].ID)
        orderedIDs.append(curOrder)
        print gbdx.ordering.status(curOrder)

xx = gbdxUrl.monitorWorkflows(sleepTime=120)    
for x in xx['FAILED']:
    print x
