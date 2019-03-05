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

from shapely.geometry import box

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST_GBDx")
sys.path.insert(0, r"C:\Code\Github\GOST_GBDX")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc

#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else qill f%*$&ing tell you that)
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

allRes = []
for x in ['1040010036B51000','10400100361EC300','10500100133D8500','103001008951A400','104001003620B600','104001003620F800','1040010036917C00','1040010036B51000']:
    xx = CatalogImage(x)
    b = box(xx.bounds[0], xx.bounds[1], xx.bounds[2], xx.bounds[3])
    allRes.append([x, xx.metadata['image']['sensorName'], b])
    #print("%s - %s" % (x, xx.metadata['image']['sensorName']))

final = pd.DataFrame(allRes, columns=['catID','type','geometry'])
final.to_csv("C:/Temp/addisImageInfo.csv")

'''
1040010036917C00, dmp, seg
1040010036B51000, dmp, seg
104001003620B600, seg 
104001003620F800, dmp, seg, pantex
'''