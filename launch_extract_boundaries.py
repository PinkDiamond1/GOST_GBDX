###################################################################################################
# Download results from S3
# Benjamin P. Stewart, May 2018
# Purpose: Use the AWS CLI for downloading S3 results 
#   - see the curFolder, imageFolder, and resultsFolder for specific folders to be downloaded
###################################################################################################
import sys, os, inspect
import pandas as pd

from gbdxtools import Interface
from gbdxtools import CatalogImage
from shapely.geometry import box

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST\GBDx")
sys.path.insert(0, r"C:\Code\Github\GOST_GBDX")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc

#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else qill f%*$&ing tell you that)
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx, wbgComp=False)

images = ['1040010036B51000','10400100361EC300','10500100133D8500']
allGeoms = []
for i in images:
    cImg = CatalogImage(i)
    b = cImg.bounds
    bGeom = box(b[0], b[1], b[2], b[3])
    allGeoms.append([i, bGeom.wkt])

