###################################################################################################
# Download results from S3
# Benjamin P. Stewart, May 2018
# Purpose: Use the AWS CLI for downloading S3 results 
#   - see the curFolder, imageFolder, and resultsFolder for specific folders to be downloaded
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
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

imageryFiles = r"Q:\WORKINGPROJECTS\LKA_Poverty\CharlesRocks_csv\Final_Scene_List_LKA.csv"
outputFolder = "D:/SriLanka/RawImagery"
inImages = pd.read_csv(imageryFiles)
s3File = "C:/Temp/s3Contents.txt"

xx = gbdxUrl.listS3Contents("s3://gbd-customer-data/%s/bps/LKA/" % gbdxUrl.prefix, outFile=s3File)
gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")

processedScenes = []
with open(s3File) as inFile:
    for f in inFile:
        #Each line in this folder represents a processed image
        splitFolder = f.split(" ")
        imageName = splitFolder[-1].replace("\n", "")
        processedScenes.append(imageName.replace("/", ""))
        curFolder = "s3://gbd-customer-data/%s/bps/LKA/%s" % (gbdxUrl.prefix, imageName)
        imageFolder = "%s%s/" % (curFolder, "clippedRaster")
        resultsFolder = "%s%s/" % (curFolder, "spfeas")
        #Download spfeas Results
        curOut = os.path.join(outputFolder, imageName)
        try:
            os.mkdir(curOut)
            xx = gbdxUrl.downloadS3Contents(curFolder, curOut, recursive=True)
            gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")
        except:
            pass