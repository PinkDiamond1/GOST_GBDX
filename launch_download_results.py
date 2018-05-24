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

#Get list of scenes for follow-up
'''
scenesList = r"H:\MEX_Pov\IMAGE_CSV\followUp_scenes.txt"
scenes = []
with open(scenesList, 'r') as sceneR:
    for l in sceneR:
        scenes.append(l.replace("\n", ""))
print scenes

#Which of the followUp scenes are already downloaded
for spFolder in os.listdir(r"H:\MEX_Pov\SpfeasRes"):
    if spFolder in scenes:
        print ("%s is already processed" % spFolder)
xx = gbdxUrl.monitorWorkflows()
for x in xx['SUCCEEDED']:
    print x['tasks'][0]['outputs'][0]['persistLocation']
'''
toDownload = ['10300100533E0000','1040010007959E00','1030010051599300','1030010045946100','1030010041200300','1030010060471400','1030010044968600','1030010056469000','1030010047872800','1030010048190900','1030010045079300','1030010041707200','1040010008379400','1040010013738600']

initials = 'bps'
location = r'MexicoPoverty_Training'
#imageryFiles = r"H:\SriLanka\IMAGE_CSV\Final_Scene_List_LKA.csv"
#inImages = pd.read_csv(imageryFiles)
outputFolder = r"H:\MEX_Pov\SpfeasRes_FollowUp"
s3File = "C:/Temp/s3Contents.txt"

alreadyProcessed = os.listdir(outputFolder)
print alreadyProcessed
xx = gbdxUrl.listS3Contents("s3://gbd-customer-data/%s/%s/%s/" % (gbdxUrl.prefix, initials, location), outFile=s3File)
gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")

processedScenes = []
with open(s3File) as inFile:
    for f in inFile:
        splitFolder = f.split(" ")
        imageName = splitFolder[-1].replace("\n", "")
        if imageName.replace("/", "") in toDownload:
            try:
                #Each line in this folder represents a processed image
                processedScenes.append(imageName.replace("/", ""))
                curFolder = "s3://gbd-customer-data/%s/%s/%s/%s" % (gbdxUrl.prefix, initials, location, imageName)
                imageFolder = "%s%s/" % (curFolder, "clippedRaster")
                resultsFolder = "%s%s/" % (curFolder, "spfeas")
                #Download spfeas Results
                curOut = os.path.join(outputFolder, imageName)
                try:
                    os.mkdir(curOut)
                    xx = gbdxUrl.downloadS3Contents(resultsFolder, curOut, recursive=True)
                    gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")
                except:
                    print "%s already exists" % curOut
            except:
                print "Error processing %s" % f