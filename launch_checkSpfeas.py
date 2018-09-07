###################################################################################################
# Download results from S3
# Benjamin P. Stewart, May 2018
# Purpose: Use the AWS CLI for downloading S3 results 
#   - see the curFolder, imageFolder, and resultsFolder for specific folders to be downloaded
###################################################################################################
import sys, os, inspect, logging, yaml
import pandas as pd

from gbdxtools import Interface

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

sys.path.insert(0, r"C:\Users\lordBen\Documents\GitHub\GOST_GBDX")
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc


#In order for the interface to be properly authenticated, follow instructions here:
#   http://gbdxtools.readthedocs.io/en/latest/user_guide.html
#   For Ben, the .gbdx-config file belongs in C:\Users\WB411133 (CAUSE no one else qill f%*$&ing tell you that)
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx, wbgComp=False)

initials = 'bps'
location = r'cityAnalysis/Kinshasa'
outputFolder = r"C:/Temp/Kinshasa/"
s3File = "C:/Temp/s3Contents.txt"
spFile = "C:/Temp/s3spContents.txt"

xx = gbdxUrl.listS3Contents("s3://gbd-customer-data/%s/%s/%s/" % (gbdxUrl.prefix, initials, location), outFile=s3File)
gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")

processedScenes = []
logging.basicConfig(level=logging.INFO)
allRes = []
with open(s3File) as inFile:
    for f in inFile:
        splitFolder = f.split(" ")
        imageName = splitFolder[-1].replace("\n", "")
        curOut = os.path.join(outputFolder, imageName)
        try:
            curFolder = "s3://gbd-customer-data/%s/%s/%s/%s" % (gbdxUrl.prefix, initials, location, imageName)
            resultsFolder = "%s%s/" % (curFolder, "spfeas")
            #download text files in spfeas folder
            try:
                os.mkdir(curOut)
            except:
                pass
            try:
                xx = gbdxUrl.listS3Contents(resultsFolder, outFile=spFile)
                gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")
                with open(spFile) as spRes:
                    for fsp in spRes:
                        splitFolder = fsp.split(" ")
                        cFile = splitFolder[-1].replace("\n", "")
                        print cFile
                        if cFile[-4:] == 'yaml':
                            yamlFile = cFile                
                xx = gbdxUrl.downloadS3Contents("%s%s" % (resultsFolder, yamlFile), curOut)
                gbdxUrl.executeAWS_file(xx, "C:/Temp/s3Execution.bat")
                #Read the spfeas YAML file
                with open(os.path.join(curOut, yamlFile), 'r') as yamlContents:
                    yamlRes = yaml.load(yamlContents)
                allRes.append([imageName, yamlRes['ALL_FINISHED']])
            except:
                allRes.append([imageName, "NOT STARTED"])
                logging.info("%s already exists" % curOut)
        except:
            logging.info("spfeas did not process for " % f)

allPD = pd.DataFrame(allRes, columns = ['IMAGE', 'SPFEAS_Status'])
allPD['shpID'], allPD['catID'] = allPD.IMAGE.str.split("_").str
allPD.to_csv(os.path.join(outputFolder, "spfeas_Res.csv"))