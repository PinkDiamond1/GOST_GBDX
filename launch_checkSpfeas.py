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
def generateSpfeasCSV(initials, location, outputFolder, s3File = "C:/Temp/s3Contents.txt", spFile = "C:/Temp/s3spContents.txt"):
    xx = gbdxUrl.listS3Contents("s3://gbd-customer-data/%s/%s/%s/" % (gbdxUrl.prefix, initials, location), outFile=s3File)
    gbdxUrl.executeAWS_file(xx, "C:/temp/s3Execution.bat")
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
                    gbdxUrl.executeAWS_file(xx, "C:/temp/s3Execution.bat")
                    with open(spFile) as spRes:
                        for fsp in spRes:
                            splitFolder = fsp.split(" ")
                            cFile = splitFolder[-1].replace("\n", "")
                            print(cFile)
                            if cFile[-4:] == 'yaml':
                                yamlFile = cFile                
                    xx = gbdxUrl.downloadS3Contents("%s%s" % (resultsFolder, yamlFile), curOut)
                    gbdxUrl.executeAWS_file(xx, "C:/temp/s3Execution.bat")
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
    #allPD['shpID'], allPD['catID'] = allPD.IMAGE.str.split("_").str
    return allPD

def processCSV(inD, initials, location, outputBase):
    #Loop through rows in input
    for idx, row in inD.iterrows():
        curStatus = row.SPFEAS_Status
        curFolder = "s3://gbd-customer-data/%s/%s/%s/%s" % (gbdxUrl.prefix, initials, location, row.IMAGE)
        spfeasFolder = "%sspfeas" % curFolder       
        imageryFolder = "%sclippedRaster" % curFolder       
        #Create output Folders
        outputFolder = os.path.join(outputBase, row.IMAGE)
        spfeasOut = os.path.join(outputFolder, "spfeas")
        imageOut = os.path.join(outputFolder, "clippedRaster")
        for f in [outputFolder, spfeasOut, imageOut]:
            try:
                os.mkdir(f)
            except:
                pass
        if curStatus == "NOT STARTED":
            #Run spfeas
            FUBAR = "DO THIS LATER"
        elif curStatus == "yes":
            #Download spfeas folder
            xx = gbdxUrl.downloadS3Contents(spfeasFolder, spfeasOut, recursive=True)
            gbdxUrl.executeAWS_file(xx, "C:/temp/s3Execution.bat")      
        elif curStatus == "no":
            #Download imagery folder
            xx = gbdxUrl.downloadS3Contents(imageryFolder, imageOut, recursive=True)
            gbdxUrl.executeAWS_file(xx, "C:/temp/s3Execution.bat")                    
    
if __name__ == "__main__":
gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx, wbgComp=False)

initials = 'bps'
location = r's3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/cityAnalysis/Kinshasa/Shohei_Poverty/'
outputFolder = r"C:/temp/Kinshasa/"
spFile = os.path.join(outputFolder, "s3Contents_all.txt")
logging.basicConfig(level=logging.INFO)

# New version
if not os.path.exists(spFile):
    xx = gbdxUrl.listS3Contents(location, outFile=spFile, recursive=True)    
    gbdxUrl.executeAWS_file(xx, "C:/temp/s3Contents.bat")
    
#Open the outputFile and find all the yaml files
allYaml = []
with open(spFile, 'r') as inFile:
    for line in inFile:
        splitFolder = line.split(" ")
        cFile = splitFolder[-1].replace("\n", "")
        if cFile[-4:] == 'yaml':
            allYaml.append(cFile)

#Download the yaml files
for cYaml in allYaml:
    outYaml = os.path.join(outputFolder, os.path.basename(cYaml))
    cYaml = os.path.join("s3://gbd-customer-data/", cYaml)
    #if not os.path.exists(outYaml):
    xx = gbdxUrl.downloadS3Contents(cYaml, outYaml, recursive=False)
    gbdxUrl.executeAWS_file(xx, "C:/temp/s3Execution.bat")    

#Process the yaml files
curData = {}
for cYaml in allYaml:
    nameSplit = cYaml.split("/")
    if not len(nameSplit[-3]) > 8:
        outYaml = os.path.join(outputFolder, os.path.basename(cYaml))
        try:
            with open(outYaml, 'r') as yamlContents:
                yamlRes = yaml.load(yamlContents)
            #curData.append([nameSplit[-5], nameSplit[-3], yamlRes['ALL_FINISHED']])
            curRes = {nameSplit[-3]:yamlRes['ALL_FINISHED']}
            try:
                curData[nameSplit[-5]] = {**curData[nameSplit[-5]], **curRes}
            except:
                curData[nameSplit[-5]] = curRes
        except:
            print("Could not process %s" % cYaml)

finalPD = pd.DataFrame(curData)#, columns=["CAT_ID", "spfeas", "Finished"])
finalPD.to_csv("C:/temp/spfeasCheck.csv")    
        
    '''
    # old version for straightforward spfeas monitoring
    spfeasCSV = "C:/Temp/Kinshasa/spfeas_Res.csv"
    if not os.path.exists(spfeasCSV):
        curRes = generateSpfeasCSV(initials, location, outputFolder)
        curRes.to_csv(spfeasCSV)
    else:
        curRes = pd.read_csv(spfeasCSV)
    processCSV(curRes, initials, location, "D:/Kinshasa")
        '''