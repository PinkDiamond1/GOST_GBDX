# Tools for GBDx
There are two sets of tools here, and a script for launching these tools.
1. GOST_GBDx_Tools/gbdxTasks.py: This library is used to launch GBDx tasks. See createTasks to launch GBDx tasks that are based on the output script from Charles' searching script
2. GOST_GBDx_Tools/gbdxURL_misc: Manipulates the GBDx url fetches - useful for monitoring workflows and listing/downloading results. **Warning, the AWSCLI stuff is off-the-wall weird because of WBG limitations of web-based command line tools - discuss with Ben**

## Example running spfeas on Charles Scripts
```python
###This opening bit should be included in all examples, as it instantiates the GBDx objects
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc
from GOST_GBDx_Tools import spfeas_results
def getParams():
    thisFolder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
    # importantParameters.json is not included in the github repo, as it has passwords and such
    inputParameters = "%s/importantParameters.json" % thisFolder
    with open(inputParameters, 'rb') as data_file:                 
        jsonParams = json.load(data_file)
    return jsonParams
    
inputParams = getParams()
gbdx = Interface(
    username=inputParams['gbdx_username'],
    password=inputParams['gbdx_password'],
    client_id=inputParams['gbdx_client_id'],
    client_secret=inputParams['gbdx_client_secret']
)

curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(inputParams)

###Execute spfeas on results from Charles' search script
scenfile = r"C:\somethingFromCharles.csv"
outFolderS3 = bps/Tblisi/%s # The %s will be filled by the scene id
curTasks.defineInputData(sceneFile)
prepppedTasks = curTasks.createTasks(curTasks.scenesDataFrame, outFolderS3,
                    spfeasParams={"triggers":'ndvi', "scales":'2', "block":'2'}, 
                    runCarFinder = 0, runSpfeas = 1, downloadImages = 1,
                    aopPan=False, aopDra=False, aopAcomp = True, aopBands='MS')

                    
for t in prepppedTasks['WORKFLOWS']:
    t.execute()
    tPrint("%s - %s - %s" % (startTime, t.id, t.status['event'])) 

results = gbdxUrl.monitorWorkflows()
for f in results['FAILED']:
    print f

```
