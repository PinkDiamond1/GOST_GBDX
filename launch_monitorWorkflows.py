#######################################
# Monitor GBDx workflows
#######################################
import sys, os, inspect, json
import geopandas as gpd

from gbdxtools import Interface

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc

gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

workflows = gbdxUrl.monitorWorkflows(sleepTime=180)

for x in workflows['FAILED']:
    print x[0]['tasks'][0]['outputs'][0]['persistLocation'].split("/")[3]



