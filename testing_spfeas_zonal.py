import os, sys, inspect, argparse, logging

import geopandas as gpd
import pandas as pd

from gbdxtools import Interface

#Import a number of existing GOST functions
GOSTRocks_folder = os.path.dirname(os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])))
GOST_GBDx_folder = r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST_GBDx"
GOST_Public_Goods = r"C:\Users\WB411133\OneDrive - WBG\AAA_BPS\Code\Code\Github\GOST_PublicGoods"

if GOSTRocks_folder not in sys.path:
    sys.path.insert(0, GOSTRocks_folder)

if GOST_GBDx_folder not in sys.path:
    sys.path.insert(0, GOST_GBDx_folder)

if GOST_Public_Goods not in sys.path:
    sys.path.insert(0, GOST_Public_Goods)

from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc
from GOST_GBDx_Tools import imagery_search

gbdx = Interface()
curTasks = gbdxTasks.GOSTTasks(gbdx)
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

inShapefile = r"s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/bps"
spfeasFolder = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee//bps/cityAnalysis/Lubumbashi_WM/spfeas/"
'''
spTask = gbdx.Task("spfeas_zonal:0.4.0", data_in=spfeasFolder, shapefile=inShapefile)
workflow = gbdx.Workflow([spTask])
workflow.savedata(spTask.outputs.data_out, location="bps/cityAnalysis/Lubumbashi_WM_spfeasZonal")
workflow.execute()
'''
print gbdxUrl.monitorWorkflows()
#gbdxUrl.descWorkflow('4920141973704071905')