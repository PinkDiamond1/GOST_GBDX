import os, sys
sys.path.append(r"C:\Code\Github\GOST_GBDX")

from gbdxtools import Interface
from gbdxtools import CatalogImage
from GOST_GBDx_Tools import gbdxURL_misc

gbdx = Interface()
gbdxUrl = gbdxURL_misc.gbdxURL(gbdx)

inFolder = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee//bps/cityAnalysis/Lubumbashi_WM/clippedRaster/"
outFolder = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/bps/cityAnalysis/Lubumbashi_WM/clippedRaster/"
#056854653010 - 1040010031293100
#056914585010 - 10400100315E4200

filesA = ['056854653010_01_assembly.IMD', '056854653010_01_assembly.XML', '056854653010_01_assembly_clip.tif']
filesB = ['056914585010_01_assembly.IMD', '056914585010_01_assembly.XML', '056914585010_01_assembly_clip.tif']

for f in filesA:
    source = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee//bps/cityAnalysis/Lubumbashi_WM/clippedRaster/%s" % f
    dests = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/bps/cityAnalysis/Lubumbashi_WM/1040010031293100/clippedRaster/%s" % f
    print ("aws s3 mv %s %s" % (source, dests))

for f in filesB:
    source = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee//bps/cityAnalysis/Lubumbashi_WM/clippedRaster/%s" % f
    dests = "s3://gbd-customer-data/1c080e9c-02cc-4e2e-a8a2-bf05b8369eee/bps/cityAnalysis/Lubumbashi_WM/10400100315E4200/clippedRaster/%s" % f
    print ("aws s3 mv %s %s" % (source, dests))
