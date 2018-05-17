################################################################################
# Process SPFEAS
# Benjamin P. Stewart
################################################################################

import sys, os, inspect

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)
from GOST_GBDx_Tools import spfeas_results

inFolder = r"D:\SriLanka\SpfeasRes"
inShp = r"Q:\WORKINGPROJECTS\LKA_Poverty\GNs\AllGNs_Merged.shp"
with open(r"D:\SriLanka\missedTriggers.csv", 'w') as missedTriggersFile:
    for f in os.listdir(inFolder):
        print f
        curSpfeas = spfeas_results.processSpfeas(os.path.join(inFolder, f))
        outZonal = r"D:\SriLanka\%s_zonal.csv" % f
        if not os.path.exists(outZonal):
            curSpfeas.generateVRT(gdalCommand=r"C:\WBG\GDAL\bin\gdalbuildvrt.exe")
            curSpfeas.zonalVRT(inShp, outZonal)
        try:
            missedTriggers = curSpfeas.missedTriggers
        except:
            curSpfeas.getResultsYAML()
            missedTriggers = curSpfeas.missedTriggers
        missedTriggersFile.write('%s,%s\n' % (f, ','.join(missedTriggers)))