################################################################################
# Process SPFEAS
# Benjamin P. Stewart
################################################################################

import sys, os, inspect, logging

logging.basicConfig(level=logging.INFO)

cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

from GOST_GBDx_Tools import spfeas_results

for catID in ['10500100133D8500']: #'10400100361EC300','1040010036B51000'
    spfeasFolder  = r"D:\Addis\spfeas\%s" % catID
    curSpfeas = spfeas_results.processLoopedSpfeas(spfeasFolder)
    curSpfeas.combineLoopedResults()
    xx = curSpfeas.attachRGB_NDSV_Bands(catID)
    for x in xx:
        os.remove(x)
    curSpfeas.buildVRT()  
    #curSpfeas.zonalVRT(r"D:\Kinshasa\Shohei_Poverty\ADMIN\bati_ilot_quartier.shp", outZonal)
    #curSpfeas.generateImageWKT(curSpfeas.outVRT.replace(".vrt", ".csv"))
    #curSpfeas.createPointSamples(r"D:\Kinshasa\Shohei_Poverty\trainingData.shp", gridSize=5)
    #curSpfeas.createSpfeasCommands(outFile="%s_classifyCommands.txt" % spfeasFolder, samplesFile="training_sites_points__10400100361EC300_stackedRGBNDSV_SAMPLES.txt")