################################################################################
# Run spfeas locally
# Benjamin Stewart, November 2018
# Purpose: Generate command lines to run spfeas analysis on imagery on a local drive
#   The results will then be analyzed through the launch_process_spfeas.py
################################################################################

import os, sys, glob

baseFolder = r"D:/Monrovia"

inImages = glob.glob("%s/*./*.tif" % baseFolder)

allImages = []
for root, dirs, files in os.walk(baseFolder):
    for f in files:
        if f[-4:] == ".tif":
            allImages.append(os.path.join(root, f))

with open("C:/Temp/spfeasCommands.txt", 'w') as outFile:
    for img in allImages:
        bFolder = os.path.dirname(os.path.dirname(img))
        cID = os.path.basename(bFolder)
        outFolder = os.path.join(bFolder, "spfeas")
        #os.makedirs(outFolder)
        for tr in ['dmp','fourier','gabor','grad','hog','lac','lbpm','lsr','mean','orb','pantex','saliency','seg','sfs']:
            curString = "spfeas -i {inImage} -o {outFolder} --sensor WorldView3 --block 4 --scales 4 8 16 32 -tr {trigger} --sect-size 2000 --gdal-cache 1024 \n".format( \
                                  inImage = img.replace(baseFolder.replace("\\", "/"), "/mnt/data"), \
                                  outFolder= os.path.join(os.path.dirname(os.path.dirname(img.replace(baseFolder, "\\mnt\\data"))), "spfeas"), \
                                  trigger = tr)
            outFile.write(curString)
            
        
        
        