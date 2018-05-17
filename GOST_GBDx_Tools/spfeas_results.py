import sys, os, re, glob, fnmatch, inspect, json, subprocess, time, yaml, logging
import geopandas as gpd
import pandas as pd
import rasterio, numpy

from shapely.geometry import Polygon
from affine import Affine
from rasterio.features import rasterize

from gbdxtools import Interface
from GOST_GBDx_Tools import gbdxTasks
from GOST_GBDx_Tools import gbdxURL_misc

def zonalStats(inVector, inRaster, bandNum=1, reProj = False, minVal = '', rastType='N', verbose=False):
    outputData=[]
    with rasterio.open(inRaster, 'r') as curRaster:
        if inVector.crs != curRaster.crs:
            if reProj:
                inVector = inVector.to_crs(curRaster.crs)
            else:
                raise ValueError("Input CRS do not match")
        fCount = 0
        tCount = len(inVector['geometry'])
        for geometry in inVector['geometry']:
            fCount = fCount + 1
            logging.info("Processing %s of %s" % (fCount, tCount))
            if fCount % 100 == 0 and verbose:
                print("Processing %s of %s" % (fCount, tCount) )
            try:
                # get pixel coordinates of the geometry's bounding box
                ul = curRaster.index(*geometry.bounds[0:2])
                lr = curRaster.index(*geometry.bounds[2:4])
                # read the subset of the data into a numpy array
                window = ((lr[0], ul[0]+1), (ul[1], lr[1]+1))
                data = curRaster.read(bandNum, window=window)
                # create an affine transform for the subset data
                t = curRaster.affine
                shifted_affine = Affine(t.a, t.b, t.c+ul[1]*t.a, t.d, t.e, t.f+lr[0]*t.e)
                # rasterize the geometry
                mask = rasterize(
                    [(geometry, 0)],
                    out_shape=data.shape,
                    transform=shifted_affine,
                    fill=1,
                    all_touched=True,
                    dtype=numpy.uint8)
                # create a masked numpy array
                masked_data = numpy.ma.array(data=data, mask=mask.astype(bool))
                if rastType == 'N':                
                    if minVal != '':
                        masked_data = numpy.ma.masked_where(masked_data < minVal, masked_data)
                        if masked_data.count() > 0:                        
                            results = [masked_data.sum(), masked_data.min(), masked_data.max(), masked_data.mean()]
                        else :
                            results = [-1, -1, -1, -1]
                    else:
                        results = [masked_data.sum(), masked_data.min(), masked_data.max(), masked_data.mean()]
                if rastType == 'C':
                    results = numpy.unique(masked_data, return_counts=True)                                        
                outputData.append(results)
                
            except Exception as e: 
                logging.warning("Error %s: %s" % (fCount, e.message) )                               
                outputData.append([-1, -1, -1, -1])
    return outputData   
    
class processSpfeas(object):
    '''The results of the spfeas analysis run on GBDx is a folder containg the following pieces
    1. Folder of tiled results
    2. YAML describing the processing
    3. VRT of the tiled results
        a. This shit is broken out of the box and needs to be re-created
    '''
    def __init__(self, spfeasFolder):
        '''read in all the input data
        spfeasFolder [string] - folder to be processed
        '''
        self.inputFolder = spfeasFolder        
        for x in os.walk(spfeasFolder):                        
            if x[0] != spfeasFolder:
                self.tiledFolder = x[0]
                print self.tiledFolder
            for y in x[2]:
                if y[-4:] == "yaml":
                    self.yamlFile = os.path.join(x[0], y)
                    with open(self.yamlFile, 'r') as yamlFile:
                        self.yaml = yaml.load(yamlFile)
                        self.getResultsYAML()
        
        self.spfeasVRT = os.path.join(spfeasFolder, "%s.vrt" % os.path.basename(self.tiledFolder))
        '''
        try:
            self.spfeasVRT = glob.glob("%s/*.vrt" % spfeasFolder)[0]
        except:
            self.spfeasVRT = ''
        '''
        self.zonalCSV = self.spfeasVRT.replace(".vrt", "_zonal.csv")
    
    def getResultsYAML(self):
        #Get a list of the specified triggers
        self.definedTriggers = self.yaml['BAND_ORDER'].keys()
        #Go through the final tile results and see how they match the defined triggers
        self.completedTriggers = []     
        #get a reference to the final tile
        tileList = []
        maxVal = 0
        for x in self.yaml.keys():
            try:
                curVal = int(x[-6:])
                if maxVal < curVal:
                    maxVal = curVal
                    finalTileIndex = x
            except:
                fu = "bar"
        
        finalTile = self.yaml[finalTileIndex]

        for key, value in finalTile.iteritems():
            if value == "complete":
                self.completedTriggers.append(key.replace("-1", ""))
        
        self.missedTriggers = []
        for f in self.definedTriggers:
            if not f in self.completedTriggers:
                self.missedTriggers.append(f)
        
    def generateVRT(self, tempFolder="C:/WBG", gdalCommand="gdalbuildvrt.exe"):
        '''The vrt of the tiled spfeas results needs to be re-built with relative paths. 
        [optional] tempFolder [string] - the gdalbuildvrt command uses a text file with a list of images
        TODO: There are weird issues with the gdalbuildvrt command - it needs to be run from the WBG folder
            in order to properly function. In my case, the .exe has been copied there, and the script
            needs to be executed from that folder
        '''
        self.imageListFile = os.path.join(tempFolder, "%s_fileList.txt" % os.path.basename(self.tiledFolder))
        with open(self.imageListFile, 'w') as imageFile:
            for image in glob.glob("%s\*.tif" % self.tiledFolder):
                imageFile.write(image)
                imageFile.write("\n") 
        curCommand = r"%s -input_file_list %s %s" % (gdalCommand, self.imageListFile, self.spfeasVRT)
        print curCommand
        subprocess.call(curCommand.split(), shell=True) 
        
    def generateZonalHeaders(self):
        '''Combine the information from the YAML file concerning the band definitions and the zonal csv
        TODO: how do we get the length of the names list
        '''
        names = [''] * 1000
        for values, keys in self.yaml['BAND_ORDER'].iteritems():
            splitVals = keys.split("-")
            s = int(splitVals[0]) - 1
            e = int(splitVals[1])
            names[s:e] = [("%s_%s" % (b, a)) for b, a in zip([values] * (e - s), range(1, e-s+1))]                       
        return [x for x in names if x != '']
    
    def getZonalResults(self):
        ''' Remove bad rows from zonal statistics results
        returns [pandas dataframe] - clean spfeas results
        '''
        try:
            curD = self.zonalRes
        except:
            self.zonalRes = pd.read_csv(self.zonalCSV)
            curD = self.zonalRes
        #Identify rows that are uniformly -1 in the results folder and remove them    
        ignoreCols = [u'Unnamed: 0', u'Unnamed: 0.1', u'built1975', u'built1990', u'built2000', u'built2014', u'concat_id', u'cvegeo']
        analyzeCols = curD
        for x in ignoreCols:
            try:
                analyzeCols = analyzeCols.drop(x, axis=0)
            except:
                fu='bar'

        def testData(row):
            xx = row.value_counts()
            return (float(xx[-1])/len(row))

        curD['QUALITY'] = analyzeCols.apply(testData, axis=1)
        
        #Drop all rows with quality > 0.8
        curD = curD.drop(curD[curD.QUALITY > 0.8].index)
        
        #Drop columns that are only half processed
        for curColumn in curD.columns:
            if curColumn.split("_")[0] in self.missedTriggers:
                curD = curD.drop(curColumn, axis=1)
        curD.set_index('concat_id')
        #curD.to_csv(self.zonalRes)
        return curD

    def zonalVRT(self, inputShapeFile, outCSV=''): 
        ''' Run zonal statistics against every vrt band
        inputShapeFile [string] - path to shapefile for running zonal statistics
            returns - nothing
        '''
        finalResults = self.zonalCSV
        inVRT = self.spfeasVRT
        inR = rasterio.open(inVRT, 'r')
        inputShapeD = gpd.read_file(inputShapeFile)
        totalBands = inR.count + 1
        #Select features from inputShapeD that intersect thise raster
        inRShape = Polygon([(inR.bounds.left, inR.bounds.top),
                            (inR.bounds.right, inR.bounds.top),
                            (inR.bounds.right, inR.bounds.bottom),
                            (inR.bounds.left, inR.bounds.bottom),
                            (inR.bounds.left, inR.bounds.top)])
        intersects=[]
        contains = []
        idx = 0
        for x in inputShapeD['geometry']:
            if x.intersects(inRShape):
                intersects.append(idx)
            if inRShape.contains(x):
                contains.append(idx)
            idx = idx + 1

        intersectD = inputShapeD.loc[intersects,:]

        allRes = []
        origD = intersectD
        colNames = self.generateZonalHeaders()
        print "bands and names: %s - %s" % (totalBands, len(colNames))
        print colNames
        for bndCnt in range(1, totalBands):    
            logging.info("Looping through band %s of %s" % (bndCnt, totalBands))
            # Run zonal statistics on raster using shapefile
            columnNames = ["%s_SUM" % colNames[bndCnt-1], 
                           "%s_MIN" % colNames[bndCnt-1], 
                           "%s_MAX" % colNames[bndCnt-1], 
                           "%s_MEAN" % colNames[bndCnt-1]]            
            curRes = pd.DataFrame(zonalStats(origD, inVRT, bndCnt, True), columns = columnNames)
            # intersectD = pd.concat([intersectD, curRes], axis=1)
            if bndCnt == 1:
                finalRes = curRes                
            else:
                finalRes = pd.concat([finalRes, curRes], axis=1)

        intersectD.index = finalRes.index
        finalD = pd.concat([intersectD, finalRes], axis=1)
        finalD = finalD.drop('geometry', 1)
        self.zonalRes = finalD
        try:
            finalD.to_csv(outCSV)
        except:
            print "why the fuck doesn't this work"
        