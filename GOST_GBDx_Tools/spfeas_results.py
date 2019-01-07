import sys, os, re, glob, fnmatch, inspect, json, subprocess, time, logging
import rasterio, numpy, scipy

import geopandas as gpd
import pandas as pd
#import dask.array as da

from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.geometry import box
from affine import Affine
from rasterio.features import rasterize
from gbdxtools import CatalogImage
from gbdxtools import Interface

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
                            results = [masked_data.sum(), masked_data.min(), masked_data.max(), masked_data.mean(), masked_data.std()]
                        else :
                            results = [-1, -1, -1, -1]
                    else:
                        results = [masked_data.sum(), masked_data.min(), masked_data.max(), masked_data.mean(), masked_data.std()]
                if rastType == 'C':
                    results = numpy.unique(masked_data, return_counts=True)                                        
                outputData.append(results)
                
            except Exception as e: 
                logging.warning("Error %s: %s" % (fCount, e.message) )                               
                outputData.append([-1, -1, -1, -1, -1])
    return outputData   
    
class processLoopedSpfeas(object):
    def __init__(self, spfeasFolder):
        self.basePath = spfeasFolder
        self.catID = os.path.dirname(spfeasFolder)
        self.triggers = os.listdir(spfeasFolder)
        self.stackedDef = ("%s_stacked_definition.txt" % spfeasFolder)        
        self.stackedFolderRGB = ("%s_stackedRGB" % spfeasFolder)
        self.outVRT   = "%s_stackedRGBNDSV.vrt" % spfeasFolder
        self.fileList = "%s_stackedRGBNDSV_files.txt" % spfeasFolder
        self.outVRT_extent = "%s_stackedRGBNDSV_extent.txt" % spfeasFolder
        if not os.path.exists(self.stackedFolderRGB):
            os.makedirs(self.stackedFolderRGB)
        self.stackedFolder = ("%s_stacked" % spfeasFolder)
        if not os.path.exists(self.stackedFolder):
            os.makedirs(self.stackedFolder)
    
    def generateImageWKT(self, outFile):
        ''' generate a text file with a boundary wkt
        
        curR = rasterio.open(self.outVRT)
        from shapely.geometry import box
        b = curR.bounds
        with open(self.outVRT_extent, 'w') as dest:
            #dest.write("{catID}, POLYGON({left} {bottom},{left} {top},{right} {top},{right} {bottom},{left} {bottom}".format(catID=self.catID, left=b.left, right=b.right, top=b.top, bottom=b.bottom))    
            dest.write("ID,WKT\n")
            dest.write("%s,%s\n" % (self.catID, str(box(b.left, b.bottom, b.right, b.top))))
        EXAMPLE:
        inFolder = r"D:\Kinshasa\Shohei_Poverty\spfeas\103001007FA97400_stackedRGB"
        '''
        inFolder = self.stackedFolderRGB
        #List all the RGB tiles in the self.stackedFolderRGB
        inFiles = os.listdir(inFolder)
        allTiles = []
        for f in inFiles:
            curTile = os.path.join(inFolder, f)
            curR = rasterio.open(curTile)
            b = curR.bounds
            allTiles.append([f, box(b.left,b.bottom,b.right,b.top)])

        res = pd.DataFrame(allTiles, columns=["FileName","geometry"])
        geoRes = gpd.GeoDataFrame(res, geometry=res.geometry)
        geoRes.to_csv(outFile) 
        
    def buildVRT(self):
        ''' buildVRT for the final stacked dataset
        '''
        fileList = os.listdir(self.stackedFolderRGB)
        with open(self.fileList, 'w') as dest:
            for f in fileList:
                dest.write("%s\n" % os.path.join(self.stackedFolderRGB, f))
        
        command = "gdalbuildvrt -input_file_list %s %s" % (self.fileList, self.outVRT)
        print(self.outVRT)
        subprocess.call(command)
    
    def createSpfeasCommands(self, outFile, samplesFile="FAKE.txt"):
        '''
        Running spfeas Docker
        docker run -it -v D:\Addis\spfeas:/mnt/work geographyis/spfeas:latest
        source activate spfeasenv
        cd /mnt/work
        sample-raster -h
        sample-raster -e
        ### Had to manually set the projection file of the sample points
        ### had to manually change the direction of the / in the vrt
        sample-raster -s trainingSites/training_sites.shp -i /mnt/work/10400100361EC300_stackedRGBNDSV.vrt

        classify -i/mnt/work/10400100361EC300_stackedRGB/058846819010_01_assembly_clip__BD1_BK8_SC8-16-32__ST1-003__TL000005.tif -o /mnt/work/10400100361EC300_classified.tif -s trainingSites/training_sites_points__10400100361EC300_stackedRGBNDSV_SAMPLES.txt --output-model trainingSites/RF_model.txt --classifier-info "{'classifier': 'RF','trees':1000}" --row-block 512 --col-block 512
        
        classify -i 
        '''
        baseFolder = os.path.dirname(self.basePath)
        outFolder = self.stackedFolderRGB + "_Classified"
        if not os.path.exists(outFolder):
            os.makedirs(outFolder)
        with open(outFile, 'w') as out:
            out.write("docker run -it -v {}:/mnt/work geographyis/spfeas:latest\n".format(baseFolder))
            out.write("source activate spfeasenv\n")
            out.write("cd /mnt/work\n")
            out.write("sample-raster -s trainingSites/training_sites.shp -i /mnt/work/{}\n".format(os.path.basename(self.outVRT)))
            inTiffs = os.listdir(self.stackedFolderRGB)
            for t in inTiffs:
                out.write("classify -i/mnt/work/{stackedFolder}/{t} -o /mnt/work/{classifiedFolder}/{t} -s trainingSites/{samplesFile} --output-model trainingSites/RF_model.txt --classifier-info \"{{'classifier': 'RF','trees':1000}}\" --row-block 625 --col-block 625 --jobs 1 --v-jobs 1\n".format(
                    stackedFolder=os.path.basename(self.stackedFolderRGB),
                    classifiedFolder=os.path.basename(outFolder),
                    t=os.path.basename(t),
                    samplesFile=samplesFile))
            
    
    def createPointSamples(self, samplesFile, gridSize=5,idColumn='id'):
        ''' Perform the sampling functionality from the mpglue classification
            1. Create gridded sample within samples file
            2. Sample the input VRT for each of the points
        INPUT:
        samplesFile [string] - path to input shapefile of sample locations
        
        EXAMPLE:
        inD = gpd.read_file(r"D:\Kinshasa\Shohei_Poverty\trainingData.shp")
        inR = rasterio.open(r"D:\Kinshasa\Shohei_Poverty\spfeas\103001007FA97400_stackedRGBNDSV.vrt")
        '''
        inD = gpd.read_file(samplesFile)        
        inR = rasterio.open(self.outVRT)
        if inD.crs != {'init':'epsg:3857'}:
            inD = inD.to_crs({'init':'epsg:3857'})

        #Create gridded point shapefile within the samples dataset
        vals = []
        for idx, row in inD.iterrows():
            b = row['geometry'].bounds
            yRange = list(range(int(b[1]), int(b[3]), gridSize))
            xRange = list(range(int(b[0]), int(b[2]), gridSize))
            for xIdx in range(0,len(xRange)):
                for yIdx in range(0,len(yRange)):
                    vals.append([idx,xIdx,yIdx,xRange[xIdx],yRange[yIdx],Point(xRange[xIdx],yRange[yIdx]),row[idColumn]])

        #Limit the point file to those that intersect the training samples
        ptSamples = pd.DataFrame(vals, columns=['gIdx',"xIdx","yIdx","X","Y","geometry","response"])
        ptGeom = gpd.GeoDataFrame(ptSamples, geometry=ptSamples['geometry'])
        ptGeom.crs = {'init':'epsg:3857'}
        baseShape = inD['geometry'].unary_union
        ptGeom = ptGeom[ptGeom.intersects(baseShape)]
        ptGeom = ptGeom.to_crs(inR.crs)

        #Sample the stacked raster dataset
        curXY = zip(ptGeom['geometry'].x, ptGeom['geometry'].y)
        xx = inR.sample(curXY)
        allVals = []
        for x in xx:
            allVals.append(x)

        sampledRes = pd.DataFrame(allVals,columns=["%s.%s" % (vrtFile, i) for i in range(1, inR.count + 1)])
        vrtFile = os.path.basename(self.outVRT).replace(".vrt","")
        vrtFile = "103001007FA97400_stackedRGBNDSV"
        toJoin = ptGeom.drop(['gIdx','xIdx','yIdx','geometry'],axis=1)
        toJoin = toJoin.reset_index()
        toJoin = sampledRes.join(toJoin)
        toJoin['Id'] = toJoin.index
        toJoin.to_csv(samplesFile.replace(".shp", "_samples.csv"), index=False)       
    
    def combineLoopedResults(self):
        ''' Sometimes, SPFEAS results come in separate folders for each trigger. 
            in such cases, the results need to be stacked together
        INPUT
        outFolder [string] - outputFolder to outputimages
        [optional] stackMeans [boolean] - if true, also stack mean band values from the original image
        [optional] calculateNDSV [boolean] - if true, also stack NDSVI from the original image        
        
        Example:
        basePath = r"D:\Kinshasa\Shohei_Poverty\spfeas\103001007FA97400"
        triggers = os.listdir(basePath)
        outFolder = r"D:\Kinshasa\Shohei_Poverty\spfeas\103001007FA97400_stacked"
        sampleTrigger = os.path.join(basePath, "dmp")
        '''
        outFolder = self.stackedFolder
        #Get basepath for all folders
        sampleTrigger = os.path.join(self.basePath, self.triggers[0])
        allTiffs = []
        for root, dirs, files in os.walk(sampleTrigger):
            for f in files:
                if f[-4:] == ".tif":
                    allTiffs.append(f)
        
        cnt = 0
        for a in allTiffs:
            cnt += 1
            logging.info("Stacking SPFEAS for %s of %s" % (cnt, len(allTiffs)))
            outTiff = os.path.join(outFolder, a)
            tNum = a.split("__")[-1]
            #Search for other similar numbered files
            #matchingTiffs = glob.glob("%s\*\*\*%s" % (self.basePath, tNum))
            matchingTiffs = []           
            for root, dirs, files in os.walk(self.basePath):
                for f in files:
                    if f[-(len(tNum)):] == tNum:
                        matchingTiffs.append(os.path.join(root, f))
            openedTiffs = []
            bandCount = 0
            #Get total number of bands for output
            for m in matchingTiffs:
                try:
                    xx = rasterio.open(m)
                    openedTiffs.append(xx)
                    bandCount += xx.count    
                except:
                    logging.warning("Could not process %s"  % m)
            #Stack spfeas triggers
            outFileMeta = openedTiffs[0].meta.copy()
            outFileMeta.update({"count":bandCount})
            with rasterio.open(outTiff, 'w', **outFileMeta) as dest:
                bndCnt = 1
                for cImg in openedTiffs:
                    curD = cImg.read()
                    for bIdx in range(0, curD.shape[0]):
                        dest.write(curD[bIdx,:,:], bndCnt)
                        bndCnt += 1

        #write matching tiff metadata
        outMeta = {}
        bndCnt = 0
        for m in matchingTiffs:
            curR = rasterio.open(m)
            outMeta[os.path.basename(os.path.dirname(os.path.dirname(m)))] = "%s - %s" % (bndCnt, bndCnt + curR.count - 1)
            bndCnt += curR.count

        with open(self.stackedDef, 'w') as dest:
            #yaml.dump(data, dest, default_flow_style=False)
            for key, value in outMeta.items():
                dest.write("%s:%s\n" % (key, value))

    def attachRGB_NDSV_Bands(self, catID):
        ''' Attach mean band values and resize to another tiled
        INPUT
        rgbI [rasterio object ... might also be a dask image] - original image to summarized
        inputTile [string] - path to stacked image of output resolution and size
        outImage [string] - new output image to create
        
        EXAMPLE
        import rasterio, scipy
        from gbdxtools import CatalogImage
        from gbdxtools import Interface
        import dask.array as da
        gbdx = Interface()
        inputTile = r"D:\Kinshasa\Shohei_Poverty\spfeas\103001007FA97400_stacked\058558367010_01_assembly_clip__BD1_BK8_SC8-16-32__ST1-006__TL000002.tif"
        outImage = r"D:\Kinshasa\Shohei_Poverty\spfeas\058558367010_01_assembly_clip__BD1_BK8_SC8-16-32__ST1-006__TL000002.tif"
        '''
        allTiffs = os.listdir(self.stackedFolder)
        rgbI = CatalogImage(catID)
        cnt = 0
        tilesToDelete = []
        for inputTile in allTiffs:
            cnt += 1
            logging.info("Adding RBG and NDSV for %s of %s" % (cnt, len(allTiffs)))
            outImage = os.path.join(self.stackedFolderRGB, os.path.basename(inputTile))
            fullInputTilePath = os.path.join(self.stackedFolder, inputTile)
            spI = rasterio.open(fullInputTilePath)
            meta = spI.meta.copy()
            tBandCount = spI.count  #Spfeas band count
            tBandCount = tBandCount + rgbI.shape[0] #Add bands from RGB
            tBandCount = tBandCount + int(rgbI.shape[0] * (rgbI.shape[0] - 1) / 2) #Add bands from NDSV
            meta.update(count = tBandCount)
            #Open satellite imagery
            try:
                data = rgbI.aoi(bbox=spI.bounds)
                allRes = []     
                resTitles = [] 
                shrunkenData = []
                bandNames = rgbI.metadata['image']['bandAliases']  
                bIdx = 0
                for bIdx in range(0, rgbI.shape[0]):
                    cData = data[bIdx,:,:]
                    newData = scipy.ndimage.zoom(cData, (1/float(cData.shape[0] / spI.shape[0])), order=2)
                    newData = newData[:spI.shape[0],:spI.shape[1]] 
                    shrunkenData.append(newData)
                    allRes.append(newData)
                    resTitles.append("RAW_%s" % bandNames[bIdx])

                for b1 in range(0, rgbI.shape[0]):
                    for b2 in range(0, rgbI.shape[0]):
                        if b1 < b2:
                            #cMetric = (data[b1,:,:] - data[b2,:,:]) / (data[b1,:,:] + data[b2,:,:])
                            cMetric = (shrunkenData[b1] - shrunkenData[b2]) / (shrunkenData[b1] + shrunkenData[b2])
                            allRes.append(cMetric)
                            resTitles.append("NDSV_%s_%s" % (bandNames[b1], bandNames[b2]))

                #Read in spfeas results and stack results
                spfeas = spI.read()
                #Write with rasterio
                with rasterio.open(outImage, 'w', **meta) as dest1:
                    totalBandCount = 1
                    #Write spfeas bands
                    for bIdx in range(0, spI.count):
                        dest1.write(spfeas[bIdx,:,:], totalBandCount)
                        totalBandCount += 1
                    #Write RGB and NDSV results
                    for bIdx in range(0, len(allRes)):#outData.shape[0]):
                        #dest1.write(outData[bIdx,:,:], totalBandCount)
                        dest1.write(allRes[bIdx], totalBandCount)    
                        totalBandCount += 1
            except Exception as e:
                logging.warning(str(e))
                tilesToDelete.append(fullInputTilePath)
        return(tilesToDelete)


    
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
            for y in x[2]:
                if y[-4:] == "yaml":
                    self.yamlFile = os.path.join(x[0], y)
                    with open(self.yamlFile, 'r') as yamlFile:
                        self.yaml = yaml.load(yamlFile)
                        self.getResultsYAML()
        
        self.spfeasVRT = os.path.join(spfeasFolder, "%s.vrt" % os.path.basename(self.tiledFolder))
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
        
    def generateVRT(self, tempFolder="C:/Temp", gdalCommand="gdalbuildvrt.exe"):
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
        for bndCnt in range(1, totalBands):    
            logging.info("Looping through band %s of %s" % (bndCnt, totalBands))
            # Run zonal statistics on raster using shapefile
            columnNames = ["%s_SUM" % colNames[bndCnt-1], 
                           "%s_MIN" % colNames[bndCnt-1], 
                           "%s_MAX" % colNames[bndCnt-1], 
                           "%s_MEAN" % colNames[bndCnt-1],
                           "%s_STD" % colNames[bndCnt-1]]            
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
            print("why the fuck doesn't this work")
        