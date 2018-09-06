import sys, os, time, subprocess, requests, json, logging
import shapely, geojson, rasterio

import pandas as pd
import geopandas as gpd
import dask.array as da

from time import strftime
from gbdxtools import Interface
from gbdxtools import CatalogImage
from shapely.geometry import shape


class GOSTTasks(object):
    def __init__(self, gbdx):
        self.gbdx = gbdx        
        self.sensorDict = {"WORLDVIEW01":"WorldView1", "WORLDVIEW02":"WorldView2", "GEOEYE01": "GeoEye1", "QUICKBIRD02":"Quickbird","WORLDVIEW03_VNIR":"WorldView3"}   
    
    def calculateIndices(self, inD, sensor, outFile):
        outImage = inD[0:2,:,:]
        if sensor in ['WV03','WV02']:
            irBand = 7
            redBand = 4
            c_Green = 0
        elif sensor in ['IKONOS-2']:
            irBand = 3
            redBand = 2
            c_Green = 1            
        else:
            raise ValueError("Indices not implemented for %s yet" % sensor)
        #calculate indices
        ndvi = (inD[irBand,:,:] - inD[redBand,:,:]) / (inD[irBand,:,:] + inD[redBand,:,:])
        ndwi = (inD[c_Green,:,:] - inD[irBand,:,:]) / (inD[c_Green,:,:] + inD[irBand,:,:])
        outData = da.stack([ndvi, ndwi])
        outData = (outData * 100) + 100
        outData = outData.astype('uint8')
        metadata = inD.ipe.metadata
        newDataset = rasterio.open(outFile, 'w', driver="GTiff", dtype='uint8',
                                    count=outData.shape[0],height=outData.shape[1],width=outData.shape[2],
                                    crs=inD.proj,transform=inD.affine)
        newDataset.write(outData)
        newDataset.close()
           
    def downloadImage(self, cat_id, outFile, output="IMAGE", boundingBox=None, curWKT=None, 
        imgChipSize = 1000, band_type = "MS", panSharpen=True, acomp=True, getOutSize=False):
        ''' Uses the CatalogImage object to download and write data
            http://gbdxtools.readthedocs.io/en/latest/api_reference.html#catalogimage
        '''
        img = CatalogImage(cat_id, pansharpen=panSharpen, band_type=band_type, acomp=acomp)
        sensor = img.ipe_metadata['image']['sensorPlatformName']
        if boundingBox:
            img = img.aoi(bbox=boundingBox)
        if curWKT:
            #Intersect the bounds of the curImg and the curWKT
            b = img.bounds
            curImageBounds = [[[b[0], b[1]], [b[0], b[3]], [b[2], b[3]], [b[2], b[1]], [b[0], b[1]]]]
            inPoly = geojson.Polygon(curImageBounds)
            imgBounds = shape(inPoly)
            img = img.aoi(wkt=str(imgBounds.intersection(curWKT)))            
        if getOutSize: #If this flag is set, return the size of the total image instead
            return img.shape        
        #If the output image is going to be large, then write the output as tiled results
        if img.shape[1] > imgChipSize or img.shape[2] > imgChipSize:
            #Create output directory based on file name
            outFolder = outFile.replace(".tif", "")
            try:
                os.mkdir(outFolder)            
            except:
                pass
            rowSteps = range(0,img.shape[1],imgChipSize)
            rowSteps.append(img.shape[1])
            colSteps = range(0,img.shape[2],imgChipSize)
            colSteps.append(img.shape[2])
            for rIdx in range(0, len(rowSteps) - 1, 1):
                for cIdx in range(0, len(colSteps) - 1, 1):
                    print("Downloading row %s of %s and column %s of %s" % (rIdx, len(rowSteps), cIdx, len(colSteps)))
                    outputChip = os.path.join(outFolder, "C%s_R%s.tif" % (cIdx, rIdx))
                    curChip = img[0:img.shape[0], rowSteps[rIdx]:rowSteps[rIdx + 1], colSteps[cIdx]:colSteps[cIdx + 1]]
                    if not os.path.exists(outputChip):
                        if output == "IMAGE":
                            curChip.geotiff(path=outputChip)
                        if output == "INDICES":
                            outImage = self.calculateIndices(curChip, sensor, outputChip) 
        else:            
            img.geotiff(path=outFile)
        return 1
    
    def downloadAOP(self, cat_id, outFolder, boundingWKT, aopDra=True, panSharpen=True, acomp=True, aopBands='Auto'):
        ''' Uses the AOP Strip Processor to standardize and download tiles
        [outFolder] - s3Folder for storing imagery
        [boundingWKT] - WKT shape for selecting tiles to process 
        https://gbdxdocs.digitalglobe.com/docs/advanced-image-preprocessor
        '''
        data = self.gbdx.catalog.get_data_location(cat_id)                    
        aopParts = self.getImageParts(cat_id, boundingWKT)
        if len(aopParts) > 0:
            aoptask = self.gbdx.Task("AOP_Strip_Processor", data=data, parts=aopParts,
                                enable_pansharpen=panSharpen, enable_dra=aopDra, 
                                enable_acomp=acomp, bands=aopBands)
            workflow = self.gbdx.Workflow([aoptask])

            # save the outputs to your s3 bucket.  This method only needs a folder specified.
            workflow.savedata(aoptask.outputs.data, location=outFolder)  
            return workflow
    
    def defineInputData(self, inSceneFile):
        #Process gbdx params and process input scene file
        inD = pd.read_csv(inSceneFile)
        geometry = inD['used_scene_region_WKT'].map(shapely.wkt.loads)
        gD = gpd.GeoDataFrame(inD, crs={'init': 'epsg:4326'}, geometry=geometry)
        tempD = pd.DataFrame(gD.bounds)
        tempD = tempD[["minx","maxy","maxx","miny"]]
        inD['bbox'] = tempD[tempD.columns[0:]].apply(lambda x: ','.join(x.astype(str)),axis=1)
        self.scenesDataFrame = inD

    def reprocessMissedTasks(self, sceneFile, inS3Folder, outS3Folder, verbose=False):
        ''' Reprocess spfeas for missing triggers created from spfeas_results.py
        [sceneFile] - input csv with two columns, cat_id and space-delimited missing triggers
        [inS3Folder] - s3 folder containing the clippedRaster and the original spfeas results. Must be defined
                        as the full 
        [outS3Folder] - OFTEN THE SAME AS inS3Folder. Can be different, will contain output in spfeas2 folder        
        '''
        inD = pd.read_csv(sceneFile)
        allTasks = []
        for index, row in inD.iterrows():
            curTasks = []
            catalog_id = row[0]
            triggers = row[1]
            inData = inS3Folder % (catalog_id, "clippedRaster")
            outData = outS3Folder % (catalog_id, "spfeas2")
            if verbose:
                print ("%s, %s, %s, %s" % (inData, self.sensorDict[row[2]], triggers, outData))            
            spTask = self.gbdx.Task("spfeas_WBG:0.4.0", data_in=inData, sensor=self.sensorDict[row[2]], 
                       triggers=triggers, scales='8 16 32', block='4', gdal_cache=64, section_size=2000)
            curTasks.append(spTask)
            workflow = self.gbdx.Workflow(curTasks)
            workflow.savedata(spTask.outputs.data_out, location=outData)
            allTasks.append(workflow)
        return(allTasks)
    
    def runSpfeas(self, inDataFolder, outDataFolder,
                    spfeasParams={"triggers":'ndvi mean', "scales":'8 16 32', "block":'4'}):
        curTasks = []
        #Run spfeas on the panchromatic band (no vegetation calculations)
        spTask = self.gbdx.Task("spfeas_WBG:0.1.3", data_in=inDataFolder, sensor=spfeasParams['SENSOR'], 
            triggers=spfeasParams['triggers'], scales=spfeasParams['scales'], block=spfeasParams['block'])
        curTasks.append(spTask)
        workflow = self.gbdx.Workflow(curTasks)
       
        # save the outputs to your s3 bucket.  This method only needs a folder specified.  It will create this folder in your gbd-customer-customer-data s3 bucket.
        workflow.savedata(spTask.outputs.data_out, location=outDataFolder)
        return workflow
  
    def createWorkflow(self, catalog_id, inputWKT, sensor, outS3Folder,
                    spfeasParams={"triggers":'ndvi mean', "scales":'8 16 32', "block":'4'}, 
                    runCarFinder = 0, runSpfeas = 1, spfeasLoop = 0, runLC = 0, downloadImages = 1, 
                    aopPan=False, aopDra=False, aopAcomp = True, aopBands='Auto', inRaster=""):
        '''
        
        REFERENCE:
        LULC: https://gbdxdocs.digitalglobe.com/docs/automated-land-cover-classification-1
        '''
        curTasks = []        
        data = self.gbdx.catalog.get_data_location(catalog_id)        
        aopParts = self.getImageParts(catalog_id, inputWKT)
        if len(aopParts) > 0:
            if inRaster == "": #Run AOP and raster clip if input raster is not defined
                aoptask = self.gbdx.Task("AOP_Strip_Processor", data=data, parts=aopParts, 
                                    enable_pansharpen=aopPan, enable_dra=aopDra, 
                                    enable_acomp=aopAcomp, bands=aopBands)
                curTasks.append(aoptask)

                #clip the image
                raster = aoptask.outputs.data.value
                #Generate ul_lr bbox from the inputWKT
                sWkt = shapely.wkt.loads(inputWKT)
                sbbox = sWkt.bounds
                bbox = '%s,%s,%s,%s' % (sbbox[0], sbbox[3], sbbox[2], sbbox[1])
                raster_clip = self.gbdx.Task("RasterClip_Extents", raster=raster, chip_ul_lr = bbox)
                curTasks.append(raster_clip)

                clippedRaster = raster_clip.outputs.data
            else:
                clippedRaster = inRaster
            if runSpfeas == 1:
                #Run spfeas on the panchromatic band (no vegetation calculations)
                spTask = self.gbdx.Task("spfeas:0.4.3", data_in=clippedRaster, sensor=self.sensorDict[sensor], 
                   triggers=spfeasParams['triggers'], scales=spfeasParams['scales'], block=spfeasParams['block'],
                   gdal_cache=spfeasParams['gdal_cache'], section_size=spfeasParams['section_size'],
                   n_jobs=spfeasParams['n_jobs'])
                curTasks.append(spTask)
            
            if spfeasLoop == 1:
                #Run spfeas on the panchromatic band (no vegetation calculations)
                spLoopTask = self.gbdx.Task("spfeas_loop:0.4.0", data_in=clippedRaster, sensor=self.sensorDict[sensor], 
                   triggers=spfeasParams['triggers'], scales=spfeasParams['scales'], block=spfeasParams['block'])
                curTasks.append(spLoopTask)
            
            if runCarFinder == 1:
                #Run carfinder task on clipped image            
                car_finder_task = self.gbdx.Task("deepcore-singleshot", data=clippedRaster)                
                curTasks.append(car_finder_task)
            if runLC == 1:
                lcTask = self.gbdx.Task('lulc', image=clippedRaster)
                curTasks.append(lcTask)
                
            #Define the tasks in your workflow
            workflow = self.gbdx.Workflow(curTasks)
            workflow.timeout = 36000

            # save the outputs to your s3 bucket.  This method only needs a folder specified.  It will create this folder in your gbd-customer-customer-data s3 bucket.
            if runCarFinder == 1:
                workflow.savedata(car_finder_task.outputs.data, location="%s/%s" % (outS3Folder, "carCount"))
            if runSpfeas == 1:
                workflow.savedata(spTask.outputs.data_out,      location="%s/%s" % (outS3Folder, "spfeas"))
            if spfeasLoop == 1:
                workflow.savedata(spLoopTask.outputs.data_out,  location="%s/%s" % (outS3Folder, "spfeasLoop"))
            if runLC == 1:
                workflow.savedata(lcTask.outputs.image,         location="%s/%s" % (outS3Folder, "lulc"))
            if downloadImages == 1:
                workflow.savedata(clippedRaster,                location="%s/%s" % (outS3Folder, "clippedRaster"))
            
            
            
            return (workflow)
            
        
    def createTasks(self, inD, outS3Folder,
                    spfeasParams={"triggers":'ndvi mean', "scales":'8 16 32', "block":'4'}, 
                    runCarFinder = 0, runSpfeas = 1, downloadImages = 1,
                    aopPan=False, aopDra=False, aopAcomp = True, aopBands='MS'):
        '''Create a series of task workflows to perform AOP striop, clip raster, then optionally
            spfeas, car finding
            
            RETURNS - dictionary of 
                'WORKFLOWS' - list of workflows to perform defined tasks
                'TO_ORDERS' - list of scene IDs that have not yet been ordered        
        '''
        gbdx = self.gbdx
        allTasks = []
        forOrder = []
        for index, row in inD.iterrows():
            curTasks = []
            catalog_id = row['ID']             
            bbox = row['bbox'] 
            inputWKT = row['used_scene_region_WKT']
            sensor = row['Sensor']
            data = gbdx.catalog.get_data_location(catalog_id)
            
            if data is None:
                logging.info("%s is not ordered" % str(catalog_id))
                #order_id = gbdx.ordering.order(str(row['ID']))
                forOrder.append(str(catalog_id))
            else:    
                #try:    
                aopParts = self.getImageParts(catalog_id, inputWKT)
                s3TaskFolder = outS3Folder % ("%s_%s" % (catalog_id, "_".join(aopParts.replace(",", ""))))                
                if len(aopParts) > 0:

                    aoptask = gbdx.Task("AOP_Strip_Processor", data=data, parts=aopParts, 
                                        enable_pansharpen=aopPan, enable_dra=aopDra, 
                                        enable_acomp=aopAcomp, bands=aopBands)
                    curTasks.append(aoptask)

                    #clip the image
                    raster = aoptask.outputs.data.value
                    raster_clip = gbdx.Task("RasterClip_Extents", raster=raster, chip_ul_lr = bbox)
                    curTasks.append(raster_clip)

                    clippedRaster = raster_clip.outputs.data
                    if runSpfeas == 1:
                        #Run spfeas on the panchromatic band (no vegetation calculations)
                        spTask = gbdx.Task("spfeas_WBG:0.1.3", data_in=clippedRaster, sensor=self.sensorDict[sensor], 
                           triggers=spfeasParams['triggers'], scales=spfeasParams['scales'], block=spfeasParams['block'])
                        curTasks.append(spTask)

                    if runCarFinder == 1:
                        #Run carfinder task on clipped image            
                        car_finder_task = gbdx.Task("deepcore-singleshot", data=clippedRaster)
                        curTasks.append(car_finder_task)

                    #Define the tasks in your workflow
                    workflow = gbdx.Workflow(curTasks)

                    if runCarFinder == 1:
                        # save the outputs to your s3 bucket.  This method only needs a folder specified.  It will create this folder in your gbd-customer-customer-data s3 bucket.
                        workflow.savedata(car_finder_task.outputs.data, location=os.path.join(s3TaskFolder, "carCount"))

                    if runSpfeas == 1:
                        workflow.savedata(spTask.outputs.data_out, location=os.path.join(outS3Folder % catalog_id, "spfeas"))

                    if downloadImages == 1:
                        rasterOut = os.path.join(outS3Folder % catalog_id, "clippedRaster")
                        workflow.savedata(raster_clip.outputs.data, location=rasterOut)
                        #workflow.savedata(aoptask.outputs.data, location=rasterOut)
                    allTasks.append(workflow)
                #except Exception as e:        
                #    logging.warning("%s could not be processed: %s" % (row['ID'], e.message))
                    
        return( { 'WORKFLOWS':allTasks, 'TO_ORDERS':forOrder } )
        
    def getImageParts(self, catid, wkt):
        ''' List the parts of the provided catid that intersect the wkt
        
        '''
        gbdx = self.gbdx
        data_loc = gbdx.catalog.get_data_location(catalog_id=catid)
        # add the types of images to search for, IDAHO
        types = [ "IDAHOImage" ]
        filters=["catalogID ='" + catid + "'"]

        sWkt = shapely.wkt.loads(wkt)
        if sWkt.geom_type == "Polygon":
            results = gbdx.catalog.search(searchAreaWkt=wkt, filters=filters, types=types)
        else:
            allRes = []
            for s in sWkt:
                results = gbdx.catalog.search(searchAreaWkt=str(s), filters=filters, types=types)
                allRes.append(results)
        # Loop through the results of the search, and get the tiles
        tiles = []
        idaho_tiles = []
        for r in results :
                tiles.append(r['properties']['vendorDatasetIdentifier'][22:25])
                idaho_tiles.append(r['properties']['idahoImageId'] + " - " + r['properties']["sensorName"])
        return ",".join(map(str,list(set(map(int, tiles)))))

      