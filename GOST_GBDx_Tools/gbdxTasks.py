import sys, os, time, subprocess, requests, json, logging
import shapely, geojson, rasterio

import pandas as pd
import geopandas as gpd
import dask.array as da

from time import strftime
from gbdxtools import CatalogImage
from shapely.geometry import shape
from shapely.wkt import loads


class GOSTTasks(object):
    ''' Wrapper for executing a pre-defined set of GBDx tasks
    '''
    def __init__(self, gbdx):        
        self.gbdx = gbdx        
        self.sensorDict = {"WORLDVIEW01":"WorldView1", "WORLDVIEW02":"WorldView2", 
                            "GEOEYE01": "GeoEye1", "QUICKBIRD02":"Quickbird",
                            "WORLDVIEW03_VNIR":"WorldView3",
                            "WV03":"WorldView3",
                            "SENTINEL1":"gray"}   
    def calculateNDSV(self, inD, sensor, outFile):
        '''The Normalized Difference Spectral Vector normalizes all bands in an image against each other
        REFERENCE: https://ieeexplore.ieee.org/document/6587128/
        
        Args:
            inD (3D dask array) - input imagery from GBDx CatalogImage
            sensor (string) - definition of input sensor
            outFile (string) - output image chip
        Example:
            inSource = CatalogImage('103001007FA97400')
            inD = inSource[:,0:1000,0:1000]
        '''
        nBands = inD.shape[0]
        
        bandNames = []   
        allRes = []        
        for b1 in range(0, nBands):
            for b2 in range(0, nBands):
                if b1 < b2:
                    cMetric = (inD[b1,:,:] - inD[b2,:,:]) / (inD[b1,:,:] + inD[b2,:,:])
                    #Convert cMetric to byte
                    cMetric = (cMetric * 100) + 100
                    cMetric = cMetric.astype('uint8')
                    bandNames.append("%s_%s" % (b1, b2))
                    allRes.append(cMetric)
        outData = da.stack(allRes)
        #calculate indices        
        metadata = inD.ipe.metadata
        newDataset = rasterio.open(outFile, 'w', driver="GTiff", dtype='uint8',
                                    count=outData.shape[0],height=outData.shape[1],width=outData.shape[2],
                                    crs=inD.proj,transform=inD.affine)
        newDataset.write(outData)
        newDataset.close()
    
    def calculateIndices(self, inD, sensor, outFile):
        ''' Calculate indices (NDVI, NDWI, BAI) from source imagery
        
        Args:
            inD (3D dask array) - input imagery from GBDx CatalogImage
            sensor (string) - definition of input sensor
            outFile (string) - output image chip
        '''
        outImage = inD[0:2,:,:]
        if sensor in ['WV03','WV02']:
            irBand = 7
            redBand = 4
            c_Green = 0 
        elif sensor in ['IKONOS-2','GE01', 'QB02']:
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
        imgChipSize = 1000, band_type = "MS", panSharpen=True, acomp=True, getOutSize=False, 
        specificTiles=[[],[]]):
        ''' Uses the CatalogImage object to download and write data
            http://gbdxtools.readthedocs.io/en/latest/api_reference.html#catalogimage
            
        Args:
            cat_id [(string) - CatalogID for Digital Globe that is present in IDAHO
            outFile (string) - path to output image
            output (string, optional: IMAGE, INDICES, NDSV) - what kind of image to return. 'IMAGE' is the raw imagery, 
                    'INDICES' returns a stacked NDVI, NDWI and 'NDSV' return the Normalized Differece Spectral Vector
            boundingBox (list of bottom, left, top, right, optional) - bounding box to subset catalog image with
            specificImages (list of list of integers, optional) - specific columns (first list) and rows (second list) 
                to create - used mostly for re-running missing, crashed, or broken results
        '''
        img = CatalogImage(cat_id, pansharpen=panSharpen, band_type=band_type, acomp=acomp)
        sensor = img.metadata['image']['sensorPlatformName']
        if boundingBox:
            img = img.aoi(bbox=boundingBox)
        if curWKT:
            #Intersect the bounds of the curImg and the curWKT
            b = img.bounds
            curImageBounds = [[[b[0], b[1]], [b[0], b[3]], [b[2], b[3]], [b[2], b[1]], [b[0], b[1]]]]
            inPoly = geojson.Polygon(curImageBounds)
            imgBounds = shape(inPoly)
            if imgBounds.intersects(curWKT):
                img = img.aoi(wkt=str(imgBounds.intersection(curWKT)))            
            else:
                raise ValueError("Provided KML does not intersect image bounds")
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
            rowIndex = range(0, len(rowSteps) - 1, 1)
            if len(specificTiles[1]) > 0:
                rowIndex = specificTiles[1]
            colSteps = range(0,img.shape[2],imgChipSize)
            colSteps.append(img.shape[2])
            colIndex = range(0, len(colSteps) - 1, 1)
            if len(specificTiles[0]) > 0:
                colIndex = specificTiles[0]
            for rIdx in rowIndex:
                for cIdx in colIndex:
                    logging.info("Downloading row %s of %s and column %s of %s" % (rIdx, len(rowSteps), cIdx, len(colSteps)))
                    outputChip = os.path.join(outFolder, "C%s_R%s.tif" % (cIdx, rIdx))
                    curChip = img[0:img.shape[0], rowSteps[rIdx]:rowSteps[rIdx + 1], colSteps[cIdx]:colSteps[cIdx + 1]]
                    if not os.path.exists(outputChip):
                        if output == "IMAGE":
                            #curChip.geotiff(path=outputChip)
                            out_meta = {"dtype":curChip.dtype,  "compress":'lzw', "driver": "GTiff",
                                "count":curChip.shape[0],"height": curChip.shape[1], "width": curChip.shape[2],
                                "transform": curChip.affine,
                                #Affine(img.metadata['georef']['scaleX'], 0.0, img.metadata['georef']['translateX'],0.0, img.metadata['georef']['scaleY'], img.metadata['georef']['translateY']),
                                "crs": curChip.proj
                                }
                            with rasterio.open(outputChip, "w", **out_meta) as dest:
                                dest.write(curChip)
                        if output == "INDICES":
                            outImage = self.calculateIndices(curChip, sensor, outputChip)
                        if output == 'NDSV': 
                            outImage = self.calculateNDSV(curChip, sensor, outputChip)
                        
        else:            
            #img.geotiff(path=outFile)
            if output == "IMAGE":
                img.geotiff(path=outFile)
            if output == "INDICES":
                outImage = self.calculateIndices(img, sensor, outFile)
            if output == 'NDSV': 
                outImage = self.calculateNDSV(img, sensor, outFile)
        return 1    
    
    def createWorkflow(self, catalog_id, inputWKT, sensor, outS3Folder,
                    spfeasParams={"triggers":'ndvi mean', "scales":'8 16 32', "block":'4'}, 
                    runCarFinder = 0, runSpfeas = 1, runLC = 0, downloadImages = 1, 
                    aopPan=False, aopDra=False, aopAcomp = True, aopBands='Auto', inRaster=""):
        ''' Create a workflow to execute on gbdx
        
        Args:
            catalog_id (string): catalog ID to lookup in gbdx and use in execution
            inputWKT (string wkt): shape to run analysis in - must be valid wkt
            sensor (string): sensor name for image, necessary for executing spfeas
            outS3Folder (string): path to store results NOT ENTIRE PATH, JUST INCLUDE NAMED PATH i.e. - bps/CityName/Results
            spfeasParams (Dictionary, optional): parameters to execute spfeas
            runCarFinder/runspfeas/runLC/downloadImages (1 or 0, optional): binary to define what tasks to run. 
                Defaults to running spfeas and downloading images
            aopPan/aopDra/aopAcomp (boolean, optional): Boolean to define what to do in AOP strip processing, defaults to ACOMP only
            aopBands (string, optional): Defines what bands to extract, defaults to auto
            inRaster (string, optional): s3 location of input imagery, means that ACOMP and clip are not run, useful for running
                multiple analyses on a single area.
        
        Returns:
            gbdx workflow: object to be exeucted in order to create analysis
        
        Reference:
            SPFEAS: https://github.com/jgrss/spfeas
            CARS: https://gbdxdocs.digitalglobe.com/docs/answer-type-slides#section-cars
            ACOMP: https://gbdxdocs.digitalglobe.com/docs/advanced-image-preprocessor
            LULC: https://gbdxdocs.digitalglobe.com/docs/answer-type-slides#section-lulc-protogen
        '''
        curTasks = []        
        data = self.gbdx.catalog.get_data_location(catalog_id)    
        if inRaster == 'SENTINEL1':
            #For sentinel1 imagery, no need to run AOP
            sWkt = shapely.wkt.loads(inputWKT)
            sbbox = sWkt.bounds
            bbox = '%s,%s,%s,%s' % (sbbox[0], sbbox[3], sbbox[2], sbbox[1])
            raster_clip = self.gbdx.Task("RasterClip_Extents", raster=data, chip_ul_lr = bbox)
            curTasks.append(raster_clip)
            clippedRaster = raster_clip.outputs.data
        elif inRaster == "": 
            #Run AOP and raster clip if input raster is not defined
            aopParts = self.getImageParts(catalog_id, inputWKT)

            if len(aopParts) > 0:
                aoptask = self.gbdx.Task("AOP_Strip_Processor", data=data, 
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
                raise(ValueError("No intersecting image parts"))
        else:
            #If raster location is provided, define it here
            clippedRaster = inRaster
        if runSpfeas == 1:
            cSensor = self.sensorDict[sensor]
            useRGB = True
            if cSensor == 'gray':
                useRGB = False
                cSensor = 'Quickbird'
            
            #Run spfeas on the panchromatic band (no vegetation calculations)
            spTask = self.gbdx.Task("spfeas:0.4.3", data_in=clippedRaster, sensor=cSensor, 
               triggers=spfeasParams['triggers'], scales=spfeasParams['scales'], block=spfeasParams['block'],
               gdal_cache=spfeasParams['gdal_cache'], section_size=spfeasParams['section_size'],
               n_jobs=spfeasParams['n_jobs'], use_rgb=useRGB)
            curTasks.append(spTask)
        
        if runCarFinder == 1:
            #Run carfinder task on clipped image            
            car_finder_task = self.gbdx.Task("deepcore-singleshot", data=clippedRaster)                
            curTasks.append(car_finder_task)
        if runLC == 1:
            lcTask = self.gbdx.Task('protogenV2LULC', raster=clippedRaster)
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
            workflow.savedata(lcTask.outputs.data,          location="%s/%s" % (outS3Folder, "lulc"))
        if downloadImages == 1:
            workflow.savedata(clippedRaster,                location="%s/%s" % (outS3Folder, "clippedRaster"))                       
        return (workflow)
                    
        
    def getImageParts(self, catid, wkt):
        ''' List the parts of the provided catid that intersect the wkt. Provided by DigitalGlobe staff
        
        '''
        gbdx = self.gbdx
        data_loc = gbdx.catalog.get_data_location(catalog_id=catid)
        # add the types of images to search for, IDAHO
        types = [ "IDAHOImage" ]
        filters=["catalogID ='" + catid + "'"]

        sWkt = loads(wkt)
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

      