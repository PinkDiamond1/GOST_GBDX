#!/usr/bin/env python

import os, fnmatch, yaml, spfeas, json
import rasterio

import dask as da

from gbdx_task_interface import GbdxTaskInterface

class imageIndex(object):
    ''' Calculate a series of remote sensing indices on input imagery
    
    TODO: Add options to process other input imageTypes
    '''
    def __init__(self, inFolder, outFolder, **kwargs):
        self.inFolder = inFolder
        self.outFolder = outFolder
        if kwargs['imageyType'] == "SINGLE":
            self.input_image = os.path.join(inFolder, fnmatch.filter(os.listdir(input_dir), "*.tif")[0])
            self.cImage = rasterio.open(self.input_image)
            
            
    def calculateNDSV(self, inD, outFile):
        '''The Normalized Difference Spectral Vector normalizes all bands in an image against each other
        REFERENCE: https://ieeexplore.ieee.org/document/6587128/
        
        INPUTS
        inD [3D dask array] - input imagery from GBDx CatalogImage
        sensor - definition of input sensor
        outFile - output image file name
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
        newDataset = rasterio.open(outFile, 'w', driver="GTiff", dtype='uint8',
                                    count=outData.shape[0],height=outData.shape[1],width=outData.shape[2],
                                    crs=inD.proj,transform=inD.affine)
        self.bandNames = bandNames
        newDataset.write(outData)
        newDataset.close()
    
    def calculateNDVI_NDWI(self, inD, sensor, outFile):        
        ''' Calculate NDVI and NDWI based on input image
        INPUTS
        inD [3D dask array] - input imagery from GBDx CatalogImage
        sensor [string] - describes sensor for identifying focal bands for calculations
        outFile [string] = output file
        '''
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
        #Convert traditional scale to 0-200 so it can be stored as uint8
        outData = (outData * 100) + 100
        outData = outData.astype('uint8')
        newDataset = rasterio.open(outFile, 'w', driver="GTiff", dtype='uint8',
                                    count=outData.shape[0],height=outData.shape[1],width=outData.shape[2],
                                    crs=inD.proj,transform=inD.affine)
        newDataset.write(outData)
        newDataset.close()    

class SpFeasTask(GbdxTaskInterface):
    def invoke(self):
        # Create the output folder  
        output_folder = self.get_output_data_port('data_out')
        print output_folder
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        user_params = {            
            'index':self.get_input_string_port('index', default = 'NDVI'),
            #Defines how the input imagery is stored: 
            #   SINGLE means single tiff in a folder
            #   FOLDER means folder of tiled tiffs
            #   VRT means a single vrt in a folder
            'imageType':self.get_input_string_port('imageFormat', default = 'SINGLE')            
        }

        # Get the input image folder
        input_dir = self.get_input_data_port('data_in', default="/mnt/work/input")
        
        # calculate indices
        try:
            indexImage = imageIndex(input_dir, output_folder, **user_params)
            
            for cIndex in user_params['index'].split(" "):
                
            
            spfeas.spatial_features(input_image,output_folder, **user_params)
            outJSON = { "status": "Success", "reason": "cause you rock!" }
        except Exception as e:
            outJSON = { "status": "Failed", "reason": e.message }
        
        #Write status file as output
        with open("/mnt/work/status.json", 'w') as statusFile:
            json.dump(outJSON, statusFile)
            
if __name__ == '__main__':
    with SpFeasTask() as task:
        task.invoke()
