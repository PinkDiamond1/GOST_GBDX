import rasterio

import numpy as np

from skimage import io


def reclassifyLandcover(inFile, outFile, color_codes ={ (0,   255, 0) :  1,
                                                        (0,   0,   128): 2,
                                                        (128, 64,  0):   3,
                                                        (128, 255, 255): 4,
                                                        (164, 74,  164): 5,
                                                        (128, 128, 128): 6 }):
    ''' convert the 3-band RBG results of the GBDx LULC classification to single-band
            Vegetation -    (0,   255, 0) :  1,
            Water -         (0,   0,   128): 2,
            bare -          (128, 64,  0):   3,
            clouds -        (128, 255, 255): 4,
            Shadows -       (164, 74,  164): 5,
            unclassed -     (128, 128, 128): 6 }
    
    Args:
        inFile (string) - path to input landcover classification file
        outFile (string) - path to output landcover classification file
    
    RETURNS
    success_variable
    '''
    ioR = io.imread(inFile)
    inR = rasterio.open(inFile)
    out_meta = inR.meta
    out_meta.update({"count":1,
        'driver': 'GTiff',
        'interleave': 'band',
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'compress': 'lzw',
        'nodata': 0
    }) 
    
    result = np.ndarray(ioR.shape[:2], dtype=int)
    for cCode, idx in color_codes.iteritems():
        result[(ioR == cCode).all(2)] = idx

    with rasterio.open(outFile, 'w', **out_meta) as dest:
        dest.write_band(1, result.astype(out_meta['dtype']))

        
if __name__ == "__main__":   
    inFile = r"Q:\WORKINGPROJECTS\Indonesia_GBDx\Balikpapan_GBDx\103001007E75D600\lulc\058350570010_01_assembly_clip_LULC.tif"
    inFile2 = r"Q:\WORKINGPROJECTS\Indonesia_GBDx\Balikpapan_GBDx\104001003E70C400\lulc\058350569010_01_assembly_clip_LULC.tif"

    reclassifyLandcover(inFile, inFile.replace(".tif", "_CLASSIFIED.tif"))
    reclassifyLandcover(inFile2, inFile2.replace(".tif", "_CLASSIFIED.tif"))
