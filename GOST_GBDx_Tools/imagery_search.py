import sys, os
import pip
import shapely
import time
import json
import pandas as pd
import geopandas as gpd

from shapely.wkt import loads
from shapely.geometry import box
from shapely.geometry import MultiPolygon
from gbdxtools import Interface
from gbdxtools.task import env
from gbdxtools import CatalogImage

def searchForImages(gbdx, AOI, outputFolder, filePrefix,
        cutoff_cloud_cover = 25,   # images with CC over this threshold discarded
        cutoff_overlap = 0,     # images with AOI overlap below this threshold discarded. [N.b.: keep small if AOI large.]
        cutoff_date = '1-Jan-15',  # images older than this date discarded
        cutoff_nadir = 25, # Images at nadir angles greater than threshold discarded
        cutoff_pan_res = 1, # Images below this resolution discarded
        accepted_bands = ['PAN','PAN_MS1','PAN_MS1_MS2'], #  Images with any other band entry discarded
        optimal_date =  '01-Mar-18', # Optimal date (enter as dd-mmm-yy)
        optimal_pan_res = 0.4, # Optimal pan resolution, metres
        optimal_nadir = 0, # optimal image angle. 0 = vertical
        pref_weights = {
            'cloud_cover': 0.2,
            'overlap':0.2,
            'date': 0.2,
            'nadir': 0.2,
            'resolution': 0.2
            }
        ):
    '''
    print gbdx
    print AOI
    print outputFolder
    print filePrefix
    '''
    missingSceneList = os.path.join(outputFolder, "%s_missing_scene_list.csv" % filePrefix)

    # Create bboxx - the square shaped box which will always contain the AOI.
    bboxx = []
    for coord in range(0,len(AOI.bounds)): 
        bboxx.append(AOI.bounds[coord])

    # Define search function. Returns up to 1000 images where cloud cover smaller than 25%
    def search_unordered(bbox, _type, count=10000, cloud_cover=25):
        aoi = AOI.wkt
        query = "item_type:{} AND item_type:DigitalGlobeAcquisition".format(_type)
        query += " AND attributes.cloudCover_int:<{}".format(cloud_cover)
        #print('print query')
        #print(query)
        return gbdx.vectors.query(aoi, query, count=count)

    # Run search on Area of Interest (AOI). Passes in AOI in Well Known Text format (wkt)
    records = search_unordered(AOI.wkt, 'DigitalGlobeAcquisition', cloud_cover = cutoff_cloud_cover)
    # Create list object of all catalog IDs returned in search
    ids = [r['properties']['attributes']['catalogID'] for r in records]

    #print('print records')
    #print(records)

    # Define Counters
    l = 0    # number of non-IDAHO images
    scenes = [] # list containing metadata dictionaries of all scenes in our AOI 
    # Toggle for printing images to screen
    download_thumbnails = 0
    # Loop catalog IDs
    for i in ids:        
        # Fetch metadata dictionary for each catalog ID in ids list
        # Check location of ID - is it in IDAHO?
        try:
            idaho = 0
            r = gbdx.catalog.get(i)        
            location = gbdx.catalog.get_data_location(i)        
            # Defines IDAHO variable as binary 1 / 0 depending on whether it is in IDAHO already or not
            if location != 'not_delivered':                
                idaho = 1                    
            # Calculate the percentage overlap with our AOI for each scene
            # load as a Shapely object the wkt representation of the scene footprint
            footprint = r['properties']['footprintWkt']
            shapely_footprint = shapely.wkt.loads(footprint)
            # Calculate the object that represents the difference between the AOI and the scene footprint 
            AA = AOI.difference(shapely_footprint)
            # Define frac as the fraction, between 0 and 1, of the AOI that the scene covers
            frac = 1 - ((AA).area / AOI.area)
            # Create BB - the proxy for the useful area. IF scene entirely contains AOI, then BB = AOI, else it is the intersection 
            # of the scene footprint and the AOI
            BB = AOI 
            if frac < 1:
                BB = AOI - AA
            #shapely_footprint.intersection(AOI)
            # Similarly, AA, the difference area between AOI and the scene, can be set to null if the scene contains 100% of the AOI 
            if frac == 1:
                AA = ""
            # Append key metadata to list obejct 'scenes' for the current scene, as a dictionary. This then moves into the pandas dataframe.
            # Several objects here are from DigitalGlobe's metadata dictionary (anything with an r start)
            scenes.append({
                'ID':i, 
                'TimeStamp':r['properties']['timestamp'],
                'CloudCover':r['properties']['cloudCover'],
                'ImageBands':r['properties']['imageBands'],
                'On_IDAHO':idaho,
                'browseURL': r['properties']['browseURL'],
                'Overlap_%': frac * 100,
                'PanResolution': r['properties']['panResolution'],
                'MultiResolution': r['properties']['multiResolution'],
                'OffNadirAngle': r['properties']['offNadirAngle'],
                'Sensor':r['properties']['sensorPlatformName'],
                'Full_scene_WKT':r['properties']['footprintWkt'],
                'missing_area_WKT':AA,
                'useful_area_WKT':BB
                })       
        except:
            pass
    # Define column order for dataframe of search results
    cols = ['ID','Sensor','ImageBands','TimeStamp','CloudCover','Overlap_%','PanResolution','MultiResolution','OffNadirAngle','On_IDAHO','browseURL','Full_scene_WKT','useful_area_WKT','missing_area_WKT']
    #Generate pandas dataframe from results
    out = pd.DataFrame(scenes,columns = cols)
    # Convert Timestamp field to pandas DateTime object
    out['TS'] = out['TimeStamp'].apply(lambda x: pd.Timestamp(x).tz_localize(None))
    # Add separate date and time columns for easy interpretation
    string = out['TimeStamp'].str.split('T')
    out['Date'] = string.str.get(0)
    out['Time'] = string.str.get(1)
    # Categorical Search: remove disqualified images. Copy of dataframe taken, renamed to 'out_1stcut'.
    cutoff_date = pd.Timestamp(cutoff_date)
    out_1stcut = out.loc[(out['CloudCover'] <= cutoff_cloud_cover) & 
                         (out['Overlap_%'] >= cutoff_overlap) & 
                         (out['TS'] > cutoff_date) &                          
                         (out['OffNadirAngle'] <= cutoff_nadir) & 
                         (out['PanResolution'] <= cutoff_pan_res) &
                         (out['ImageBands'].isin(accepted_bands))
                        ]    
    # Apply ranking method over all non-disqualified search results for each field
    optimal_date = pd.to_datetime(optimal_date, utc = True).tz_localize(None)

    # each 1% of cloud cover = 1 point
    out_1stcut['points_CC'] = (out_1stcut['CloudCover'])  

    # each 1% of overlap missed = 1 point
    out_1stcut['points_Overlap'] = (100 - out_1stcut['Overlap_%'])  

    # each week away from the optimal date = 1 point 
    out_1stcut['points_Date'] = ((abs(out_1stcut['TS'] - optimal_date)).view('int64') / 60 / 60 / 24 / 1E9) / 7 

    # each degree off nadir = 1 point
    out_1stcut['points_Nadir'] = abs(out_1stcut['OffNadirAngle'] - optimal_nadir) 

    # each cm of resolution worse than the optimal resolution = 1 point
    out_1stcut['points_Res'] = (out_1stcut['PanResolution'] - optimal_pan_res).apply(lambda x: max(x,0)) * 100 

    # Define ranking algorithm - weight point components defined above by the preference weighting dictionary
    def Ranker(out_1stcut, pref_weights):
        a = out_1stcut['points_CC'] * pref_weights['cloud_cover']
        b = out_1stcut['points_Overlap'] * pref_weights['overlap']
        c = out_1stcut['points_Date'] * pref_weights['date'] 
        d = out_1stcut['points_Nadir'] * pref_weights['nadir']
        e = out_1stcut['points_Res'] * pref_weights['resolution']
        
        # Score is linear addition of the number of 'points' the scene wins as defined above. More points = worse fit to criteria
        rank = a + b + c + d + e
        return rank

    # Add new column - Rank Result - with the total number of points accrued by the scene 
    out_1stcut['RankResult'] = Ranker(out_1stcut,pref_weights)

    # Add a Preference order column - Pref_Order - based on Rank Result, sorted ascending (best scene first)
    out_1stcut = out_1stcut.sort_values(by = 'RankResult', axis = 0, ascending = True)
    out_1stcut = out_1stcut.reset_index()
    out_1stcut['Pref_order'] = out_1stcut.index + 1
    out_1stcut = out_1stcut.drop(['index'], axis = 1)
    
    cols = ['ID','Sensor','ImageBands','Date','Time','CloudCover','Overlap_%','PanResolution','MultiResolution','OffNadirAngle','On_IDAHO','Pref_order','RankResult','points_CC','points_Overlap','points_Date','points_Nadir','points_Res','browseURL','Full_scene_WKT','useful_area_WKT','missing_area_WKT']
    out_1stcut = out_1stcut[cols]
    # Create a new copy of the dataframe to work on
    finaldf = out_1stcut
    # Add column for used scene region area, expressed as .wkt
    finaldf['used_scene_region_WKT'] = 0
    finaldf['used_area'] = 0
    finaldf['geom_type'] = 0
    # Set initial value of AOI_remaining to the full AOI under consideration
    AOI_remaining = AOI
    # Create two lists - usedareas for the areas of scenes used in the final product, and AOI_rems to record sequential reduction in 
    # remaining AOI that needs to be filled
    usedareas = []
    AOI_rems = []
    # Set up loop for each image in dataframe of ranked images
    for s in finaldf.index:
        # pick up the WKT of the useful area as the useful_scene_region variable
        useful_scene_region = finaldf['useful_area_WKT'].loc[s]
        # Set up try loop - to catch if there is no intersection of AOI_remaining and useful_scene_region
        try:            
            # define 'used_scene_region' as the useable bit of the image that overlaps the AOI; add its geometry to df. 
            used_scene_region = AOI_remaining.intersection(useful_scene_region)
            finaldf['geom_type'] = used_scene_region.type
            # calculate the area of that region
            used_area = used_scene_region.area
            # Check to see if this is a geometry collection. This shapely type if for 'jumbles' of outputs (e.g. Polygons + Lines)
            # This can be created if the intersection process decides that it also wants a 1-pixel strip from the bottom of the image
            # as well as the main chunk. This won't translate back to a shapefile, so we drop non-Polygon objects iteratively. 
            if used_scene_region.type == 'GeometryCollection':
                xlist = []
                # Iterate through all objects in the geometry collection
                for y in used_scene_region.geoms:
                    # Add polygons to a fresh list
                    if y.type == 'Polygon':
                        xlist.append(y)
                # Convert that list to a multipolygon object
                used_scene_region = MultiPolygon(xlist)
            else:
                pass
            # Append the used bit of the image to the usedareas list. 
            usedareas.append(used_scene_region)
            # Add two new columns to the dataframe - the used scene geometry in wkt, and the area of the used scene
            finaldf['used_scene_region_WKT'].loc[s] = used_scene_region
            finaldf['used_area'].loc[s] = used_area
            # Redefine the area of the AOI that needs to be filled by the next, lower-rank image
            AOI_remaining = AOI_remaining.difference(used_scene_region)
            # Add this to the AOI_rems list for troubelshooting and verification
            AOI_rems.append(AOI_remaining)
        except:
            pass
    
    return finaldf
