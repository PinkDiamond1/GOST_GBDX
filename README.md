# Tools for GBDx
There are two sets of tools here, and a script for launching these tools.
1. GOST_GBDx_Tools/gbdxTasks.py: This library is used to launch GBDx tasks. See createTasks to launch GBDx tasks that are based on the output script from Charles' searching script
2. GOST_GBDx_Tools/gbdxURL_misc: Manipulates the GBDx url fetches - useful for monitoring workflows and listing/downloading results. 

See the launch_* tools for executing these tools.

# Setup
1. Install gbdxtools 
```
C:\AFOLDER\Somewhere> conda install -c conda-forge -c digitalglobe gbdxtools
```
2. Test the gbdx install. 
`python -c "import gbdxtools"`
3. Enter Python and test the Interface object - this will require creating the .gbdx-config file in the home directory
```
C:\AFOLDER\Somewhere> python
> from gbdxtools import Interface
> gbdx = Interface()
> print gbdx.s3.info
```
4. Check out the Notebooks folder to get started

# Examples
### Imagery Search
```Python
import geopandas as gpd

from gbdxtools import Interface
from GOST_GBDx_Tools import imagery_search

gbdx = Interface() #This needs to be authenticated

inAOI = r"C:\temp\someFile.shp"
imageryCSV = AOI.replace(".shp", "_imagerySearch.csv")

inShape = gpd.read_filein(AOI)
curWKT = inShape.unary_union.wkt
curRes = imagery_search.searchForImages(gbdx, curWKT,
                cutoff_date='1-Jan-2017', optimal_date='1-Jan-2019', #Search for imagery in a 2-year window with more recent being better
                cutoff_cloud_cover = 50, cutoff_nadir = 90, imageType='*')
curRes.to_csv(imageryCSV)
```
