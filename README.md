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
```
import geopandas as gpd

from gbdxtools import Interface
from GOST_GBDx_Tools import imagery_search

gbdx = Interface() #This needs to be authenticated

AOI = r"C:\temp\someFile.shp"
imageryCSV = AOI.replace(".shp", "_imagerySearch.csv")

inShape = gpd.read_file(AOI)
curWKT = inShape.unary_union.wkt
curRes = imagery_search.searchForImages(gbdx, curWKT,
                cutoff_date='1-Jan-2019', optimal_date='1-Jun-2019',
                cutoff_cloud_cover = 50, cutoff_nadir = 90, imageType='*')
curRes.to_csv(imageryCSV)
```
