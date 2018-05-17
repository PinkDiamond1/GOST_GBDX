# Tools for GBDx
There are two sets of tools here, and a script for launching these tools.
1. GOST_GBDx_Tools/gbdxTasks.py: This library is used to launch GBDx tasks. See createTasks to launch GBDx tasks that are based on the output script from Charles' searching script
2. GOST_GBDx_Tools/gbdxURL_misc: Manipulates the GBDx url fetches - useful for monitoring workflows and listing/downloading results. **Warning, the AWSCLI stuff is off-the-wall weird because of WBG limitations of web-based command line tools - discuss with Ben**

See the launch_* tools for executing these tools.

# Setup
1. Install gbdxtools **in a new conda environment, this has caused problems for Clara**
```
C:\AFOLDER\Somewhere> conda -c digitalglobe install gbdxtools
```
2. Test the gbdx install. 
`python -c "import gbdxtools"`
3. Enter Python and test the Interface object - this will require creating the .gbdx-config file in the home directory
```
C:\AFOLDER\Somewhere> python
> from gbdxtools import Interface
> gbdx = Interface()
> print gbdx.s3.Info
```
4. Check out the launch_* scripts to get started
